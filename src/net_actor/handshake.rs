use std::io;

use anyhow::anyhow;
use log::{debug, info};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::bolt_version::BoltCapabilities;
use crate::ext::fmt::IterFmtExt;
use crate::net_actor::{cancelable_io, NetActor, NetActorError, NetActorResult};
use crate::parser::ActorConfig;
use crate::str_bytes;

impl<T: AsyncRead + AsyncWrite + Unpin> NetActor<'_, T> {
    pub(super) async fn handshake(&mut self) -> NetActorResult<()> {
        self.preamble().await?;
        let res = match self.negotiate_client_version_request().await? {
            None => {
                swallow_anyhow_error(self.send_no_negotiated_bolt_version().await)?;
                Err(NetActorError::from_anyhow(anyhow!(
                    "No comon bolt version found"
                )))
            }
            Some((MANIFEST_MAJOR, 1)) => {
                self.handshake_delay().await;
                self.perform_manifest_v1_negotiation().await
            }
            Some((MANIFEST_MAJOR, manifest)) => Err(NetActorError::from_anyhow(anyhow!(
                "Unimplemented manifest version {manifest}"
            ))),
            Some((major, minor)) => {
                if self.script.config.bolt_capabilities != Default::default() {
                    let msg = "Script contains bolt capabilities, \
                        but non-manifest style negotiation is used.";
                    debug!("{msg}");
                    swallow_anyhow_error(self.send_no_negotiated_bolt_version().await)?;
                    Err(NetActorError::from_anyhow(anyhow!("{msg}")))
                } else {
                    self.handshake_delay().await;
                    self.send_non_manifest_bolt_version(major, minor).await
                }
            }
        };
        res.inspect_err(|e| debug!("Handshake failed: {e}"))
    }

    async fn preamble(&mut self) -> NetActorResult<()> {
        let mut buffer = [0u8; 4];
        cancelable_io(
            "reading magic preamble",
            &self.ct,
            self.conn.read_exact(&mut buffer),
        )
        .await?;

        info!("C: <MAGIC> {}", str_bytes::fmt_bytes_compact(&buffer));

        if buffer != [0x60, 0x60, 0xB0, 0x17] {
            return Err(NetActorError::from_anyhow(anyhow!(
                "Received wrong magic preamble: {}",
                str_bytes::fmt_bytes(&buffer)
            )));
        }
        Ok(())
    }

    async fn negotiate_client_version_request(&mut self) -> NetActorResult<Option<(u8, u8)>> {
        let mut buffer = [0u8; 16];
        cancelable_io(
            "reading client handshake request",
            &self.ct,
            self.conn.read_exact(&mut buffer),
        )
        .await?;

        info!(
            "C: <HANDSHAKE> {}",
            buffer
                .chunks(4)
                .map(str_bytes::fmt_bytes_compact)
                .join_display(" ")
        );

        for entry in buffer.chunks(4) {
            let entry = entry.try_into().unwrap();
            debug!("Decoding client handshake request: {entry:?}");
            let agreement = ClientVersionRequest::from_bytes(entry)?.negotiate(&self.script.config);
            if let Some(agreement) = agreement {
                return Ok(Some(agreement));
            }
        }
        Ok(None)
    }

    async fn send_no_negotiated_bolt_version(&mut self) -> NetActorResult<()> {
        info!("S: <HANDSHAKE> 0x00000000");
        cancelable_io(
            "sending no negotiated bolt version",
            &self.ct.clone(),
            async {
                self.conn.write_all(&[0x00, 0x00, 0x00, 0x00]).await?;
                self.conn.flush().await
            },
        )
        .await
    }

    async fn send_non_manifest_bolt_version(&mut self, major: u8, minor: u8) -> NetActorResult<()> {
        let response = [0x00, 0x00, minor, major];
        info!("S: <HANDSHAKE> {}", str_bytes::fmt_bytes_compact(&response));
        cancelable_io(
            "sending non-manifest negotiated bolt version",
            &self.ct.clone(),
            async {
                self.conn.write_all(&response).await?;
                self.conn.flush().await
            },
        )
        .await
    }

    async fn perform_manifest_v1_negotiation(&mut self) -> NetActorResult<()> {
        self.write_manifest_v1_offer().await?;
        self.read_manifest_v1_response().await
    }

    async fn write_manifest_v1_offer(&mut self) -> NetActorResult<()> {
        let version = self.script.config.bolt_version_raw;
        let version_aliases = self
            .script
            .config
            .bolt_version
            .backwards_equivalent_versions();
        let versions: Vec<_> = [version]
            .into_iter()
            .chain(version_aliases.iter().copied())
            .map(|(major, minor)| [0, 0, minor, major])
            .collect();
        let capabilities = self.script.config.bolt_capabilities.raw();
        info!(
            "S: <HANDSHAKE> 0x0000FF01 [{}] {} {}",
            versions.len(),
            versions
                .iter()
                .map(|b| str_bytes::fmt_bytes_compact(b))
                .join_display(" "),
            str_bytes::fmt_bytes_compact(capabilities)
        );
        cancelable_io(
            "sending manifest v1 style server offer",
            &self.ct.clone(),
            async {
                self.conn.write_all(&[0x00, 0x00, 0x01, 0xFF]).await?;
                self.write_var_int(versions.len()).await?;
                for version in versions {
                    self.conn.write_all(&version).await?;
                }
                self.conn.write_all(capabilities).await?;
                self.conn.flush().await
            },
        )
        .await
    }

    async fn read_manifest_v1_response(&mut self) -> NetActorResult<()> {
        let mut version_choice = [0u8; 4];
        cancelable_io(
            "reading manifest v1 version choice",
            &self.ct,
            self.conn.read_exact(&mut version_choice),
        )
        .await?;
        let raw_capabilities = cancelable_io(
            "reading manifest v1 capabilities",
            &self.ct.clone(),
            self.read_capabilities(),
        )
        .await?;

        info!(
            "C: <HANDSHAKE> {} {}",
            str_bytes::fmt_bytes_compact(&version_choice),
            str_bytes::fmt_bytes_compact(&raw_capabilities)
        );

        ClientVersionRequest::from_bytes(version_choice)?
            .accept_exact_version(&self.script.config)?;
        let capabilities = BoltCapabilities::from_bytes(raw_capabilities).map_err(|e| {
            NetActorError::from_anyhow(anyhow!("Failed to parse client capabilities: {e}"))
        })?;
        if capabilities != self.script.config.bolt_capabilities {
            return Err(NetActorError::from_anyhow(anyhow!(
                "Client capabilities don't match expected capabilities"
            )));
        }
        Ok(())
    }

    async fn write_var_int(&mut self, mut val: usize) -> io::Result<()> {
        loop {
            let mut byte = (val & 0x7F) as u8;
            val >>= 7;
            if val != 0 {
                byte |= 0x80;
            }

            self.conn.write_all(&[byte]).await?;
            if val == 0 {
                break;
            }
        }
        Ok(())
    }

    async fn read_capabilities(&mut self) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(self.script.config.bolt_capabilities.raw().len());
        let mut byte = [0u8; 1];
        loop {
            self.conn.read_exact(&mut byte).await?;
            bytes.push(byte[0]);
            if byte[0] & 0x80 == 0 {
                break;
            }
        }
        Ok(bytes)
    }

    async fn handshake_delay(&self) {
        let Some(delay) = self.script.config.handshake_delay else {
            return;
        };
        info!("S: <HANDSHAKE DELAY> {}s", delay.as_secs_f64());
        tokio::time::sleep(delay).await;
    }
}

const MANIFEST_MAJOR: u8 = 0xFF;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
struct ClientVersionRequest {
    major: u8,
    minor: u8,
    range: u8,
}

impl ClientVersionRequest {
    fn new(major: u8, minor: u8, range: u8) -> NetActorResult<Self> {
        if range > minor {
            return Err(NetActorError::from_anyhow(anyhow!(
                "Range ({range}) may not be greater than minor ({minor})",
            )));
        }
        Ok(Self {
            major,
            minor,
            range,
        })
    }

    fn from_bytes(bytes: [u8; 4]) -> NetActorResult<Self> {
        let [_, range, minor, major] = bytes;
        Self::new(major, minor, range)
    }

    fn negotiate(&self, actor_config: &ActorConfig) -> Option<(u8, u8)> {
        match (actor_config.handshake_manifest_version, self.major) {
            (None, MANIFEST_MAJOR) => {
                let requested_min = self.minor - self.range;
                let requested_max = self.minor;
                let max = actor_config.bolt_version.max_handshake_manifest_version();
                debug!(
                    "Manifest style request {}",
                    if requested_min == requested_max {
                        format!("v{requested_min}")
                    } else {
                        format!("v{requested_min}-v{requested_max}")
                    }
                );
                match max {
                    0 => {
                        debug!("Server manifest max v0 => no manifest support => reject");
                        None
                    }
                    _ if max < requested_min => {
                        debug!("Server manifest max v{max} lower than requested minimum => reject");
                        None
                    }
                    _ if max < requested_max => {
                        debug!(
                            "Server manifest max v{max} is lower than requested maximum => \
                            accept server maximum"
                        );
                        Some((MANIFEST_MAJOR, max))
                    }
                    _ => {
                        debug!(
                            "Server manifest max v{max} is higher than requested maximum => \
                            accept requested maximum"
                        );
                        Some((MANIFEST_MAJOR, requested_max))
                    }
                }
            }
            (Some(0), MANIFEST_MAJOR) => {
                debug!("Non-manifest style enforced => reject manifest style request");
                None
            }
            (None, _) | (Some(0), _) => {
                let (server_major, server_minor) = actor_config.bolt_version_raw;
                if self.matches(server_major, server_minor) {
                    debug!(
                        "Non-manifest style request matches server version {:?} => accept",
                        actor_config.bolt_version_raw
                    );
                    return Some((server_major, server_minor));
                }
                for alias in actor_config.bolt_version.backwards_equivalent_versions() {
                    if self.matches(alias.0, alias.1) {
                        debug!(
                            "Non-manifest style request matches server alias {alias:?} => accept",
                        );
                        return Some(*alias);
                    }
                }
                debug!(
                    "Non-manifest style request doesn't match server version {:?} \
                    or aliases {:?} => reject",
                    actor_config.bolt_version.backwards_equivalent_versions(),
                    actor_config.bolt_version_raw
                );
                None
            }
            (Some(forced_manifest), MANIFEST_MAJOR) => {
                match self.matches(MANIFEST_MAJOR, forced_manifest) {
                    true => {
                        debug!("Enforced manifest style v{forced_manifest} matches => accept");
                        Some((MANIFEST_MAJOR, forced_manifest))
                    }
                    false => {
                        debug!(
                            "Enforced manifest style v{forced_manifest} doesn't matches => reject"
                        );
                        None
                    }
                }
            }
            (Some(forced_manifest), _) => {
                debug!(
                    "Enforced manifest style v{forced_manifest} => \
                    reject non-manifest style request"
                );
                None
            }
        }
    }

    fn accept_exact_version(&self, actor_config: &ActorConfig) -> NetActorResult<()> {
        if self.range != 0 {
            debug!("Rejecting version as it contains a range: {self:?}");
            return Err(NetActorError::from_anyhow(anyhow!(
                "Expected exact version, got range {self:?}",
            )));
        }
        if self.major == MANIFEST_MAJOR {
            debug!("Rejecting version as it is a manifest marker: {self:?}");
            return Err(NetActorError::from_anyhow(anyhow!(
                "Expected exact version, got manifest version {self:?}",
            )));
        }
        let (major, minor) = actor_config.bolt_version_raw;
        if self.major != major || self.minor != minor {
            debug!(
                "Rejecting version as it doesn't match server version {:?}: {self:?}",
                actor_config.bolt_version_raw
            );
            return Err(NetActorError::from_anyhow(anyhow!(
                "Expected exact version {:?}, got {self:?}",
                actor_config.bolt_version_raw,
            )));
        }
        Ok(())
    }

    fn matches(&self, major: u8, minor: u8) -> bool {
        self.major == major && self.minor == minor
            || (self.major.saturating_sub(self.range)..self.major).contains(&minor)
    }
}

fn swallow_anyhow_error(res: NetActorResult<()>) -> NetActorResult<()> {
    match res {
        Err(NetActorError::Anyhow(e)) => {
            debug!("Swallowed IO error: {e:#}");
            Ok(())
        }
        res => res,
    }
}
