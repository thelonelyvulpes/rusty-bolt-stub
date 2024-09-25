use crate::parser::ActorScript;
use crate::types::actor_types::ActorBlock;
use anyhow::{anyhow, Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

pub struct NetActor<T> {
    pub ct: CancellationToken,
    pub conn: T,
    pub name: String,
    pub script: &'static ActorScript,
}

impl<'a, T: AsyncRead + AsyncWrite + Unpin> NetActor<T> {
    pub async fn run_client_connection(&mut self) -> Result<()> {
        self.handshake().await?;

        let curr = &self.script.tree;

        match curr {
            ActorBlock::BlockList(_, _) => {
                // Top Level!
            }
            ActorBlock::ClientMessageValidate(_, _) => {
                // recv and die.
            }
            ActorBlock::ServerMessageSend(_, _) => {
                // send and die
            }
            ActorBlock::Python(_, _) => {
                // run some python and die.
            }
            ActorBlock::Alt(_, _) => {}
            ActorBlock::Optional(_, _) => {}
            ActorBlock::Repeat(_, _, _) => {}
            ActorBlock::AutoMessage(_, _) => {}
            ActorBlock::NoOp(_) => {}
        }

        Ok(())
    }

    async fn handshake(&mut self) -> Result<()> {
        let mut buffer = [0u8; 20];

        // TODO: Make cancellable
        self.conn
            .read_exact(&mut buffer)
            .await
            .context("Reading handshake and magic preamble.")?;

        if buffer[0..4] != [0x60, 0x60, 0xB0, 0x17] {
            return Err(anyhow::anyhow!("Invalid magic preamble."));
        }

        // TODO: Tidy up
        let valid_handshake = self.negotiate_bolt_version(&mut buffer);
        if !valid_handshake {
            self.send_no_negotiated_bolt_version().await?;
            return Err(anyhow::anyhow!("Invalid Bolt version"));
        }

        let to_negotiate = &self.script.config.bolt_version;
        self.conn
            .write_all(&[0x00, 0x00, to_negotiate.minor(), to_negotiate.major()])
            .await?;
        Ok(())
    }

    fn negotiate_bolt_version(&self, buffer: &mut [u8; 20]) -> bool {
        let to_negotiate = &self.script.config.bolt_version;

        for entry in 1..=4 {
            let slice = &buffer[entry * 4..entry * 4 + 4];
            let range = slice[1];
            let max_minor = slice[2];

            if slice[3] == to_negotiate.major()
                && to_negotiate.minor() >= max_minor - range
                && to_negotiate.minor() <= max_minor
            {
                return true;
            }
        }
        false
    }

    async fn send_no_negotiated_bolt_version(&mut self) -> Result<()> {
        self.conn.write_all(&[0x00, 0x00, 0x00, 0x00]).await?;
        Ok(())
    }
}
