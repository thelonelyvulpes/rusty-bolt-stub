use crate::parser::ActorScript;
use crate::types::actor_types::ActorBlock;
use crate::values::ClientMessage;
use anyhow::{anyhow, Context, Result};
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::select;
use tokio_util::sync::CancellationToken;

pub struct NetActor<T> {
    pub ct: CancellationToken,
    pub conn: T,
    pub name: String,
    pub script: &'static ActorScript,
    pub peeked_message: Option<ClientMessage>
}

impl<'a, T: AsyncRead + AsyncWrite + Unpin> NetActor<T> {
    pub async fn run_client_connection(&mut self) -> Result<()> {
        self.handshake().await?;

        let r = self.run_block(&self.script.tree).await?;
        Ok(())
    }

    async fn run_block(&mut self, curr: &'static ActorBlock) -> Result<()> {
        match curr {
            ActorBlock::BlockList(ctx, blocks) => {
                for block in blocks {
                    Box::pin(self.run_block(block)).await.context(ctx)?;
                }
                Ok(())
            }
            ActorBlock::ClientMessageValidate(ctx, validator) => {
                let message = self.read_message().await?;
                validator.validate(message).context(ctx)
            }
            ActorBlock::ServerMessageSend(ctx, data) => {
                // send and die
                let d = data.send()?;
                self.conn.write_all(&d).await.context(ctx)
            }
            ActorBlock::Python(_, _) => {
                // run some python and die.
                todo!();
            }
            ActorBlock::Alt(ctx, alt_blocks) => {
                self.peek_message().await?;
                let peeked_message = self.peeked_message().expect("peeked above");
                for alt_block in alt_blocks {
                    if Self::simulate(alt_block, &peeked_message).is_ok() {
                        return Box::pin(self.run_block(alt_block)).await;
                    }
                }
                Err(anyhow!("No blocks matched")).context(ctx)
            }
            ActorBlock::Optional(_, optional_block) => {
                self.peek_message().await?;
                let peeked_message = self.peeked_message().expect("peeked above");
                if Self::simulate(optional_block, &peeked_message).is_ok() {
                    return Box::pin(self.run_block(optional_block)).await;
                }
                Ok(())
            }
            ActorBlock::Repeat(ctx, block, rep) => {
                let mut c = 0;
                loop {
                    self.peek_message().await?;
                    let peeked_message = self.peeked_message().expect("peeked above");
                    if Self::simulate(block, &peeked_message).is_ok() {
                        Box::pin(self.run_block(block)).await?;
                        c += 1;
                    } else {
                        break;
                    }
                }
                if c < *rep {
                    return Err(anyhow!(
                        "Expected {rep} repetitions but only {c} were completed."
                    ))
                    .context(ctx);
                }
                Ok(())
            }
            ActorBlock::AutoMessage(ctx, handler) => {
                let message = self.read_message().await?;
                handler.client_validator.validate(message).context(ctx)?;
                let d = handler.server_sender.send()?;
                self.conn.write_all(&d).await.context(ctx)
            }
            ActorBlock::NoOp(_) => Ok(()),
        }
    }

    async fn handshake(&mut self) -> Result<()> {
        let mut buffer = [0u8; 20];

        select! {
            res = self.conn.read_exact(&mut buffer) => {
                res.context("Reading handshake and magic preamble.")?;
            },
            _ = self.ct.cancelled() => {
                return Err(anyhow!("no good"));
            }
        }
        // TODO: Make cancellable

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

    async fn read_message(&mut self) -> Result<ClientMessage> {
        if let Some(peeked) = self.peeked_message.take() {
            return Ok(peeked);
        }

        todo!("rouven to do!")
    }

    fn simulate(p0: &ActorBlock, p1: &ClientMessage) -> Result<()> {
        todo!()
    }

    async fn peek_message(&mut self) -> Result<()> {
        if self.peeked_message.is_none() {
            todo!("rouven to do!")
        }
        Ok(())
    }

    fn peeked_message(&self) -> Option<&ClientMessage> {
        self.peeked_message.as_ref()
    }
}
