use crate::parser::ActorScript;
use crate::types::actor_types::ActorBlock;
use crate::values::ClientMessage;
use anyhow::{anyhow, Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::select;
use tokio_util::sync::CancellationToken;

pub struct NetActor<T> {
    ct: CancellationToken,
    conn: T,
    name: String,
    script: &'static ActorScript,
    peeked_message: Option<ClientMessage>,
}

impl<'a, T: AsyncRead + AsyncWrite + Unpin> NetActor<T> {
    pub fn new(
        ct: CancellationToken,
        conn: T,
        name: String,
        script: &'static ActorScript,
    ) -> NetActor<T> {
        NetActor {
            ct,
            conn,
            name,
            script,
            peeked_message: None,
        }
    }

    pub async fn run_client_connection(&mut self) -> Result<()> {
        self.handshake().await?;

        self.run_block(&self.script.tree).await
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
                validator.validate(&message).context(ctx)
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
                let peeked_message =
                    Self::peek_message(&mut self.conn, &mut self.peeked_message).await?;
                for alt_block in alt_blocks {
                    if Self::simulate_block(alt_block, &peeked_message).is_ok() {
                        return Box::pin(self.run_block(alt_block)).await;
                    }
                }
                Err(anyhow!("No blocks matched")).context(ctx)
            }
            ActorBlock::Optional(_, optional_block) => {
                let peeked_message =
                    Self::peek_message(&mut self.conn, &mut self.peeked_message).await?;
                if Self::simulate_block(optional_block, &peeked_message).is_ok() {
                    return Box::pin(self.run_block(optional_block)).await;
                }
                Ok(())
            }
            ActorBlock::Repeat(ctx, block, rep) => {
                let mut c = 0;
                loop {
                    let peeked_message =
                        Self::peek_message(&mut self.conn, &mut self.peeked_message).await?;
                    if Self::simulate_block(block, &peeked_message).is_ok() {
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
                handler.client_validator.validate(&message).context(ctx)?;
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

    pub fn simulate_block(curr: &ActorBlock, message: &ClientMessage) -> Result<()> {
        match curr {
            ActorBlock::BlockList(_, blocks) => {
                Self::simulate_block(blocks.first().unwrap(), message)
            }
            ActorBlock::ClientMessageValidate(ctx, validator) => validator.validate(message),
            ActorBlock::ServerMessageSend(_, _) => Ok(()),
            ActorBlock::Python(_, _) => Ok(()),
            ActorBlock::Alt(ctx, alt_blocks) => {
                for alt_block in alt_blocks {
                    if Self::simulate_block(alt_block, message).is_ok() {
                        return Ok(());
                    }
                }
                Err(anyhow!("No blocks matched"))
            }
            ActorBlock::Optional(ctx, optional_block) => {
                Self::simulate_block(optional_block, message)
            }
            ActorBlock::Repeat(ctx, block, _) => Self::simulate_block(block, message),
            ActorBlock::AutoMessage(ctx, handler) => handler.client_validator.validate(message),
            ActorBlock::NoOp(_) => Ok(()),
        }
    }

    async fn read_message(&mut self) -> Result<ClientMessage> {
        if let Some(peeked) = self.peeked_message.take() {
            return Ok(peeked);
        }
        Self::read_unbuffered_message(&mut self.conn).await
    }

    async fn read_unbuffered_message(_: &'_ mut T) -> Result<ClientMessage> {
        todo!("rouven to do!")
    }

    async fn peek_message<'buf>(
        conn: &'_ mut T,
        message_buffer: &'buf mut Option<ClientMessage>,
    ) -> Result<&'buf ClientMessage> {
        if message_buffer.is_none() {
            let a: ClientMessage = Self::read_unbuffered_message(conn).await?;
            message_buffer.replace(a);
        }
        Ok(message_buffer.as_ref().unwrap())
    }
}

#[cfg(test)]
mod tests {
    mod simulate {
        use crate::types::actor_types::{ActorBlock, ClientMessageValidator, ScriptLine};
        use crate::types::Context;
        use crate::values::ClientMessage;
        use std::fmt::{Debug, Formatter};
        use anyhow::anyhow;
        use tokio::net::TcpStream;
        use crate::net_actor::NetActor;

        struct TestValidator {
            pub valid: bool
        }

        impl ScriptLine for TestValidator {
            fn original_line(&self) -> &str {
                ""
            }
        }

        impl Debug for TestValidator {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "")
            }
        }

        impl ClientMessageValidator for TestValidator {
            fn validate(&self, message: &ClientMessage) -> anyhow::Result<()> {
                match self.valid {
                    true => {
                        Ok(())
                    }
                    false => {
                        Err(anyhow!("Not valid"))
                    }
                }
            }
        }

        #[test]
        fn should_ok() {
            let test_block = ActorBlock::Alt(
                Context {
                    start_line_number: 0,
                    end_line_number: 1,
                },
                vec![ActorBlock::BlockList(
                    Context {
                        start_line_number: 1,
                        end_line_number: 3,
                    },
                    vec![ActorBlock::ClientMessageValidate(
                        Context {
                            start_line_number: 2,
                            end_line_number: 2,
                        },
                        Box::new(TestValidator {valid: true}),
                    )],
                )],
            );
            let message = ClientMessage::new(0, vec![]);
            let res = NetActor::<TcpStream>::simulate_block(&test_block, &message);
            assert!(res.is_ok());
        }
        
        #[test]
        fn should_fail() {
            let test_block = ActorBlock::Alt(
                Context {
                    start_line_number: 0,
                    end_line_number: 1,
                },
                vec![ActorBlock::BlockList(
                    Context {
                        start_line_number: 1,
                        end_line_number: 3,
                    },
                    vec![ActorBlock::ClientMessageValidate(
                        Context {
                            start_line_number: 2,
                            end_line_number: 2,
                        },
                        Box::new(TestValidator {valid: false}),
                    )],
                )],
            );
            let message = ClientMessage::new(0, vec![]);
            let res = NetActor::<TcpStream>::simulate_block(&test_block, &message);
            assert!(res.is_err());
        }
    }
}
