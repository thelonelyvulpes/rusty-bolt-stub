use anyhow::{anyhow, Context as AnyhowContext, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::bolt_version::BoltVersion;
use crate::context::Context;
use crate::parser::ActorScript;
use crate::types::actor_types::{
    ActorBlock, AutoMessageHandler, ClientMessageValidator, ServerMessageSender,
};
use crate::values;
use crate::values::value::Struct;
use crate::values::BoltMessage;

pub struct NetActor<T> {
    ct: CancellationToken,
    conn: T,
    name: String,
    script: &'static ActorScript,
    peeked_message: Option<BoltMessage>,
}

impl<T: AsyncRead + AsyncWrite + Unpin> NetActor<T> {
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
        let mut block = BlockWithState::new(&self.script.tree);
        loop {
            self.server_action(&mut block).await?;
            if block.done() {
                break;
            }
            if !self.try_consume(&mut block).await? {
                // TODO: enhance script mismatch error:
                //  * Which lines were expected?
                //  * What was received?
                return Err(anyhow!("Script mismatch"));
            }
        }
        Ok(())
    }

    /// # Returns
    /// bool indicates if a message could be consumed `true` or there was a script mismatch `false`
    async fn try_consume(&mut self, block: &mut BlockWithState<'_>) -> Result<bool> {
        match block {
            BlockWithState::BlockList(state, _, blocks) => {
                if state.current_block >= blocks.len() {
                    return Ok(false);
                }
                for block in blocks.iter_mut().skip(state.current_block) {
                    if Box::pin(self.try_consume(block)).await? {
                        state.current_block += 1;
                        return Ok(true);
                    }
                    if block.can_skip() {
                        state.current_block += 1;
                        continue;
                    }
                }
                Ok(false)
            }
            BlockWithState::ClientMessageValidate(state, _, validator) => match state.done {
                true => Ok(false),
                false => {
                    let peeked_message = Self::peek_message(
                        &mut self.conn,
                        &mut self.peeked_message,
                        self.script.config.bolt_version,
                    )
                    .await?;
                    if validator.validate(peeked_message).is_err() {
                        return Ok(false);
                    }
                    _ = self.read_message().await?; // consume the message
                    Ok(true)
                }
            },
            BlockWithState::Alt(state, _, blocks) => match state {
                BranchState::Init => {
                    for (i, block) in blocks.iter_mut().enumerate() {
                        if Box::pin(self.try_consume(block)).await? {
                            if block.done() {
                                *state = BranchState::Done;
                            } else {
                                *state = BranchState::InBlock(i);
                            }
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                BranchState::InBlock(i) => {
                    let res = Box::pin(self.try_consume(&mut blocks[*i])).await;
                    if blocks[*i].done() {
                        *state = BranchState::Done;
                    }
                    res
                }
                BranchState::Done => Ok(false),
            },
            BlockWithState::Parallel(state, _, blocks) => {
                if state.done {
                    return Ok(false);
                }
                let mut matched = false;
                for block in blocks.iter_mut() {
                    if block.done() {
                        continue;
                    }
                    if Box::pin(self.try_consume(block)).await? {
                        matched = true;
                        break;
                    }
                }
                if blocks.iter().all(BlockWithState::done) {
                    state.done = true;
                }
                Ok(matched)
            }
            BlockWithState::Optional(state, _, block) => match state {
                OptionalState::Init | OptionalState::Started => {
                    let res = Box::pin(self.try_consume(block)).await;
                    if let Ok(true) = res {
                        *state = OptionalState::Started;
                    }
                    if block.done() {
                        *state = OptionalState::Done;
                    }
                    res
                }
                OptionalState::Done => Ok(false),
            },
            BlockWithState::Repeat(state, _, block, _) => {
                if Box::pin(self.try_consume(block)).await? {
                    state.in_block = true;
                    return Ok(true);
                }
                if state.in_block && block.can_skip() {
                    // try form the top
                    let peeked_message = Self::peek_message(
                        &mut self.conn,
                        &mut self.peeked_message,
                        self.script.config.bolt_version,
                    )
                    .await?;
                    match Self::can_consume(state.initial_state.as_ref(), peeked_message) {
                        true => {
                            *block = state.initial_state.clone();
                            state.count += 1;
                            assert!(Box::pin(self.try_consume(block)).await?);
                            state.in_block = true;
                            Ok(true)
                        }
                        false => Ok(false),
                    }
                } else {
                    Ok(false)
                }
            }
            BlockWithState::AutoMessage(state, ctx, auto_handler) => {
                let message = self.read_message().await?;
                auto_handler
                    .client_validator
                    .validate(&message)
                    .context(*ctx)?;
                let response = auto_handler.server_sender.send()?;
                self.conn.write_all(response).await.context(*ctx)?;
                state.done = true;
                Ok(true)
            }
            BlockWithState::ServerMessageSend(..)
            | BlockWithState::Python(..)
            | BlockWithState::NoOp(..) => {
                panic!("Should've called server_action before: {block:?}")
            }
        }
    }

    async fn server_action(&mut self, block: &mut BlockWithState<'_>) -> Result<()> {
        match block {
            BlockWithState::BlockList(state, _, blocks) => {
                let Some(block) = blocks.get_mut(state.current_block) else {
                    return Ok(());
                };
                loop {
                    Box::pin(self.server_action(block)).await?;
                    if !block.done() {
                        return Ok(());
                    }
                    state.current_block += 1;
                }
            }
            BlockWithState::ServerMessageSend(state, ctx, sender) => match state.done {
                true => Ok(()),
                false => {
                    let data = sender.send()?;
                    self.conn.write_all(data).await.context(*ctx)?;
                    state.done = true;
                    Ok(())
                }
            },
            BlockWithState::Python(state, _, _command) => match state.done {
                true => Ok(()),
                false => {
                    todo!("Python commands not yet implemented");
                    // state.done = true;
                    // Ok(())
                }
            },
            BlockWithState::Alt(state, _, blocks) => match state {
                BranchState::Init => Ok(()),
                BranchState::InBlock(i) => {
                    Box::pin(self.server_action(&mut blocks[*i])).await?;
                    if blocks[*i].done() {
                        *state = BranchState::Done
                    }
                    Ok(())
                }
                BranchState::Done => Ok(()),
            },
            BlockWithState::Parallel(state, _, blocks) => match state.done {
                true => Ok(()),
                false => {
                    for block in blocks.iter_mut() {
                        if block.done() {
                            continue;
                        }
                        Box::pin(self.server_action(block)).await?;
                    }
                    if blocks.iter().all(BlockWithState::done) {
                        state.done = true;
                    }
                    Ok(())
                }
            },
            BlockWithState::Optional(state, _, block) => match state {
                // optional block children cannot start with an action block
                OptionalState::Init => Ok(()),
                OptionalState::Started => {
                    let res = Box::pin(self.server_action(block)).await;
                    if block.done() {
                        *state = OptionalState::Done;
                    }
                    res
                }
                OptionalState::Done => Ok(()),
            },
            BlockWithState::Repeat(state, _, block, _) => match state.in_block {
                true => {
                    let res = Box::pin(self.server_action(block)).await;
                    if block.done() {
                        state.in_block = false;
                        state.count += 1;
                    }
                    res
                }
                // repeat block children cannot start with an action block
                false => Ok(()),
            },
            BlockWithState::AutoMessage(..)
            | BlockWithState::ClientMessageValidate(..)
            | BlockWithState::NoOp(..) => Ok(()),
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

        // TODO: handle equivalent versions (e.g. a bolt 4.2 script is allowed to run on bolt 4.1)

        // TODO: handshake manifest v1

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

    /// # Returns
    /// bool indicating the message can be consumed `true` or there was a script mismatch `false`
    fn can_consume(block: &BlockWithState<'_>, message: &BoltMessage) -> bool {
        match block {
            BlockWithState::BlockList(state, _, blocks) => {
                for block in blocks.iter().skip(state.current_block) {
                    if Self::can_consume(block, message) {
                        return true;
                    }
                    if !block.can_skip() {
                        break;
                    }
                }
                false
            }
            BlockWithState::ClientMessageValidate(state, _, validator) => {
                !state.done && validator.validate(message).is_ok()
            }
            BlockWithState::Alt(state, _, child_blocks) => match state {
                BranchState::Init => child_blocks.iter().any(|b| Self::can_consume(b, message)),
                BranchState::InBlock(i) => Self::can_consume(&child_blocks[*i], message),
                BranchState::Done => false,
            },
            BlockWithState::Parallel(state, _, blocks) => {
                !state.done && blocks.iter().any(|b| Self::can_consume(b, message))
            }
            BlockWithState::Optional(state, _, block) => match state {
                OptionalState::Init | OptionalState::Started => Self::can_consume(block, message),
                OptionalState::Done => false,
            },
            BlockWithState::Repeat(state, _, block, _) => {
                Self::can_consume(block, message)
                    || (state.in_block
                        && block.can_skip()
                        && Self::can_consume(&state.initial_state, message))
            }
            BlockWithState::AutoMessage(state, _, handler) => {
                !state.done && handler.client_validator.validate(message).is_ok()
            }
            BlockWithState::ServerMessageSend(..)
            | BlockWithState::Python(..)
            | BlockWithState::NoOp(..) => {
                panic!("Should've called server_action before: {block:?}")
            }
        }
    }

    async fn read_message(&mut self) -> Result<BoltMessage> {
        if let Some(peeked) = self.peeked_message.take() {
            return Ok(peeked);
        }
        Self::read_unbuffered_message(&mut self.conn, self.script.config.bolt_version).await
    }

    async fn read_unbuffered_message(
        data_stream: &'_ mut T,
        bolt_version: BoltVersion,
    ) -> Result<BoltMessage> {
        let mut nibble_buffer = [0u8; 2];
        let mut message_buffer = Vec::with_capacity(32);
        let mut curr_idx = 0usize;
        loop {
            data_stream.read_exact(&mut nibble_buffer).await?;
            let chunk_length = u16::from_be_bytes(nibble_buffer) as usize;
            if chunk_length == 0usize {
                break;
            }
            message_buffer.try_reserve(chunk_length)?;
            data_stream
                .read_exact(&mut message_buffer[curr_idx..curr_idx + chunk_length])
                .await?;
            curr_idx += chunk_length;
        }

        parse_message(message_buffer, bolt_version)
    }

    async fn peek_message<'buf>(
        conn: &'_ mut T,
        message_buffer: &'buf mut Option<BoltMessage>,
        bolt_version: BoltVersion,
    ) -> Result<&'buf BoltMessage> {
        if message_buffer.is_none() {
            let a: BoltMessage = Self::read_unbuffered_message(conn, bolt_version).await?;
            message_buffer.replace(a);
        }
        Ok(message_buffer.as_ref().unwrap())
    }
}

#[derive(Debug, Clone)]
enum BlockWithState<'a> {
    BlockList(ListState, Context, Vec<BlockWithState<'a>>),
    ClientMessageValidate(OneShotState, Context, &'a dyn ClientMessageValidator),
    ServerMessageSend(OneShotState, Context, &'a dyn ServerMessageSender),
    Python(OneShotState, Context, &'a str),
    Alt(BranchState, Context, Vec<BlockWithState<'a>>),
    Parallel(OneShotState, Context, Vec<BlockWithState<'a>>),
    Optional(OptionalState, Context, Box<BlockWithState<'a>>),
    Repeat(RepeatState<'a>, Context, Box<BlockWithState<'a>>, usize),
    AutoMessage(OneShotState, Context, &'a AutoMessageHandler),
    NoOp(Context),
}

/*
match block {
    BlockWithState::BlockList(state, ctx, blocks) => todo!(),
    BlockWithState::ClientMessageValidate(state, ctx, validator) => todo!(),
    BlockWithState::ServerMessageSend(state, ctx, sender) => todo!(),
    BlockWithState::Python(state, ctx, command) => todo!(),
    BlockWithState::Alt(state, ctx, blocks) => todo!(),
    BlockWithState::Parallel(state, ctx, blocks) => todo!(),
    BlockWithState::Optional(state, ctx, block) => todo!(),
    BlockWithState::Repeat(state, ctx, block, rep) => todo!(),
    BlockWithState::AutoMessage(state, ctx, auto_handler) => todo!(),
    BlockWithState::NoOp(ctx) => todo!(),
}
*/

#[derive(Debug, Default, Clone)]
struct ListState {
    current_block: usize,
}

#[derive(Debug, Default, Clone)]
struct OneShotState {
    done: bool,
}

#[derive(Debug, Default, Clone)]
enum BranchState {
    #[default]
    Init,
    InBlock(usize),
    Done,
}

#[derive(Debug, Default, Clone)]
enum OptionalState {
    #[default]
    Init,
    Started,
    Done,
}

#[derive(Debug, Clone)]
struct RepeatState<'a> {
    in_block: bool,
    count: usize,
    initial_state: Box<BlockWithState<'a>>,
}

impl<'a> RepeatState<'a> {
    fn new(initial_state: Box<BlockWithState<'a>>) -> Self {
        Self {
            in_block: false,
            count: 0,
            initial_state,
        }
    }
}

impl<'a> BlockWithState<'a> {
    fn new(block: &'a ActorBlock) -> Self {
        match block {
            ActorBlock::BlockList(ctx, blocks) => Self::BlockList(
                Default::default(),
                *ctx,
                blocks.iter().map(Self::new).collect(),
            ),
            ActorBlock::ClientMessageValidate(ctx, validator) => {
                Self::ClientMessageValidate(Default::default(), *ctx, validator.as_ref())
            }
            ActorBlock::ServerMessageSend(ctx, sender) => {
                Self::ServerMessageSend(Default::default(), *ctx, sender.as_ref())
            }
            ActorBlock::Python(ctx, command) => Self::Python(Default::default(), *ctx, command),
            ActorBlock::Alt(ctx, blocks) => Self::Alt(
                Default::default(),
                *ctx,
                blocks.iter().map(Self::new).collect(),
            ),
            ActorBlock::Parallel(ctx, blocks) => Self::Parallel(
                Default::default(),
                *ctx,
                blocks.iter().map(Self::new).collect(),
            ),
            ActorBlock::Optional(ctx, block) => {
                Self::Optional(Default::default(), *ctx, Box::new(Self::new(block)))
            }
            ActorBlock::Repeat(ctx, block, rep) => Self::Repeat(
                RepeatState::new(Box::new(Self::new(block))),
                *ctx,
                Box::new(Self::new(block)),
                *rep,
            ),
            ActorBlock::AutoMessage(ctx, handler) => {
                Self::AutoMessage(Default::default(), *ctx, handler)
            }
            ActorBlock::NoOp(ctx) => Self::NoOp(*ctx),
        }
    }

    fn done(&self) -> bool {
        match self {
            BlockWithState::BlockList(state, _, blocks) => state.current_block >= blocks.len(),
            BlockWithState::Alt(state, _, blocks) => match state {
                BranchState::Init => false,
                BranchState::InBlock(i) => blocks[*i].done(),
                BranchState::Done => true,
            },
            BlockWithState::Parallel(state, _, blocks) => {
                state.done || blocks.iter().all(Self::done)
            }
            BlockWithState::Optional(state, _, _) => matches!(state, OptionalState::Done),
            BlockWithState::Repeat(_, _, _, _) => false,
            BlockWithState::ClientMessageValidate(state, _, _)
            | BlockWithState::ServerMessageSend(state, _, _)
            | BlockWithState::Python(state, _, _)
            | BlockWithState::AutoMessage(state, _, _) => state.done,
            BlockWithState::NoOp(_) => true,
        }
    }

    fn can_skip(&self) -> bool {
        match self {
            BlockWithState::BlockList(state, _, blocks) => {
                blocks.iter().skip(state.current_block).all(Self::can_skip)
            }
            BlockWithState::Alt(state, _, blocks) => match state {
                BranchState::Init => blocks.iter().any(Self::can_skip),
                BranchState::InBlock(i) => blocks[*i].can_skip(),
                BranchState::Done => true,
            },
            BlockWithState::Parallel(state, _, blocks) => {
                state.done || blocks.iter().all(Self::can_skip)
            }
            BlockWithState::Optional(state, _, block) => match state {
                OptionalState::Init | OptionalState::Done => true,
                OptionalState::Started => block.can_skip(),
            },
            BlockWithState::Repeat(state, _, block, rep) => match state.in_block {
                true => block.can_skip(),
                false => state.count >= *rep,
            },
            BlockWithState::ClientMessageValidate(state, _, _)
            | BlockWithState::ServerMessageSend(state, _, _)
            | BlockWithState::Python(state, _, _)
            | BlockWithState::AutoMessage(state, _, _) => state.done,
            BlockWithState::NoOp(_) => true,
        }
    }
}

fn parse_message(data: Vec<u8>, bolt_version: BoltVersion) -> Result<BoltMessage> {
    if data.len() <= 1 {
        return Err(anyhow!(
            "Message too short, less than or one byte was received."
        ));
    }

    let value =
        values::value::Value::from_data_consume_all(&data).context("Parsing bolt message")?;
    let values::value::Value::Struct(Struct { tag, fields }) = value else {
        return Err(anyhow!("Expected a bolt message but got: {:?}.", value));
    };
    Ok(BoltMessage::new(tag, fields, bolt_version))
}

#[cfg(test)]
mod tests {
    use super::*;

    mod simulate {
        use anyhow::anyhow;
        use std::fmt::{Debug, Formatter};
        use tokio::net::TcpStream;

        use super::*;
        use crate::types::actor_types::ScriptLine;

        struct TestValidator {
            pub valid: bool,
        }

        impl ScriptLine for TestValidator {
            fn original_line<'a>(&self, _script: &'a str) -> &'a str {
                ""
            }
        }

        impl Debug for TestValidator {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "")
            }
        }

        impl ClientMessageValidator for TestValidator {
            fn validate(&self, _: &BoltMessage) -> anyhow::Result<()> {
                match self.valid {
                    true => Ok(()),
                    false => Err(anyhow!("Not valid")),
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
                        Box::new(TestValidator { valid: true }),
                    )],
                )],
            );

            let test_block_stateful = BlockWithState::new(&test_block);
            let message = BoltMessage::new(0, vec![], BoltVersion::V4_4);
            let res = NetActor::<TcpStream>::can_consume(&test_block_stateful, &message);
            assert!(res);
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
                        Box::new(TestValidator { valid: false }),
                    )],
                )],
            );

            let test_block_stateful = BlockWithState::new(&test_block);
            let message = BoltMessage::new(0, vec![], BoltVersion::V4_4);
            let res = NetActor::<TcpStream>::can_consume(&test_block_stateful, &message);
            assert!(!res);
        }

        #[test]
        fn nested_pass() {
            let test_block = ActorBlock::Alt(
                Context {
                    start_line_number: 0,
                    end_line_number: 1,
                },
                vec![
                    ActorBlock::BlockList(
                        Context {
                            start_line_number: 1,
                            end_line_number: 3,
                        },
                        vec![ActorBlock::ClientMessageValidate(
                            Context {
                                start_line_number: 2,
                                end_line_number: 2,
                            },
                            Box::new(TestValidator { valid: false }),
                        )],
                    ),
                    ActorBlock::BlockList(
                        Context {
                            start_line_number: 1,
                            end_line_number: 3,
                        },
                        vec![ActorBlock::Alt(
                            Context {
                                start_line_number: 0,
                                end_line_number: 1,
                            },
                            vec![
                                ActorBlock::BlockList(
                                    Context {
                                        start_line_number: 1,
                                        end_line_number: 3,
                                    },
                                    vec![ActorBlock::ClientMessageValidate(
                                        Context {
                                            start_line_number: 2,
                                            end_line_number: 2,
                                        },
                                        Box::new(TestValidator { valid: false }),
                                    )],
                                ),
                                ActorBlock::BlockList(
                                    Context {
                                        start_line_number: 1,
                                        end_line_number: 3,
                                    },
                                    vec![ActorBlock::ClientMessageValidate(
                                        Context {
                                            start_line_number: 2,
                                            end_line_number: 2,
                                        },
                                        Box::new(TestValidator { valid: true }),
                                    )],
                                ),
                            ],
                        )],
                    ),
                ],
            );

            let test_block_stateful = BlockWithState::new(&test_block);
            let message = BoltMessage::new(0, vec![], BoltVersion::V4_4);
            let res = NetActor::<TcpStream>::can_consume(&test_block_stateful, &message);
            assert!(res);
        }

        #[test]
        fn nested_failure() {
            let test_block = ActorBlock::Alt(
                Context {
                    start_line_number: 0,
                    end_line_number: 1,
                },
                vec![
                    ActorBlock::BlockList(
                        Context {
                            start_line_number: 1,
                            end_line_number: 3,
                        },
                        vec![ActorBlock::ClientMessageValidate(
                            Context {
                                start_line_number: 2,
                                end_line_number: 2,
                            },
                            Box::new(TestValidator { valid: false }),
                        )],
                    ),
                    ActorBlock::BlockList(
                        Context {
                            start_line_number: 1,
                            end_line_number: 3,
                        },
                        vec![ActorBlock::Alt(
                            Context {
                                start_line_number: 0,
                                end_line_number: 1,
                            },
                            vec![
                                ActorBlock::BlockList(
                                    Context {
                                        start_line_number: 1,
                                        end_line_number: 3,
                                    },
                                    vec![ActorBlock::ClientMessageValidate(
                                        Context {
                                            start_line_number: 2,
                                            end_line_number: 2,
                                        },
                                        Box::new(TestValidator { valid: false }),
                                    )],
                                ),
                                ActorBlock::BlockList(
                                    Context {
                                        start_line_number: 1,
                                        end_line_number: 3,
                                    },
                                    vec![ActorBlock::ClientMessageValidate(
                                        Context {
                                            start_line_number: 2,
                                            end_line_number: 2,
                                        },
                                        Box::new(TestValidator { valid: false }),
                                    )],
                                ),
                            ],
                        )],
                    ),
                ],
            );

            let test_block_stateful = BlockWithState::new(&test_block);
            let message = BoltMessage::new(0, vec![], BoltVersion::V4_4);
            let res = NetActor::<TcpStream>::can_consume(&test_block_stateful, &message);
            assert!(!res);
        }
    }
}
