mod handshake;
mod logging;

use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::{atomic, Arc};

use anyhow::{anyhow, Context as AnyhowContext};
use logging::{debug, info};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::bolt_version::BoltVersion;
use crate::context::Context;
use crate::net_actor::logging::{trace, HasLoggingCtx, LoggingCtx};
use crate::parser::ActorScript;
use crate::str_bytes::fmt_bytes;
use crate::types::actor_types::{
    ActorBlock, AutoMessageHandler, ClientMessageValidator, ScriptLine, ServerAction,
    ServerActionLine, ServerMessageSender,
};
use crate::values;
use crate::values::bolt_message::BoltMessage;
use crate::values::pack_stream_value::PackStreamStruct;

type NetActorResult<T> = Result<T, NetActorError>;

#[derive(Debug)]
enum NetActorError {
    Anyhow(anyhow::Error),
    Cancellation(String),
    Exit,
}

impl Display for NetActorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NetActorError::Anyhow(err) => err.fmt(f),
            NetActorError::Cancellation(reason) => reason.fmt(f),
            NetActorError::Exit => write!(f, "<EXIT> signal"),
        }
    }
}

impl Error for NetActorError {}

impl NetActorError {
    fn from_anyhow(err: anyhow::Error) -> Self {
        Self::Anyhow(err)
    }

    fn from_cancellation(reason: String) -> Self {
        Self::Cancellation(reason)
    }
}

impl From<anyhow::Error> for NetActorError {
    fn from(err: anyhow::Error) -> Self {
        Self::Anyhow(err)
    }
}

mod private {
    pub(super) trait Sealed {}
    impl Sealed for tokio::net::TcpStream {}
}

#[allow(private_bounds)]
pub trait Connection: AsyncRead + AsyncWrite + Unpin + private::Sealed {
    fn addresses(&self) -> io::Result<(SocketAddr, SocketAddr)>;
}

impl Connection for TcpStream {
    fn addresses(&self) -> io::Result<(SocketAddr, SocketAddr)> {
        Ok((self.peer_addr()?, self.local_addr()?))
    }
}

pub struct NetActor<'a, C> {
    ct: CancellationToken,
    shutting_down: Arc<atomic::AtomicBool>,
    conn: C,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
    script: &'a ActorScript<'a>,
    peeked_message: Option<BoltMessage>,
}

impl<'a, C: Connection> NetActor<'a, C> {
    pub fn new(
        ct: CancellationToken,
        shutting_down: Arc<atomic::AtomicBool>,
        conn: C,
        script: &'a ActorScript,
    ) -> Self {
        let (peer_addr, local_addr) = conn
            .addresses()
            .unwrap_or((([0, 0, 0, 0], 0).into(), ([0, 0, 0, 0], 0).into()));
        NetActor {
            ct,
            shutting_down,
            conn,
            peer_addr,
            local_addr,
            script,
            peeked_message: None,
        }
    }

    pub async fn run_client_connection(&mut self) -> anyhow::Result<()> {
        info!(
            self,
            "S: <ACCEPT> {} -> {}", self.peer_addr, self.local_addr
        );
        let res = self.play_script().await;
        info!(self, "S: <HANGUP>");
        res
    }

    async fn play_script(&mut self) -> anyhow::Result<()> {
        match self.handshake().await {
            Err(NetActorError::Cancellation(reason))
                if !self.shutting_down.load(atomic::Ordering::Acquire) =>
            {
                debug!(
                    self,
                    "Ignoring NetActor error because another connection failed: {reason}",
                );
                return Ok(());
            }
            Err(err) => return Err(err.into()),
            Ok(res) => res,
        }
        let mut block = BlockWithState::new(&self.script.tree);
        match self.run_block(&mut block).await {
            Err(NetActorError::Exit) => {
                debug!(
                    self,
                    "Handling NetActorError::Exit (<EXIT>): done playing script"
                );
                Ok(())
            }
            Err(err) if block.can_skip() => {
                debug!(
                    self,
                    "Ignoring NetActor error because script reached the end: {err:#}"
                );
                info!(self, "Script finished");
                Ok(())
            }
            Err(NetActorError::Cancellation(reason))
                if !self.shutting_down.load(atomic::Ordering::Acquire) =>
            {
                debug!(
                    self,
                    "Ignoring NetActor error because another connection failed: {reason}"
                );
                info!(self, "Script finished");
                Ok(())
            }
            Ok(res) => {
                info!(self, "Script finished");
                Ok(res)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn run_block(&mut self, block: &mut BlockWithState<'_>) -> NetActorResult<()> {
        loop {
            self.server_action(block).await?;
            if block.done() {
                break;
            }
            let mut consume_res = self.try_consume(block).await;
            if let Ok(false) = consume_res {
                debug!(self, "No match in script found, trying auto bang handlers");
                consume_res = self.try_auto_bang_handler().await;
            }
            match consume_res {
                Ok(true) => {}
                Ok(false) => {
                    let msg = self.format_script_missmatch(block);
                    return Err(NetActorError::Anyhow(anyhow::Error::msg(msg)));
                }
                Err(err) => {
                    let msg = self.format_scrip_read_failure(block, &err);
                    return Err(NetActorError::Anyhow(anyhow::Error::msg(msg)));
                }
            }
        }
        Ok(())
    }

    /// Doesn't progress the state if the call fails.
    ///
    /// # Returns
    /// bool indicates if a message could be consumed `true` or there was a script mismatch `false`
    async fn try_consume(&mut self, block: &mut BlockWithState<'_>) -> anyhow::Result<bool> {
        match block {
            BlockWithState::BlockList(state, ctx, blocks) => {
                if state.current_block >= blocks.len() {
                    return Ok(false);
                }
                let blocks_len = blocks.len();
                for block in blocks.iter_mut().skip(state.current_block) {
                    if Box::pin(self.try_consume(block)).await? {
                        if block.done() {
                            state.current_block += 1;
                            debug!(
                                self,
                                "list child block done: moving block list ({}) to {}/{blocks_len}",
                                ctx,
                                state.current_block + 1
                            );
                        }
                        return Ok(true);
                    }
                    if !block.can_skip() {
                        break;
                    }
                    state.current_block += 1;
                    debug!(
                        self,
                        "list child block didn't match, but is skippable: \
                        moving block list ({}) to {}/{blocks_len}",
                        ctx,
                        state.current_block + 1
                    );
                }
                Ok(false)
            }
            BlockWithState::ClientMessageValidate(state, ctx, validator) => match state.done {
                true => Ok(false),
                false => {
                    let peeked_message = Self::peek_message(
                        self.logging_ctx(),
                        &self.ct,
                        &mut self.conn,
                        &mut self.peeked_message,
                        self.script.config.bolt_version,
                    )
                    .await?;
                    if validator.validate(peeked_message).is_err() {
                        return Ok(false);
                    }
                    _ = self.read_message(*validator).await?; // consume the message
                    debug!(self, "client line ({ctx}) matched: done");
                    state.done = true;
                    Ok(true)
                }
            },
            BlockWithState::Alt(state, ctx, blocks) => match state {
                BranchState::Init => {
                    for (i, block) in blocks.iter_mut().enumerate() {
                        if Box::pin(self.try_consume(block)).await? {
                            if block.done() {
                                debug!(
                                    self,
                                    "alt block ({ctx}) child {} started and done: moving to Done",
                                    i + 1
                                );
                                *state = BranchState::Done;
                            } else {
                                debug!(
                                    self,
                                    "alt block ({ctx}) child {} started: moving to InBlock",
                                    i + 1
                                );
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
                        debug!(
                            self,
                            "alt block ({ctx}) child {} done: moving to Done",
                            *i + 1
                        );
                        *state = BranchState::Done;
                    }
                    res
                }
                BranchState::Done => Ok(false),
            },
            BlockWithState::Parallel(state, ctx, blocks) => {
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
                    debug!(
                        self,
                        "parallel block ({ctx}) all children done: moving to Done"
                    );
                    state.done = true;
                }
                Ok(matched)
            }
            BlockWithState::Optional(state, ctx, block) => match state {
                OptionalState::Init | OptionalState::Started => {
                    let res = Box::pin(self.try_consume(block)).await?;
                    if res {
                        debug!(
                            self,
                            "optional block ({ctx}) child stared: moving to Started"
                        );
                        *state = OptionalState::Started;
                    }
                    if block.done() {
                        debug!(self, "optional block ({ctx}) child done: moving to Done");
                        *state = OptionalState::Done;
                    }
                    Ok(res)
                }
                OptionalState::Done => Ok(false),
            },
            BlockWithState::Repeat(state, ctx, block, count) => {
                if Box::pin(self.try_consume(block)).await? {
                    debug!(
                        self,
                        "repeat{count} block ({ctx}) child consumed message: InBlock count {}",
                        state.count
                    );
                    state.in_block = true;
                    return Ok(true);
                }
                if state.in_block && block.can_skip() {
                    // try form the top
                    let peeked_message = Self::peek_message(
                        self.logging_ctx(),
                        &self.ct,
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
                            debug!(
                                self,
                                "repeat{count} block ({ctx}) looping around: InBlock count {}",
                                state.count
                            );
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
                let peeked_message = Self::peek_message(
                    self.logging_ctx(),
                    &self.ct,
                    &mut self.conn,
                    &mut self.peeked_message,
                    self.script.config.bolt_version,
                )
                .await?;
                if auto_handler
                    .client_validator
                    .validate(peeked_message)
                    .is_err()
                {
                    return Ok(false);
                }
                _ = self.read_message(&*auto_handler.client_validator).await?; // consume the message
                self.write_message(&*auto_handler.server_sender)
                    .await
                    .with_context(|| format!("Reading on {ctx}"))?;
                debug!(self, "auto message ({ctx}) matched: done");
                state.done = true;
                Ok(true)
            }
            BlockWithState::ServerMessageSend(..)
            | BlockWithState::ServerActionLine(..)
            | BlockWithState::Python(..)
            | BlockWithState::NoOp(..) => {
                panic!("Should've called server_action before {block:?}")
            }
        }
    }

    async fn try_auto_bang_handler(&mut self) -> anyhow::Result<bool> {
        let peeked_message = Self::peek_message(
            self.logging_ctx(),
            &self.ct,
            &mut self.conn,
            &mut self.peeked_message,
            self.script.config.bolt_version,
        )
        .await?;
        let tag = peeked_message.tag;
        let Some(handler) = self.script.config.auto_responses.get(&tag) else {
            debug!(self, "No auto bang handler found for tag: {tag}");
            return Ok(false);
        };
        debug!(self, "Found auto bang handler for tag: {tag}");
        _ = self.read_message(&handler.ctx).await?; // consume the message
        self.write_message(&*handler.sender)
            .await
            .inspect_err(|err| info!(self, "Error sending message: {err}"))?;
        Ok(true)
    }

    /// Progresses the state even if the call fails.
    async fn server_action(&mut self, block: &mut BlockWithState<'_>) -> NetActorResult<()> {
        match block {
            BlockWithState::BlockList(state, ctx, blocks) => {
                let blocks_len = blocks.len();
                let mut error = None;
                loop {
                    let Some(block) = blocks.get_mut(state.current_block) else {
                        break;
                    };
                    let res = Box::pin(self.server_action(block)).await;
                    if let Err(err) = res {
                        error.get_or_insert(err);
                    }
                    if !block.done() {
                        break;
                    }
                    state.current_block += 1;
                    debug!(
                        self,
                        "list child block done: moving block list ({}) to {}/{blocks_len}",
                        ctx,
                        state.current_block + 1
                    );
                }
                match error {
                    None => Ok(()),
                    Some(err) => Err(err),
                }
            }
            BlockWithState::ServerMessageSend(state, ctx, sender) => match state.done {
                true => Ok(()),
                false => {
                    let res = self
                        .write_message(*sender)
                        .await
                        .inspect_err(|err| info!(self, "Error sending message: {err}"));
                    state.done = true;
                    debug!(self, "server line ({ctx}): done");
                    res
                }
            },
            BlockWithState::ServerActionLine(state, ctx, action) => match state.done {
                true => Ok(()),
                false => {
                    let res = self
                        .server_action_line(*action)
                        .await
                        .inspect_err(|err| info!(self, "Error on server action: {err}"));
                    state.done = true;
                    debug!(self, "server action ({ctx}): done");
                    res
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
            BlockWithState::Alt(state, ctx, blocks) => match state {
                BranchState::Init => Ok(()),
                BranchState::InBlock(i) => {
                    let res = Box::pin(self.server_action(&mut blocks[*i])).await;
                    if blocks[*i].done() {
                        debug!(
                            self,
                            "alt block ({ctx}) child {} done: moving to Done",
                            *i + 1
                        );
                        *state = BranchState::Done
                    }
                    res
                }
                BranchState::Done => Ok(()),
            },
            BlockWithState::Parallel(state, ctx, blocks) => match state.done {
                true => Ok(()),
                false => {
                    let mut error = None;
                    for block in blocks.iter_mut() {
                        if block.done() {
                            continue;
                        }
                        let res = Box::pin(self.server_action(block)).await;
                        if let Err(err) = res {
                            error.get_or_insert(err);
                        }
                    }
                    if blocks.iter().all(BlockWithState::done) {
                        debug!(
                            self,
                            "parallel block ({ctx}) all children done: moving to Done"
                        );
                        state.done = true;
                    }
                    match error {
                        None => Ok(()),
                        Some(err) => Err(err),
                    }
                }
            },
            BlockWithState::Optional(state, ctx, block) => match state {
                // optional block children cannot start with an action block
                OptionalState::Init => Ok(()),
                OptionalState::Started => {
                    let res = Box::pin(self.server_action(block)).await;
                    if block.done() {
                        debug!(self, "optional block ({ctx}) child done: moving to Done");
                        *state = OptionalState::Done;
                    }
                    res
                }
                OptionalState::Done => Ok(()),
            },
            BlockWithState::Repeat(state, ctx, block, count) => match state.in_block {
                true => {
                    let res = Box::pin(self.server_action(block)).await;
                    if block.done() {
                        state.in_block = false;
                        state.count += 1;
                        debug!(
                            self,
                            "repeat{count} block ({ctx}) reached end: count {}", state.count
                        );
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

    fn format_script_missmatch(&self, current_block: &BlockWithState<'_>) -> String {
        let received = match self.peeked_message.as_ref() {
            None => String::from("Nothing? Huh... This shouldn't have happened!"),
            Some(msg) => msg.repr(),
        };
        let script_name = &self.script.script.name;
        let possible_verifiers = self.format_possible_verifiers(current_block);
        let one_of = match possible_verifiers.len() {
            0 => "... nothing? Huh. This shouldn't have happened!",
            1 => "\n",
            _ => " one of:\n",
        };
        let possible_verifiers = possible_verifiers.join("\n");

        format!(
            "Script mismatch in {script_name:?}:\n\
            Expected{one_of}\
            {possible_verifiers}\n\n\
            Received:\n\
            {received}"
        )
    }

    fn format_scrip_read_failure(
        &self,
        current_block: &BlockWithState<'_>,
        err: &anyhow::Error,
    ) -> String {
        let script_name = &self.script.script.name;
        let possible_verifiers = self.format_possible_verifiers(current_block);
        let one_of = match possible_verifiers.len() {
            0 => "... nothing? Huh. This shouldn't have happened!",
            1 => "\n",
            _ => " one of:\n",
        };
        let possible_verifiers = possible_verifiers.join("\n");

        format!(
            "Read failure in {script_name:?}: {err:#}\n\
            Expected{one_of}\
            {possible_verifiers}",
        )
    }

    fn format_possible_verifiers(&self, current_block: &BlockWithState<'_>) -> Vec<String> {
        let possible_verifiers = &mut Vec::new();
        Self::current_verifiers(current_block, possible_verifiers);
        self.script
            .config
            .auto_responses
            .values()
            .map(|handler| {
                let line = handler.ctx.original_line(self.script.script.input);
                let line_number = handler.ctx.start_line_number;
                format!("({line_number:4}) !: AUTO {line}")
            })
            .chain(possible_verifiers.iter().map(|v| {
                let line = v.line_repr(self.script.script.input);
                match v.line_number() {
                    None => format!("(   ?) C: {line}"),
                    Some(line_number) => format!("({line_number:4}) C: {line}"),
                }
            }))
            .collect()
    }

    fn current_verifiers<'b>(
        block: &BlockWithState<'b>,
        res: &mut Vec<&'b dyn ClientMessageValidator>,
    ) {
        match block {
            BlockWithState::BlockList(state, _, blocks) => {
                for block in blocks.iter().skip(state.current_block) {
                    Self::current_verifiers(block, res);
                    if !block.can_skip() {
                        break;
                    }
                }
            }
            BlockWithState::ClientMessageValidate(state, _, validator) => {
                if !state.done {
                    res.push(*validator);
                }
            }
            BlockWithState::Alt(state, _, blocks) => match state {
                BranchState::Init => {
                    for block in blocks {
                        Self::current_verifiers(block, res);
                    }
                }
                BranchState::InBlock(i) => {
                    Self::current_verifiers(&blocks[*i], res);
                }
                BranchState::Done => {}
            },
            BlockWithState::Parallel(state, _, blocks) => {
                if state.done {
                    return;
                }
                for block in blocks {
                    Self::current_verifiers(block, res);
                }
            }
            BlockWithState::Optional(state, _, block) => match state {
                OptionalState::Init | OptionalState::Started => Self::current_verifiers(block, res),
                OptionalState::Done => {}
            },
            BlockWithState::Repeat(state, _, block, _) => {
                if !state.in_block {
                    return Self::current_verifiers(block, res);
                }
                let mut sub_res = Vec::new();
                Self::current_verifiers(block, &mut sub_res);
                if !block.can_skip() {
                    res.extend(sub_res);
                    return;
                }
                Self::current_verifiers(&state.initial_state, &mut sub_res);
                let mut reported_verifiers = HashSet::with_capacity(sub_res.len());
                for verifier in sub_res {
                    if reported_verifiers.insert(verifier as *const _) {
                        res.push(verifier);
                    }
                }
            }
            BlockWithState::AutoMessage(state, _, auto_handler) => {
                if !state.done {
                    res.push(&*auto_handler.client_validator);
                }
            }
            BlockWithState::ServerMessageSend(..)
            | BlockWithState::ServerActionLine(..)
            | BlockWithState::Python(..)
            | BlockWithState::NoOp(..) => {}
        }
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
            | BlockWithState::ServerActionLine(..)
            | BlockWithState::Python(..)
            | BlockWithState::NoOp(..) => {
                panic!("Should've called server_action before: {block:?}")
            }
        }
    }

    async fn read_message(&mut self, line: &dyn ScriptLine) -> anyhow::Result<BoltMessage> {
        if let Some(peeked) = self.peeked_message.take() {
            info!(self, "{}", self.fmt_reader_message(&peeked, line));
            return Ok(peeked);
        }
        let res = Self::read_unbuffered_message(
            self.logging_ctx(),
            &self.ct,
            &mut self.conn,
            self.script.config.bolt_version,
        )
        .await
        .inspect_err(|err| info!(self, "Error reading message: {err}"))?;
        info!(self, "{}", self.fmt_reader_message(&res, line));
        Ok(res)
    }

    fn fmt_reader_message(&self, msg: &BoltMessage, sender: &dyn ScriptLine) -> String {
        let line = msg.repr();
        match sender.line_number() {
            None => format!("(   ?) C: {line}"),
            Some(line_number) => format!("({line_number:4}) C: {line}"),
        }
    }

    async fn read_unbuffered_message(
        logging_ctx: LoggingCtx,
        ct: &'_ CancellationToken,
        data_stream: &'_ mut C,
        bolt_version: BoltVersion,
    ) -> NetActorResult<BoltMessage> {
        let mut nibble_buffer = [0u8; 2];
        let mut message_buffer = Vec::with_capacity(32);
        let mut curr_idx = 0usize;
        loop {
            cancelable_io(
                "reading chunk header",
                logging_ctx,
                ct,
                data_stream.read_exact(&mut nibble_buffer),
            )
            .await?;
            let chunk_length = u16::from_be_bytes(nibble_buffer) as usize;
            if chunk_length == 0usize {
                break;
            }
            message_buffer.extend((0..chunk_length).map(|_| 0));
            cancelable_io(
                "reading chunk content",
                logging_ctx,
                ct,
                data_stream.read_exact(&mut message_buffer[curr_idx..curr_idx + chunk_length]),
            )
            .await?;
            curr_idx += chunk_length;
        }

        trace!(logging_ctx, "Read message: {}", fmt_bytes(&message_buffer));
        parse_message(message_buffer, bolt_version)
    }

    async fn write_message(&mut self, sender: &dyn ServerMessageSender) -> NetActorResult<()> {
        info!(self, "{}", self.fmt_sender_message(sender));
        let mut data = sender.send()?;
        trace!(self, "Writing message: {}", fmt_bytes(data));
        while !data.is_empty() {
            let chunk_size = data.len().min(0xFFFF);
            cancelable_io(
                "writing chunk header",
                self.logging_ctx(),
                &self.ct,
                self.conn.write_all(&(chunk_size as u16).to_be_bytes()),
            )
            .await?;
            cancelable_io(
                "writing chunk body",
                self.logging_ctx(),
                &self.ct,
                self.conn.write_all(&data[..chunk_size]),
            )
            .await?;
            data = &data[chunk_size..];
        }

        cancelable_io(
            "writing message end",
            self.logging_ctx(),
            &self.ct,
            self.conn.write_all(&[0, 0]),
        )
        .await?;

        cancelable_io(
            "flushing message end",
            self.logging_ctx(),
            &self.ct,
            self.conn.flush(),
        )
        .await?;
        Ok(())
    }

    fn fmt_sender_message(&self, sender: &dyn ServerMessageSender) -> String {
        let line = sender.line_repr(self.script.script.input);
        match sender.line_number() {
            None => format!("(AUTO) S: {line}"),
            Some(line_number) => format!("({line_number:4}) S: {line}"),
        }
    }

    async fn server_action_line(&mut self, action: &dyn ServerActionLine) -> NetActorResult<()> {
        info!(self, "{}", self.fmt_server_action_line(action));
        match action.get_action() {
            ServerAction::Exit => {
                trace!(self, "¡Hasta la vista ✌️! Closing the connection.");
                Err(NetActorError::Exit)
            }
            ServerAction::Shutdown => {
                trace!(
                    self,
                    "It was an honor, captain 🫡! \
                    See you on the other side of `std::process::exit(0)`."
                );
                std::process::exit(0);
            }
            ServerAction::Noop => {
                trace!(self, "Writing noop: {}", fmt_bytes(&[0, 0]));
                cancelable_io(
                    "writing noop",
                    self.logging_ctx(),
                    &self.ct,
                    self.conn.write_all(&[0, 0]),
                )
                .await?;
                cancelable_io(
                    "flushing noop",
                    self.logging_ctx(),
                    &self.ct,
                    self.conn.flush(),
                )
                .await
            }
            ServerAction::Raw(data) => {
                trace!(self, "Writing raw data: {}", fmt_bytes(data));
                cancelable_io(
                    "writing raw data",
                    self.logging_ctx(),
                    &self.ct,
                    self.conn.write_all(data),
                )
                .await?;
                cancelable_io(
                    "flushing raw data",
                    self.logging_ctx(),
                    &self.ct,
                    self.conn.flush(),
                )
                .await
            }
            ServerAction::Sleep(duration) => {
                trace!(
                    self,
                    "On sec, please 🥱... Sleeping for {} seconds",
                    duration.as_secs_f64()
                );
                select! {
                    _ = tokio::time::sleep(*duration) => {Ok(())}
                    _ = self.ct.cancelled() => {
                        let msg = "Sleeping cancelled";
                        debug!(self, "{msg}");
                        Err(NetActorError::from_cancellation(String::from(msg)))
                    }
                }
            }
            ServerAction::AssertOrder(duration) => {
                trace!(
                    self,
                    "Waiting for {} seconds to verify that no pipelined message arrives",
                    duration.as_secs_f64()
                );
                select! {
                    _ = tokio::time::sleep(*duration) => {Ok(())}
                    peeked_message = Self::peek_message(
                        self.logging_ctx(),
                        &self.ct,
                        &mut self.conn,
                        &mut self.peeked_message,
                        self.script.config.bolt_version,
                    ) => {
                        let peeked_message = peeked_message?;
                        Err(NetActorError::Anyhow(anyhow!(
                            "Expected no pipelined message, received {}",
                            peeked_message.repr()
                        )))
                    }
                }
            }
        }
    }

    fn fmt_server_action_line(&self, action: &dyn ServerActionLine) -> String {
        let line = action.line_repr(self.script.script.input);
        match action.line_number() {
            None => format!("(   ?) S: {line}"),
            Some(line_number) => format!("({line_number:4}) S: {line}"),
        }
    }

    async fn peek_message<'buf>(
        logging_ctx: LoggingCtx,
        ct: &CancellationToken,
        conn: &'_ mut C,
        message_buffer: &'buf mut Option<BoltMessage>,
        bolt_version: BoltVersion,
    ) -> NetActorResult<&'buf BoltMessage> {
        trace!(logging_ctx, "Peeking message");
        if message_buffer.is_none() {
            let a: BoltMessage =
                Self::read_unbuffered_message(logging_ctx, ct, conn, bolt_version).await?;
            message_buffer.replace(a);
            trace!(logging_ctx, "Buffered new message");
        }
        Ok(message_buffer.as_ref().unwrap())
    }
}

#[derive(Debug, Clone)]
enum BlockWithState<'a> {
    BlockList(ListState, Context, Vec<BlockWithState<'a>>),
    ClientMessageValidate(OneShotState, Context, &'a dyn ClientMessageValidator),
    ServerMessageSend(OneShotState, Context, &'a dyn ServerMessageSender),
    ServerActionLine(OneShotState, Context, &'a dyn ServerActionLine),
    // TODO: bring Python in
    #[allow(dead_code)]
    Python(OneShotState, Context, &'a str),
    Alt(BranchState, Context, Vec<BlockWithState<'a>>),
    Parallel(OneShotState, Context, Vec<BlockWithState<'a>>),
    Optional(OptionalState, Context, Box<BlockWithState<'a>>),
    Repeat(RepeatState<'a>, Context, Box<BlockWithState<'a>>, usize),
    AutoMessage(OneShotState, Context, &'a AutoMessageHandler),
    NoOp(#[allow(dead_code)] Context),
}

/*
match block {
    BlockWithState::BlockList(state, ctx, blocks) => {},
    BlockWithState::ClientMessageValidate(state, ctx, validator) => {},
    BlockWithState::ServerMessageSend(state, ctx, sender) => {},
    BlockWithState::Python(state, ctx, command) => {},
    BlockWithState::Alt(state, ctx, blocks) => {},
    BlockWithState::Parallel(state, ctx, blocks) => {},
    BlockWithState::Optional(state, ctx, block) => {},
    BlockWithState::Repeat(state, ctx, block, rep) => {},
    BlockWithState::AutoMessage(state, ctx, auto_handler) => {},
    BlockWithState::NoOp(ctx) => {},
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
            ActorBlock::ServerActionLine(ctx, line) => {
                Self::ServerActionLine(Default::default(), *ctx, line.as_ref())
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
            | BlockWithState::ServerActionLine(state, _, _)
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
            | BlockWithState::ServerActionLine(state, _, _)
            | BlockWithState::Python(state, _, _)
            | BlockWithState::AutoMessage(state, _, _) => state.done,
            BlockWithState::NoOp(_) => true,
        }
    }
}

fn parse_message(data: Vec<u8>, bolt_version: BoltVersion) -> NetActorResult<BoltMessage> {
    if data.len() <= 1 {
        return Err(NetActorError::Anyhow(anyhow!(
            "Message too short, less than or one byte was received."
        )));
    }

    let value = values::pack_stream_value::PackStreamValue::from_data_consume_all(&data)
        .context("Parsing bolt message")?;
    let values::pack_stream_value::PackStreamValue::Struct(PackStreamStruct { tag, fields }) =
        value
    else {
        return Err(NetActorError::Anyhow(anyhow!(
            "Expected a bolt message but got: {:?}.",
            value
        )));
    };
    let msg = BoltMessage::new(tag, fields, bolt_version);
    Ok(msg)
}

async fn cancelable_io<
    'a,
    T,
    E: Error + Send + Sync + 'static,
    F: Future<Output = Result<T, E>> + 'a,
>(
    ctx: &'static str,
    logging_ctx: LoggingCtx,
    ct: &'a CancellationToken,
    future: F,
) -> NetActorResult<T> {
    select! {
        res = future => {
            match res {
                Ok(res) => Ok(res),
                Err(err) => {
                    let msg = format!("IO failed {ctx}");
                    debug!(logging_ctx, "{msg}");
                    let err: anyhow::Error = err.into();
                    Err(NetActorError::from_anyhow(err.context(msg)))
                }
            }
        },
        _ = ct.cancelled() => {
            let msg = format!("IO cancelled {ctx}");
            debug!(logging_ctx, "{msg}");
            Err(NetActorError::from_cancellation(msg))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod simulate {
        use std::fmt::{Debug, Formatter};

        use anyhow::anyhow;
        use tokio::net::TcpStream;

        use super::*;
        use crate::types::actor_types::ScriptLine;

        struct TestValidator {
            pub valid: bool,
        }

        impl ScriptLine for TestValidator {
            fn line_repr<'a: 'c, 'b: 'c, 'c>(&'b self, _script: &'a str) -> &'c str {
                ""
            }
            fn line_number(&self) -> Option<usize> {
                None
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

        fn make_ctx(start_line_number: usize, end_line_number: usize) -> Context {
            Context {
                start_line_number,
                end_line_number,
                start_byte: 0,
                end_byte: 0,
            }
        }

        #[test]
        fn should_ok() {
            let test_block = ActorBlock::Alt(
                make_ctx(0, 1),
                vec![ActorBlock::BlockList(
                    make_ctx(1, 3),
                    vec![ActorBlock::ClientMessageValidate(
                        make_ctx(2, 2),
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
                make_ctx(0, 1),
                vec![ActorBlock::BlockList(
                    make_ctx(1, 3),
                    vec![ActorBlock::ClientMessageValidate(
                        make_ctx(2, 2),
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
                make_ctx(0, 1),
                vec![
                    ActorBlock::BlockList(
                        make_ctx(1, 3),
                        vec![ActorBlock::ClientMessageValidate(
                            make_ctx(2, 2),
                            Box::new(TestValidator { valid: false }),
                        )],
                    ),
                    ActorBlock::BlockList(
                        make_ctx(1, 3),
                        vec![ActorBlock::Alt(
                            make_ctx(0, 1),
                            vec![
                                ActorBlock::BlockList(
                                    make_ctx(1, 3),
                                    vec![ActorBlock::ClientMessageValidate(
                                        make_ctx(2, 2),
                                        Box::new(TestValidator { valid: false }),
                                    )],
                                ),
                                ActorBlock::BlockList(
                                    make_ctx(1, 3),
                                    vec![ActorBlock::ClientMessageValidate(
                                        make_ctx(2, 2),
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
                make_ctx(0, 1),
                vec![
                    ActorBlock::BlockList(
                        make_ctx(1, 3),
                        vec![ActorBlock::ClientMessageValidate(
                            make_ctx(2, 2),
                            Box::new(TestValidator { valid: false }),
                        )],
                    ),
                    ActorBlock::BlockList(
                        make_ctx(1, 3),
                        vec![ActorBlock::Alt(
                            make_ctx(0, 1),
                            vec![
                                ActorBlock::BlockList(
                                    make_ctx(1, 3),
                                    vec![ActorBlock::ClientMessageValidate(
                                        make_ctx(2, 2),
                                        Box::new(TestValidator { valid: false }),
                                    )],
                                ),
                                ActorBlock::BlockList(
                                    make_ctx(1, 3),
                                    vec![ActorBlock::ClientMessageValidate(
                                        make_ctx(2, 2),
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
