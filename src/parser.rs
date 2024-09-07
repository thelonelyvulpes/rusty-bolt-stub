use crate::types::actor_types::{
    ActorBlock, AutoMessageHandler, ClientMessageValidator, ServerMessageSender,
};
use crate::types::{BangLine, BoltVersion, Context, ScanBlock, Script};
use itertools::Itertools;
use std::error::Error;
use std::time::Duration;
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Actor {
    pub config: ActorConfig,
    pub tree: ActorBlock,
    pub script: Script,
}

#[derive(Debug)]
pub struct ActorConfig {
    pub bolt_version: BoltVersion,
    pub allow_restart: bool,
    pub allow_concurrent: bool,
    pub handshake_delay: Option<Duration>,
    pub handshake: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct ParseError {
    pub message: String,
    pub ctx: Option<Context>,
}

impl ParseError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ctx: None,
        }
    }

    pub fn new_ctx(ctx: Context, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ctx: Some(ctx),
        }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(ctx) = &self.ctx {
            write!(f, "{}: {}", ctx, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl Error for ParseError {}

type Result<T> = std::result::Result<T, ParseError>;

pub fn contextualize_res<T>(res: Result<T>, script: &str) -> anyhow::Result<T> {
    fn script_excerpt(script: &str, start: usize, end: usize) -> String {
        const CONTEXT_LINES: usize = 3;

        let line_num_max = script.lines().count();
        let line_num_width = line_num_max.to_string().len();
        let lines = script.lines().enumerate();
        let mut excerpt_lines = Vec::<String>::with_capacity(end - start + 3 + 2 * CONTEXT_LINES);
        if start.saturating_sub(CONTEXT_LINES) > 1 {
            excerpt_lines.push(format!("  {: >width$} ...", "", width = line_num_width).into());
        }
        for (mut line_num, line) in lines {
            line_num += 1;
            if line_num < start.saturating_sub(CONTEXT_LINES) {
                // pre context
                continue;
            }
            if line_num < start {
                // context before the excerpt
                excerpt_lines.push(
                    format!("  {: >width$} {}", line_num, line, width = line_num_width).into(),
                );
                continue;
            }
            if line_num <= end {
                // the excerpt
                excerpt_lines.push(
                    format!("> {: >width$} {}", line_num, line, width = line_num_width).into(),
                );
                continue;
            }
            // context after the excerpt
            excerpt_lines
                .push(format!("  {: >width$} {}", line_num, line, width = line_num_width).into());
            if line_num >= end.saturating_add(CONTEXT_LINES) {
                // past context
                break;
            }
        }
        if end.saturating_add(CONTEXT_LINES) < line_num_max {
            excerpt_lines.push(format!("  {: >width$} ...", "", width = line_num_width).into());
        }
        excerpt_lines.join("\n")
    }

    match res {
        Ok(t) => Ok(t),
        Err(mut e) => {
            let ctx = e.ctx.take();
            Err(match ctx {
                None => anyhow::anyhow!("Error parsing script: {}", e.message),
                Some(ctx) => {
                    if ctx.start_line_number == ctx.end_line_number {
                        anyhow::anyhow!(
                            "Error parsing script on line ({}): {}\n{}",
                            ctx.start_line_number,
                            e.message,
                            script_excerpt(script, ctx.start_line_number, ctx.end_line_number)
                        )
                    } else {
                        anyhow::anyhow!(
                            "Error parsing script on lines ({}-{}): {}\n{}",
                            ctx.start_line_number,
                            ctx.end_line_number,
                            e.message,
                            script_excerpt(script, ctx.start_line_number, ctx.end_line_number)
                        )
                    }
                }
            })
        }
    }
}

pub fn parse(script: Script) -> Result<Actor> {
    let config = parse_config(&script.bang_lines)?;
    let tree = parse_block(&script.body, &config)?;

    Ok(Actor {
        config,
        tree,
        script,
    })
}

fn parse_config(bang_lines: &[BangLine]) -> Result<ActorConfig> {
    let mut bolt_version: Option<BoltVersion> = None;
    let mut allow_restart: Option<bool> = None;
    let mut allow_concurrent: Option<bool> = None;
    let mut handshake: Option<Vec<u8>> = None;
    let mut handshake_delay: Option<Duration> = None;

    for bang_line in bang_lines {
        match bang_line {
            BangLine::Version(ctx, major, minor) => {
                if bolt_version.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple BOLT version bang lines found",
                    ));
                }
                let bolt = BoltVersion::match_valid_version(*major, minor.unwrap_or_default())
                    .ok_or(ParseError::new_ctx(*ctx, "Invalid BOLT version"))?;
                bolt_version = Some(bolt);
            }
            BangLine::AllowRestart(ctx) => {
                if allow_restart.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple allow restart bang lines found",
                    ));
                }
                allow_restart = Some(true);
            }
            BangLine::Auto(_, _) => {
                todo!("Auto blocks are not yet supported in the actor")
            }
            BangLine::Concurrent(ctx) => {
                if allow_concurrent.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple allow concurrent bang lines found",
                    ));
                }
                allow_concurrent = Some(true);
            }
            BangLine::Handshake(ctx, byte_str) => {
                if handshake.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple handshake bang lines found",
                    ));
                }
                todo!("Parse handshake bytes")
            }
            BangLine::HandshakeDelay(_, _) => {
                todo!("Parse handshake delay")
            }
            BangLine::Python(_, _) => {
                todo!("Python blocks are not yet supported in the actor")
            }
        }
    }

    let bolt_version = bolt_version.ok_or(ParseError::new("Bolt version not specified"))?;
    let allow_restart = allow_restart.unwrap_or(false);
    let allow_concurrent = allow_concurrent.unwrap_or(false);
    Ok(ActorConfig {
        bolt_version,
        allow_restart,
        allow_concurrent,
        handshake,
        handshake_delay,
    })
}

fn parse_block(block: &ScanBlock, config: &ActorConfig) -> Result<ActorBlock> {
    match block {
        ScanBlock::List(ctx, scan_blocks) => {
            let mut actor_blocks = Vec::with_capacity(scan_blocks.len());
            for scan_block in scan_blocks {
                let b = parse_block(scan_block, config)?;
                if matches!(b, ActorBlock::NoOp(_)) {
                    continue;
                }
                actor_blocks.push(b);
            }
            validate_list_children(&actor_blocks)?;

            match actor_blocks.len() {
                0 => Ok(ActorBlock::NoOp(*ctx)),
                1 => Ok(actor_blocks.remove(0)),
                _ => {
                    // TODO: if to sibling blocks are lists, merge them
                    Ok(ActorBlock::BlockList(*ctx, actor_blocks))
                }
            }
        }
        ScanBlock::Alt(ctx, scan_blocks) => {
            let mut actor_blocks = Vec::with_capacity(scan_blocks.len());
            for block in scan_blocks {
                let b = parse_block(block, config)?;
                validate_alt_child(&b)?;
                actor_blocks.push(b);
            }
            Ok(ActorBlock::Alt(*ctx, actor_blocks))
        }
        ScanBlock::Parallel(_, _) => {
            todo!("Parallel blocks are not yet supported in the actor")
        }
        ScanBlock::Optional(ctx, optional_scan_block) => {
            // TODO: Handle bad optional blocks
            let b = parse_block(optional_scan_block, config)?;
            validate_non_action(&b)?;
            Ok(ActorBlock::Optional(*ctx, Box::new(b)))
        }
        ScanBlock::Repeat0(ctx, b) => Ok(ActorBlock::Repeat(
            *ctx,
            Box::new(parse_block(b, config)?),
            0,
        )),
        ScanBlock::Repeat1(ctx, b) => Ok(ActorBlock::Repeat(
            *ctx,
            Box::new(parse_block(b, config)?),
            1,
        )),
        ScanBlock::ClientMessage(ctx, message_name, body_string) => {
            let validator = create_validator(message_name, body_string, config)?;
            Ok(ActorBlock::ClientMessageValidate(*ctx, validator))
        }
        ScanBlock::ServerMessage(ctx, message_name, body_string) => {
            let server_message_sender = create_message_sender(message_name, body_string, config)?;
            Ok(ActorBlock::ServerMessageSend(*ctx, server_message_sender))
        }
        ScanBlock::AutoMessage(ctx, client_message_name, client_body_string) => {
            let validator = create_validator(client_message_name, client_body_string, config)?;
            let server_message_sender = create_auto_message_sender(client_message_name, config)?;
            Ok(ActorBlock::AutoMessage(
                *ctx,
                AutoMessageHandler {
                    client_validator: validator,
                    server_sender: server_message_sender,
                },
            ))
        }
        ScanBlock::Comment(ctx) => Ok(ActorBlock::NoOp(*ctx)),
        ScanBlock::Python(_, _) => {
            todo!("Python blocks are not yet supported in the actor")
        }
        ScanBlock::Condition(_, _) => {
            todo!("Python blocks are not yet supported in the actor")
        }
    }
}

fn create_auto_message_sender(
    client_message_tag: &str,
    config: &ActorConfig,
) -> Result<Box<dyn ServerMessageSender>> {
    todo!()
}

fn create_message_sender(
    message_name: &str,
    message_body: &Option<String>,
    config: &ActorConfig,
) -> Result<Box<dyn ServerMessageSender>> {
    todo!()
}

fn create_validator(
    expected_tag: &str,
    expected_body_string: &Option<String>,
    config: &ActorConfig,
) -> Result<Box<dyn ClientMessageValidator>> {
    todo!()
}

fn message_tag_from_name(tag_name: &str, config: &ActorConfig) -> Result<u8> {
    todo!()
}

fn validate_non_action(p0: &ActorBlock) -> Result<()> {
    todo!()
}

fn validate_list_children(blocks: &[ActorBlock]) -> Result<()> {
    todo!()
}

fn validate_alt_child(b: &ActorBlock) -> Result<()> {
    if let ActorBlock::NoOp(ctx) = b {
        return Err(ParseError::new_ctx(*ctx, "Empty branch in Alt block"));
    }
    Ok(())
}
