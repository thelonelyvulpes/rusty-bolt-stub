use crate::types::actor_types::{
    ActorBlock, AutoMessageHandler, ClientMessageValidator, ServerMessageSender,
};
use crate::types::{BangLine, BoltVersion, ScanBlock, Script};
use anyhow::{anyhow, Result};
use itertools::Itertools;
use nom::character::complete::{char, hex_digit1};
use nom::character::is_hex_digit;
use nom::{AsChar, HexDisplay};
use std::time::Duration;
use tokio::net::TcpStream;

pub struct Actor {
    pub config: ActorConfig,
    pub tree: ActorBlock,
    pub script: Script,
}

pub struct ActorConfig {
    pub bolt_version: BoltVersion,
    pub allow_restart: bool,
    pub allow_concurrent: bool,
    pub handshake_delay: Option<Duration>,
    pub handshake: Option<Vec<u8>>,
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
            BangLine::Version(_, major, minor) => {
                if bolt_version.is_some() {
                    return Err(anyhow::anyhow!("Multiple BOLT version bang lines found"));
                }
                let bolt = BoltVersion::match_valid_version(*major, minor.unwrap_or_default())
                    .ok_or(anyhow::anyhow!("Invalid BOLT version"))?;
                bolt_version = Some(bolt);
            }
            BangLine::AllowRestart(_) => {
                if allow_restart.is_some() {
                    return Err(anyhow::anyhow!("Multiple allow restart bang lines found"));
                }
                allow_restart = Some(true);
            }
            BangLine::Auto(_, _) => {
                todo!("Auto blocks are not yet supported in the actor")
            }
            BangLine::Concurrent(_) => {
                if allow_concurrent.is_some() {
                    return Err(anyhow::anyhow!(
                        "Multiple allow concurrent bang lines found"
                    ));
                }
                allow_concurrent = Some(true);
            }
            BangLine::Handshake(_, byte_str) => {
                if handshake.is_some() {
                    return Err(anyhow::anyhow!("Multiple handshake bang lines found"));
                }
                str_to_data(byte_str);
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

    let bolt_version = bolt_version.ok_or(anyhow::anyhow!("Bolt version not specified"))?;
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

fn str_to_data(byte_str: &str) -> Result<Vec<u8>> {
    let clean = byte_str.replace(' ', "");
    if clean.len() % 2 != 0 {
        return Err(anyhow!("not right"));
    }
    let mut res = Vec::with_capacity(byte_str.len() / 2);
    for i in (0..clean.len()).step_by(2) {
        match u8::from_str_radix(&clean[i..i + 2], 16) {
            Ok(val) => {
                res.push(val);
                continue;
            }
            Err(e) => return Err(anyhow!(e)),
        }
    }
    Ok(res)
}

fn parse_block(block: &ScanBlock, config: &ActorConfig) -> Result<ActorBlock> {
    match block {
        ScanBlock::List(_, scan_blocks) => {
            let mut actor_blocks = Vec::with_capacity(scan_blocks.len());
            for scan_block in scan_blocks {
                let b = parse_block(scan_block, config)?;
                if matches!(b, ActorBlock::NoOp) {
                    continue;
                }
                actor_blocks.push(b);
            }
            validate_list_children(&actor_blocks)?;

            match actor_blocks.len() {
                0 => Ok(ActorBlock::NoOp),
                1 => Ok(actor_blocks.remove(0)),
                _ => Ok(ActorBlock::BlockList(actor_blocks)),
            }
        }
        ScanBlock::Alt(_, scan_blocks) => {
            let mut actor_blocks = Vec::with_capacity(scan_blocks.len());
            for block in scan_blocks {
                let b = parse_block(block, config)?;
                validate_alt_child(&b)?;
                actor_blocks.push(b);
            }
            Ok(ActorBlock::Alt(actor_blocks))
        }
        ScanBlock::Parallel(_, _) => {
            todo!("Parallel blocks are not yet supported in the actor")
        }
        ScanBlock::Optional(_, optional_scan_block) => {
            // TODO: Handle bad optional blocks
            let b = parse_block(optional_scan_block, config)?;
            validate_non_action(&b)?;
            Ok(ActorBlock::Optional(Box::new(b)))
        }
        ScanBlock::Repeat0(_, b) => Ok(ActorBlock::Repeat(Box::new(parse_block(b, config)?), 0)),
        ScanBlock::Repeat1(_, b) => Ok(ActorBlock::Repeat(Box::new(parse_block(b, config)?), 1)),
        ScanBlock::ClientMessage(_, message_name, body_string) => {
            let validator = create_validator(message_name, body_string, config)?;
            Ok(ActorBlock::ClientMessageValidate(validator))
        }
        ScanBlock::ServerMessage(_, message_name, body_string) => {
            let server_message_sender = create_message_sender(message_name, body_string, config)?;
            Ok(ActorBlock::ServerMessageSend(server_message_sender))
        }
        ScanBlock::AutoMessage(_, client_message_name, client_body_string) => {
            let validator = create_validator(client_message_name, client_body_string, config)?;
            let server_message_sender = create_auto_message_sender(client_message_name, config)?;
            Ok(ActorBlock::AutoMessage(AutoMessageHandler {
                client_validator: validator,
                server_sender: server_message_sender,
            }))
        }
        ScanBlock::Comment(_) => Ok(ActorBlock::NoOp),
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
    if matches!(b, ActorBlock::NoOp) {
        // TODO: Improve error handling
        return Err(anyhow::anyhow!("NoOp not allowed in Alt block"));
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::parser::str_to_data;

    #[test]
    pub fn test() {
        let bytes = "FF 00 22FF ffa1";
        let bytes = str_to_data(bytes).unwrap();
        assert_eq!(vec![255, 0, 34, 255, 255, 17], bytes);
    }
}
