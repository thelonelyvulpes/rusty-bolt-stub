use anyhow::anyhow;
use crate::types::{ScanBlock, Script};
use crate::types::actor_types::ActorBlock;

pub struct Actor {
    pub config: ActorConfig,
    pub tree: ActorBlock,
    pub script: Script,
}

pub struct ActorConfig {
}

pub fn parse(script: Script) -> anyhow::Result<Actor> {
    let config = ActorConfig {

    };

    let tree = parse_block(&script.body, &config)?;

    Ok(Actor {
        config,
        tree,
        script,
    })
}

fn parse_block(block: &ScanBlock, config: &ActorConfig) -> anyhow::Result<ActorBlock> {
    match block {
        ScanBlock::List(block_nodes) => {
            let mut actor_nodes = Vec::with_capacity(block_nodes.len());
            for node in block_nodes {
                let b = parse_block(node, config)?;
                if matches!(b, ActorBlock::NoOp) {
                    continue;
                }
                actor_nodes.push(b);
            }
            match actor_nodes.len() {
                0 => Ok(ActorBlock::NoOp),
                1 => Ok(actor_nodes.remove(0)),
                _ => Ok(ActorBlock::BlockList(actor_nodes)),
            }
        }
        ScanBlock::Alt(alt_blocks) => {
            let mut actor_blocks = Vec::with_capacity(alt_blocks.len());
            for block in alt_blocks {
                let b = parse_block(block, config)?;
                if matches!(b, ActorBlock::NoOp) {
                    // TODO: Improve error handling
                    return Err(anyhow!("NoOp not allowed in Alt block"));
                }
                actor_blocks.push(b);
            }
            Ok(ActorBlock::Alt(actor_blocks))
        }
        ScanBlock::Parallel(_) => {
            // TODO: implement parallel
            Ok(ActorBlock::NoOp)
        }
        ScanBlock::Opt(b) => {
            // TODO: Handle bad optional blocks
            Ok(ActorBlock::Optional(Box::new(parse_block(b, config)?)))
        },
        ScanBlock::Repeat0(b) => Ok(ActorBlock::Repeat(Box::new(parse_block(b, config)?), 0)),
        ScanBlock::Repeat1(b) => Ok(ActorBlock::Repeat(Box::new(parse_block(b, config)?), 1)),
        ScanBlock::ClientMessage(_, _) => {
            panic!("ClientMessage not implemented");
        },
        ScanBlock::ServerMessage(_, _) => {
            panic!("ServerMessage not implemented");
        },
        ScanBlock::AutoMessage(_, _) => Ok(ActorBlock::NoOp),
        ScanBlock::Comment => Ok(ActorBlock::NoOp),
        ScanBlock::Python(_) => Ok(ActorBlock::NoOp),
        ScanBlock::Condition(_) => Ok(ActorBlock::NoOp),
    }
}
