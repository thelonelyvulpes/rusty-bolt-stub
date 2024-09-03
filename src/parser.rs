use crate::types::{Block, Script};
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

    let tree = parse_body(&script.body)?;

    Ok(Actor {
        config,
        tree,
        script,
    })
}

fn parse_body(block: &Block) -> anyhow::Result<ActorBlock> {
    panic!("Not implemented")
}
