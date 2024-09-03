use crate::types::Script;

pub struct Engine {
    pub allow_restart: bool,
    pub allow_concurrent: bool,
    pub script: Script,
}

pub fn parse(script: Script) -> anyhow::Result<Engine> {
    Ok(Engine {
        allow_restart: false,
        allow_concurrent: false,
        script,
    })
}
