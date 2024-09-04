use crate::parser::Actor;

pub struct Server<'a> {
    address: &'a str,
    server_script_cfg: &'a Actor,
}

impl Server<'_> {
    pub fn new<'a>(address: &'a str, server_script_cfg: &'a Actor) -> Server<'a> {
        Server {
            address,
            server_script_cfg,
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        //Create tokio runtime
        Ok(())
    }
}
