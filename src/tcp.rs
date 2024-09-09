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

    pub fn start(&mut self) -> anyhow::Result<()> {
        let mut rt = match self.server_script_cfg.config.allow_concurrent {
            true => tokio::runtime::Builder::new_multi_thread().enable_all().build(),
            false => tokio::runtime::Builder::new_current_thread().enable_all().build(),
        }?;

        rt.block_on(async {
            //Create tokio runtime
            Ok(())
        })
    }
}
