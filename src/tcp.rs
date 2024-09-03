use crate::parser::Engine;
use std::net::TcpListener;

pub struct Server<'a> {
    address: &'a str,
    server_script_cfg: &'a Engine,
}

impl Server<'_> {
    pub fn new<'a>(address: &'a str, server_script_cfg: &'a Engine) -> Server<'a> {
        Server {
            address,
            server_script_cfg,
        }
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.address)?;
        if self.server_script_cfg.allow_concurrent {
            // start the server so that it concurrently accepts connections
        } else if self.server_script_cfg.allow_restart {
            // start the server in a way that lets it run 1 connection at a time
        } else {
            // start the server in a 1 pass
        }
        Ok(())
    }
}
