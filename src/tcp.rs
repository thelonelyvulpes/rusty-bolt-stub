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

    pub async fn start(&mut self) -> anyhow::Result<()> {
        //Create tokio runtime
        Ok(())
    }
}
