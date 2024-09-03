// TODO: remove when rapid PoC phase is over
#![allow(dead_code)]

mod parser;
mod scanner;
mod tcp;
mod types;

use anyhow::Context;
use clap::Parser;
use std::io::Read;
use std::sync::OnceLock;

const LISTEN_ADDR_HELP: &'static str = "The base address on which to listen for incoming \
connections in INTERFACE:PORT format, where INTERFACE may be omitted for 'localhost'. Each script \
(which doesn't specify an explicit port number) will use subsequent ports. If completely omitted, \
this defaults to ':17687'.";
const TIMEOUT_HELP: &'static str = "The number of seconds for which the stub server will run \
before automatically terminating. If unspecified, the server will wait for 30 seconds.";

#[derive(Parser)]
struct StubArgs {
    #[arg(short, long, default_value_t=30.0, help=TIMEOUT_HELP)]
    timeout: f32,
    #[arg(long="listen-addr", short, default_value=":17687", help=LISTEN_ADDR_HELP)]
    listen_addr: String,
    #[arg(
        short,
        long,
        help = "Show more detail about the client-server exchange."
    )]
    verbose: bool,
    script: String,
}

static SCRIPT: OnceLock<String> = OnceLock::new();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = StubArgs::parse();
    let script_text = std::fs::read_to_string(&args.script)
        .with_context(|| format!("Failed to read script file: {}", &args.script))?;
    SCRIPT.get_or_init(move || script_text);

    let output = scanner::scan_script(SCRIPT.get().unwrap(), args.script.into())?;

    let engine = parser::parse(output)?;

    let mut server = tcp::Server::new(&args.listen_addr, &engine);

    server.start().await
}
