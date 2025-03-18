// TODO: remove when rapid PoC phase is over
#![allow(dead_code)]

mod bang_line;
mod bolt_version;
mod context;
mod jolt;
mod logging;
mod net;
mod net_actor;
mod parse_error;
mod parser;
mod scanner;
mod serde_json_ext;
mod str_byte;
mod types;
mod util;
mod values;

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use log::{debug, LevelFilter};

use crate::logging::init_logging;
use crate::parser::ActorScript;

const TIMEOUT_HELP: &str = "The number of seconds for which the stub server will run \
before automatically terminating. If unspecified, the server will wait for 30 seconds.";
const LISTEN_ADDR_HELP: &str = "The base address on which to listen for incoming \
connections in INTERFACE:PORT format, where INTERFACE may be omitted for 'localhost'. Each script \
(which doesn't specify an explicit port number) will use subsequent ports. If completely omitted, \
this defaults to ':17687'.";
const VERBOSE_HELP: &str = "Show more detail about the client-server exchange. \
Supply the flag up to 3 times to increase verbosity with each.";
const GRACE_PERIOD_HELP: &str =
    "Grace period in seconds to stop the server after receiving SIGTERM or equivalent.";

#[derive(Parser)]
struct StubArgs {
    #[arg(short, long, default_value_t=30.0, help=TIMEOUT_HELP)]
    timeout: f32,
    #[arg(long="listen-addr", short, default_value=":17687", help=LISTEN_ADDR_HELP)]
    listen_addr: String,
    #[arg(short, long, action=clap::ArgAction::Count, help=VERBOSE_HELP)]
    verbose: u8,
    script: String,
    #[arg(short, long, default_value_t=5.0, help=GRACE_PERIOD_HELP)]
    grace_period: f32,
}

static SCRIPT: OnceLock<String> = OnceLock::new();
static PARSED: OnceLock<ActorScript> = OnceLock::new();

fn main() -> anyhow::Result<()> {
    let args = StubArgs::parse();
    let log_level = match args.verbose {
        0 => LevelFilter::Off,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };
    init_logging(log_level);
    if args.verbose > 3 {
        log::warn!("Verbose level capped at 3");
    }
    let script_text = std::fs::read_to_string(&args.script)
        .with_context(|| format!("Failed to read script file: {}", &args.script))?;
    debug!(
        "Read script file: {}\n\
        ================================================================\n\
        {script_text}\n\
        ================================================================",
        &args.script
    );
    SCRIPT.get_or_init(move || script_text);

    let output = scanner::scan_script(SCRIPT.get().unwrap(), args.script)?;
    let engine = parser::contextualize_res(parser::parse(output), SCRIPT.get().unwrap())?;

    PARSED.get_or_init(move || engine);

    let shutdown_grace_period = Duration::from_secs_f32(args.grace_period);
    net::Server::new(
        &args.listen_addr,
        PARSED.get().unwrap(),
        shutdown_grace_period,
    )
    .start()
}
