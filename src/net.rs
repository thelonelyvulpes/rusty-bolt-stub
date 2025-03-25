use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::ops::Add;
use std::sync::{atomic, Arc};

use anyhow::{anyhow, Error, Result};
use itertools::Itertools;
use log::{debug, info};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::net_actor::NetActor;
use crate::parser::ActorScript;

pub struct Server {
    address: String,
    server_script_cfg: &'static ActorScript<'static>,
    ever_acted: bool,
    shutting_down: Arc<atomic::AtomicBool>,
}

impl Server {
    pub fn new(address: &str, server_script_cfg: &'static ActorScript<'static>) -> Self {
        let address = if address.starts_with(":") {
            format!("localhost{}", address)
        } else {
            address.to_string()
        };
        debug!("Creating server listening on {}", address);
        Server {
            address,
            server_script_cfg,
            ever_acted: Default::default(),
            shutting_down: Default::default(),
        }
    }

    pub fn start(&mut self) -> Result<()> {
        debug!("Starting server");
        let rt = match self.server_script_cfg.config.allow_concurrent {
            true => tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build(),
            false => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build(),
        }?;

        rt.block_on(self.run())
    }

    async fn run(&mut self) -> Result<()> {
        let listener = TcpListener::bind(&self.address).await?;
        println!("Listening");
        let mut set: JoinSet<Result<()>> = Default::default();

        let ct = CancellationToken::new();

        let signal_handler = async {
            {
                let ct = ct.clone();
                let mut i = 0;
                loop {
                    Self::exit_signal()?.await;
                    self.shutting_down.store(true, atomic::Ordering::Release);
                    match i {
                        0 => {
                            info!("1st interrupt signal received. Cancelling all actors.");
                            ct.cancel();
                        }
                        _ => {
                            info!("2nd interrupt signal received. Hard exit.");
                            panic!("Hard exit after cancellation didn't succeed.");
                        }
                    }
                    i += 1;
                }
                #[allow(unreachable_code)] // needed to disambiguate the return type
                Ok::<(), Error>(())
            }
        };
        {
            select! {
                _ = signal_handler => {
                    debug!("signal_handler exit");
                    Ok::<(), Error>(())
                },
                _ = self.run_server(ct.child_token(), listener, &mut set) => {
                    // successfully completed script!
                    debug!("self.run_server exit");
                    Ok::<(), Error>(())
                }
            }
        }?;

        self.ever_acted = !set.is_empty();
        let r = set.join_all().await;
        validate_results(&r)
    }

    pub(crate) fn ever_acted(&self) -> bool {
        self.ever_acted
    }

    fn exit_signal() -> Result<impl Future<Output = Option<()>>> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let mut signal = signal(SignalKind::interrupt())?;
            Ok(async move { signal.recv().await })
        }
        #[cfg(windows)]
        {
            use tokio::signal::windows::ctrl_break;

            let mut signal = ctrl_break()?;
            Ok(async move { signal.recv().await })
        }
        #[cfg(not(any(unix, windows)))]
        {
            compile_error!("Only Unix and Windows signals are supported for now");
        }
    }

    async fn run_server(
        &self,
        ct: CancellationToken,
        listener: TcpListener,
        handles: &mut JoinSet<Result<()>>,
    ) -> Result<()> {
        loop {
            let ct_connection = ct.clone();
            select! {
                conn = listener.accept() => {
                    self.handle_connection(conn, ct_connection,handles).await?;
                },
                _ = ct.cancelled() => {
                    return Err(anyhow!("Shutdown while awaiting new connection"));
                }
            }
        }
    }

    async fn handle_connection(
        &self,
        conn: io::Result<(TcpStream, SocketAddr)>,
        ct: CancellationToken,
        handles: &mut JoinSet<Result<()>>,
    ) -> Result<()> {
        let restarts = self.server_script_cfg.config.allow_restart;
        let concurrent = self.server_script_cfg.config.allow_concurrent;

        match conn {
            Ok((conn, addr)) => {
                debug!("Server accepted connection from {}", addr);
                conn.set_nodelay(true)?;
                let script = self.server_script_cfg;
                let shutting_down = Arc::clone(&self.shutting_down);
                let mut actor = NetActor::new(ct.child_token(), shutting_down, conn, script);
                handles.spawn(async move {
                    let res = actor.run_client_connection().await;
                    if res.is_err() {
                        debug!("Error on connection {addr}, cancelling other workers: {res:?}");
                        ct.cancel();
                    }
                    res
                });

                if !(concurrent || restarts) {
                    debug!("Server stops listening after single connection.");
                    return Ok(());
                }
                debug!("Server listening for more connections.");
                Ok(())
            }
            Err(inner) => {
                debug!("Server stops listening after error: {inner}");
                Err(Error::from(inner))
            }
        }
    }
}

fn validate_results(results: &[Result<()>]) -> Result<()> {
    let errors = results
        .iter()
        .filter(|e| !e.is_ok())
        .map(|res| match res {
            Ok(_) => panic!("failed to filter"),
            Err(err) => err,
        })
        .collect_vec();

    if !errors.is_empty() {
        let mut sb = String::with_capacity(1024);
        sb = sb.add(
            format!(
                "{} errors occurred while shutting down.\n---\n",
                errors.len()
            )
            .as_str(),
        );
        for error in errors {
            sb = sb.add(error.chain().join("\n").as_str());
            sb.push('\n')
        }
        // report all errors
        return Err(anyhow!(sb));
    }
    Ok(())
}
