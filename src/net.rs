use crate::net_actor::NetActor;
use crate::parser::ActorScript;
use anyhow::{anyhow, Error, Result};
use itertools::Itertools;
use std::ops::Add;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub struct Server<'a> {
    address: &'a str,
    server_script_cfg: &'static ActorScript,
    grace_period: Duration,
}

impl Server<'_> {
    pub fn new<'a>(
        address: &'a str,
        server_script_cfg: &'static ActorScript,
        grace_period: Duration,
    ) -> Server<'a> {
        Server {
            address,
            server_script_cfg,
            grace_period,
        }
    }

    pub fn start(&mut self) -> Result<()> {
        let rt = match self.server_script_cfg.config.allow_concurrent {
            true => tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build(),
            false => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build(),
        }?;

        rt.block_on(self.run())
    }

    async fn run(&mut self) -> Result<()> {
        let listener = TcpListener::bind(self.address).await?;
        let mut set: JoinSet<Result<()>> = JoinSet::new();

        let ct = CancellationToken::new();
        #[cfg(unix)]
        {
            let mut sigterm = signal(SignalKind::terminate())?;
            let mut sigint = signal(SignalKind::interrupt())?;
            select! {
                _ = sigterm.recv() => {
                    ct.cancel();
                    tokio::time::sleep(self.grace_period).await;
                    Ok::<(), Error>(())
                },
                _ = sigint.recv() => {
                    // aggressively interrupt
                    Ok::<(), Error>(())
                },
                _ = self.run_server(ct.child_token(), listener, &mut set) => {
                    // successfully completed script!
                    Ok::<(), Error>(())
                }
            }
        }?;

        let r = set.join_all().await;
        validate_results(&r)
    }

    async fn run_server(
        &self,
        ct: CancellationToken,
        listener: TcpListener,
        handles: &mut JoinSet<Result<()>>,
    ) -> Result<()> {
        let restarts = self.server_script_cfg.config.allow_restart;
        let concurrent = self.server_script_cfg.config.allow_concurrent;
        loop {
            let client_token = ct.clone();
            select! {
                res = listener.accept() => {
                    match res {
                        Ok((conn, addr)) => {
                            conn.set_nodelay(true)?;
                            let s = self.server_script_cfg;
                            let name = addr.to_string();
                            let mut actor = NetActor {
                                conn,
                                script: s,
                                name,
                                ct: client_token
                            };

                            handles.spawn(async move {
                                actor.run_client_connection().await
                            });

                            if !concurrent || !restarts {
                                return Ok(())
                            }
                        },
                        Err(inner) => {
                            return Err(Error::from(inner));
                        }
                    }
                },
                _ = ct.cancelled() => {
                    return Err(anyhow!("Shutdown while awaiting new connection"));
                }
            };
        }
    }
}

fn validate_results(results: &Vec<Result<()>>) -> Result<()> {
    let errors = results
        .iter()
        .filter(|e| !e.is_ok())
        .map(|res| match res {
            Ok(_) => panic!("failed to filter"),
            Err(err) => err,
        })
        .collect_vec();

    if errors.len() != 0 {
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
