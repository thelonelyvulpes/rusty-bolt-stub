use crate::parser::ActorScript;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

pub struct NetActor<T> {
    pub ct: CancellationToken,
    pub conn: T,
    pub name: String,
    pub script: &'static ActorScript,
}

impl<'a, T: AsyncRead + AsyncWrite> NetActor<T> {
    pub async fn run_client_connection(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}
