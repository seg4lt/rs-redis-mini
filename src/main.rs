#![warn(
    clippy::all,
    // clippy::restriction,
    // clippy::pedantic,
    // clippy::nursery,
    // clippy::cargo
)]

use anyhow::Context;
use tokio::net::TcpListener;
use tracing::{debug, info};

use crate::log::setup_log;

pub(crate) mod log;

pub const LINE_ENDING: &str = "\r\n";
pub const NEW_LINE: u8 = b'\n';

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_log()?;
    debug!("Logs from your program will appear here!");
    let port = 6_379_u16;
    let _listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .context(fdbg!("Unable to bind to port:{}", port));
    info!("Server started on 127.0.0.1:{}", port);
    Ok(())
    // #[allow(unused_labels)]
    // 'conn_accept_loop: loop {
    //     let (_socket, _) = listener.accept().await?;
    // }
}
