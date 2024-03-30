use crate::{
    cli_args::CliArgs,
    store::{Store, KEY_IS_MASTER, KEY_MASTER_REPLID, KEY_MASTER_REPL_OFFSET},
};
use std::{
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{anyhow, Context};
pub(crate) mod cli_args;
pub(crate) mod command;
pub(crate) mod hash;
pub(crate) mod master_things;
pub(crate) mod replica_things;
pub(crate) mod resp_parser;
pub(crate) mod server_things;
pub(crate) mod store;

pub const LINE_ENDING: &str = "\r\n";
pub const NEW_LINE: u8 = b'\n';

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Logs from your program will appear here!");

    // TODO: To many mutex / locks - will app bottleneck because thread can't get a lock?
    let shared_map: Arc<RwLock<Store>> = Arc::new(RwLock::new(Store::new()));
    let cmd_args = Arc::new(CliArgs::get()?);
    let replicas: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
    let default_port = "6379".to_string();
    let port = match cmd_args.get("--port") {
        Some(CliArgs::Port(port)) => port,
        _ => &default_port,
    };
    match cmd_args.get("--replicaof") {
        None => {
            let hash = hash::generate_random_string();
            let mut map = shared_map.write().unwrap();
            map.set(KEY_IS_MASTER.into(), "true".into(), None);
            map.set(KEY_MASTER_REPLID.into(), hash, None);
            map.set(KEY_MASTER_REPL_OFFSET.into(), "0".into(), None);
        }
        Some(CliArgs::ReplicaOf(ip, master_port)) => {
            let (port, ip, master_port) = (port.clone(), ip.clone(), master_port.clone());
            std::thread::spawn(move || -> anyhow::Result<()> {
                replica_things::sync_with_master(port, ip, master_port)
                    .context("Unable to sync with master")?;
                Ok(())
            });
        }
        _ => Err(anyhow!("Invalid --replicaof argument"))?,
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
    for stream in listener.incoming() {
        let (map, args, replicas) = (shared_map.clone(), cmd_args.clone(), replicas.clone());
        // TODO: Implement event loop like redis??
        std::thread::spawn(move || {
            let stream = stream.unwrap();
            server_things::parse_tcp_stream(stream, map, args, replicas)
                .context("Unable to parse tcp stream")
                .unwrap();
        });
    }
    Ok(())
}
