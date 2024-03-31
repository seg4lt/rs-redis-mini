use crate::{
    cli_args::CliArgs,
    store::{Store, KEY_IS_MASTER, KEY_MASTER_REPLID, KEY_MASTER_REPL_OFFSET},
};
use std::{
    collections::HashMap,
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{anyhow, Context};
use tracing::{debug, info, Level};
pub(crate) mod cli_args;
pub(crate) mod cmd_processor;
pub(crate) mod command;
pub(crate) mod dev;
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
    setup_log()?;
    debug!("Logs from your program will appear here!");

    // TODO: To many mutex / locks - will app bottleneck because thread can't get a lock?
    let shared_map: Arc<RwLock<Store>> = Arc::new(RwLock::new(Store::new()));
    let cmd_args = Arc::new(CliArgs::get()?);
    let replicas: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
    let port = get_port(&cmd_args);
    setup_server(&cmd_args, &port, &shared_map)?;

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
    info!("Server started on 127.0.0.1:{}", port);

    for stream in listener.incoming() {
        let (map, args, replicas) = (shared_map.clone(), cmd_args.clone(), replicas.clone());
        // TODO: Implement event loop like redis??
        std::thread::spawn(move || {
            let stream = stream.unwrap();
            server_things::parse_tcp_stream(stream, map, args, replicas)
                .context(fdbg!("Server crashed for some reason !!"))
                .expect("Server closed");
        });
    }
    Ok(())
}

fn get_port(cmd_args: &Arc<HashMap<String, CliArgs>>) -> String {
    let default_port = "6379".to_string();
    let port = match cmd_args.get("--port") {
        Some(CliArgs::Port(port)) => port,
        _ => &default_port,
    };
    port.to_owned()
}

fn setup_server(
    cmd_args: &Arc<HashMap<String, CliArgs>>,
    cur_server_port: &String,
    map: &Arc<RwLock<Store>>,
) -> anyhow::Result<()> {
    match cmd_args.get("--replicaof") {
        None => {
            let hash = hash::generate_random_string();
            let mut map = map.write().unwrap();
            map.set(KEY_IS_MASTER.into(), "true".into(), None);
            map.set(KEY_MASTER_REPLID.into(), hash, None);
            map.set(KEY_MASTER_REPL_OFFSET.into(), "0".into(), None);
        }
        Some(CliArgs::ReplicaOf(ip, master_port)) => {
            let (port, ip, master_port) =
                (cur_server_port.clone(), ip.clone(), master_port.clone());
            let (map, args) = (map.clone(), cmd_args.clone());
            std::thread::spawn(move || {
                replica_things::sync_with_master(port, ip, master_port, map, args)
                    .context(fdbg!("Unable to sync with master"))
                    .expect("Error on replica connection to the master");
            });
        }
        _ => Err(anyhow!("Invalid --replicaof argument"))?,
    };
    Ok(())
}

fn setup_log() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        // Use a more compact, abbreviated log format
        .compact()
        .without_time()
        // Display source code file paths
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        // Display the thread ID an event was recorded on
        .with_thread_ids(true)
        // Don't display the event's target (module path)
        .with_target(false)
        // Build the subscriber
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}
