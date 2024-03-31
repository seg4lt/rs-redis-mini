use std::collections::HashMap;

use anyhow::anyhow;
use tracing::debug;

pub enum CliArgs {
    Port(String),
    ReplicaOf(String, String),
}
impl CliArgs {
    pub fn get() -> anyhow::Result<HashMap<String, CliArgs>> {
        let mut args = std::env::args();
        args.next();
        let mut map = HashMap::new();
        while let Some(arg) = args.next() {
            debug!("Processing arg: {}", arg);
            let cli_arg = match arg.as_str() {
                "--port" => match args.next() {
                    Some(port) => CliArgs::Port(port),
                    None => Err(anyhow!("Port number not provided"))?,
                },
                "--replicaof" => {
                    let host = match args.next() {
                        Some(host) => host,
                        None => Err(anyhow!("replicaof host not provided"))?,
                    };
                    let port = match args.next() {
                        Some(port) => port,
                        None => Err(anyhow!("replicaof port number not provided"))?,
                    };
                    CliArgs::ReplicaOf(host, port)
                }
                _ => Err(anyhow!("Unknown argument"))?,
            };
            map.insert(arg, cli_arg);
        }
        Ok(map)
    }
}
