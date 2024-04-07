use std::collections::HashMap;

use anyhow::anyhow;
use tracing::debug;

static CLI_ARGS: std::sync::OnceLock<HashMap<String, CliArgs>> =
    std::sync::OnceLock::<HashMap<String, CliArgs>>::new();

#[derive(Debug, Clone)]
pub enum CliArgs {
    Port(u16),
    ReplicaOf(String, String),
}
impl CliArgs {
    pub(crate) fn get_port() -> u16 {
        let args = CLI_ARGS.get_or_init(|| Self::init().unwrap());
        args.get("--port")
            .map(|v| match v {
                CliArgs::Port(port) => *port,
                _ => 6_379_u16,
            })
            .unwrap_or(6_379_u16)
    }
    pub fn init() -> anyhow::Result<HashMap<String, CliArgs>> {
        let mut args = std::env::args();
        args.next();
        let mut map = HashMap::new();
        while let Some(arg) = args.next() {
            debug!("Processing arg: {}", arg);
            let cli_arg = match arg.as_str() {
                "--port" => match args.next() {
                    Some(port) => {
                        let port = port.parse::<u16>()?;
                        CliArgs::Port(port)
                    }
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
        debug!("CliArgs ➡️  {map:?}");
        Ok(map)
    }
}
