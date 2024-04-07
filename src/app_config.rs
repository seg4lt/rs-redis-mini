use std::{collections::HashMap, sync::OnceLock};

use anyhow::anyhow;
use tracing::debug;

type AppConfigMap = HashMap<String, AppConfig>;

static APP_CONFIGS: OnceLock<AppConfigMap> = OnceLock::new();

#[derive(Debug, Clone)]
pub enum AppConfig {
    Port(u16),
    ReplicaOf(String, String),
}
impl AppConfig {
    pub(crate) fn get_port() -> u16 {
        let args = APP_CONFIGS.get_or_init(|| Self::init().unwrap());
        args.get("--port")
            .map(|v| match v {
                AppConfig::Port(port) => *port,
                _ => 6_379_u16,
            })
            .unwrap_or(6_379_u16)
    }
    pub(crate) fn is_master() -> bool {
        let args = APP_CONFIGS.get_or_init(|| Self::init().unwrap());
        args.get("--replicaof").is_none()
    }
    pub fn init() -> anyhow::Result<AppConfigMap> {
        let mut args = std::env::args();
        args.next();
        let mut map = HashMap::new();
        while let Some(arg) = args.next() {
            debug!("Processing arg: {}", arg);
            let cli_arg = match arg.as_str() {
                "--port" => match args.next() {
                    Some(port) => {
                        let port = port.parse::<u16>()?;
                        AppConfig::Port(port)
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
                    AppConfig::ReplicaOf(host, port)
                }
                _ => Err(anyhow!("Unknown argument"))?,
            };
            map.insert(arg, cli_arg);
        }
        debug!("AppConfigs ➡️  {map:?}");
        Ok(map)
    }
}
