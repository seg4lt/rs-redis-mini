use std::{collections::HashMap, sync::OnceLock};

use anyhow::anyhow;
use tracing::debug;

type AppConfigMap = HashMap<String, AppConfig>;

static APP_CONFIGS: OnceLock<AppConfigMap> = OnceLock::new();

#[derive(Debug, Clone)]
pub enum AppConfig {
    Port(u16),
    ReplicaOf(String, String),
    MasterReplId(String),
    MasterReplOffset(u128),
    RDSDir(String),
    RDSFileName(String),
}
impl AppConfig {
    pub(crate) fn get_rds_dir() -> String {
        let args = APP_CONFIGS.get_or_init(|| Self::init().unwrap());
        args.get("--dir")
            .map(|v| match v {
                AppConfig::RDSDir(dir) => dir.clone(),
                _ => "NOTHING_DIR".to_string(),
            })
            .unwrap_or("NOTHING_DIR".to_string())
    }
    pub(crate) fn get_rds_file_name() -> String {
        let args = APP_CONFIGS.get_or_init(|| Self::init().unwrap());
        args.get("--dbfilename")
            .map(|v| match v {
                AppConfig::RDSFileName(file_name) => file_name.clone(),
                _ => "NOTHING_FILE_NAME".to_string(),
            })
            .unwrap_or("NOTHING_FILE_NAME".to_string())
    }
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
    pub(crate) fn get_replicaof() -> Option<(String, String)> {
        let args = APP_CONFIGS.get_or_init(|| Self::init().unwrap());
        let Some(AppConfig::ReplicaOf(host, port)) = args.get("--replicaof") else {
            return None;
        };
        Some((host.to_owned(), port.to_owned()))
    }
    pub(crate) fn get_master_replid() -> String {
        let args = APP_CONFIGS.get_or_init(|| Self::init().unwrap());
        args.get("$$master_replid")
            .map(|v| match v {
                AppConfig::MasterReplId(replid) => replid.clone(),
                _ => "".to_string(),
            })
            .unwrap_or("".to_string())
    }
    pub(crate) fn get_master_repl_offset() -> u128 {
        let args = APP_CONFIGS.get_or_init(|| Self::init().unwrap());
        args.get("$$master_repl_offset")
            .map(|v| match v {
                AppConfig::MasterReplOffset(offset) => *offset,
                _ => 0,
            })
            .unwrap_or(0)
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
                    let Some(replica_of) = args.next() else {
                        Err(anyhow!("replicaof host and port not provided"))?
                    };
                    let split = replica_of.split(" ").collect::<Vec<&str>>();
                    let host = split.get(0).expect("host not provided");
                    let port = split.get(1).expect("port not provided");
                    AppConfig::ReplicaOf(host.to_string(), port.to_string())
                }
                "--dir" => {
                    let dir_name = match args.next() {
                        Some(dir_name) => dir_name,
                        None => Err(anyhow!("dir_name is not provided"))?,
                    };
                    AppConfig::RDSDir(dir_name)
                }
                "--dbfilename" => {
                    let db_file_name = match args.next() {
                        Some(db_file_name) => db_file_name,
                        None => Err(anyhow!("dir_name is not provided"))?,
                    };
                    AppConfig::RDSFileName(db_file_name)
                }
                _ => Err(anyhow!("Unknown argument"))?,
            };
            map.insert(arg, cli_arg);
        }

        // Setup matser replication id and offset
        match map.get("--replicaof") {
            None => {
                let alpha_numeric = b"abcdefghijklmnopqrstuvwxyz0123456789";
                let hash = (0..40)
                    .map(|_| {
                        let idx = rand::random::<usize>() % alpha_numeric.len();
                        *alpha_numeric.get(idx).unwrap()
                    })
                    .collect::<Vec<u8>>();
                let hash = String::from_utf8(hash).unwrap();
                map.insert("$$master_replid".to_string(), AppConfig::MasterReplId(hash));
                map.insert(
                    "$$master_repl_offset".to_string(),
                    AppConfig::MasterReplOffset(0),
                );
            }
            _ => {}
        }
        debug!("AppConfigs ➡️  {map:?}");
        Ok(map)
    }
}
