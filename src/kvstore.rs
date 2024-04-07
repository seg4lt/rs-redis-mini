use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use tokio::sync::{
    mpsc::{self, channel},
    oneshot,
};
use tracing::{info, span, Level};
use KvStoreCmd::*;

pub type KvChan = mpsc::Sender<KvStoreCmd>;

#[derive(Debug)]
pub enum KvStoreCmd {
    Set {
        key: String,
        value: String,
        flags: HashMap<String, String>,
    },
    #[allow(dead_code)]
    SetWithGet {
        resp: oneshot::Sender<Option<String>>,
        key: String,
        value: String,
    },
    Get {
        resp: oneshot::Sender<Option<String>>,
        key: String,
    },
}

pub struct KvStore {
    value: String,
    exp_time: Option<Instant>,
}
type KvMap = HashMap<String, KvStore>;

pub async fn prepare_kvstore_channel() -> KvChan {
    let (tx, mut rx) = channel::<KvStoreCmd>(100);
    tokio::spawn(async move {
        let mut map: KvMap = HashMap::new();
        while let Some(cmd) = rx.recv().await {
            let span = span!(Level::DEBUG, "KvStoreChannel");
            let _guard = span.enter();
            match cmd {
                Set { key, value, flags } => set(&key, &value, Some(&flags), &mut map),
                SetWithGet { key, value, resp } => {
                    let prev_value = get(&key, &mut map);
                    set(&key, &value, None, &mut map);
                    resp.send(prev_value).unwrap();
                }
                Get { key, resp } => {
                    let value = get(&key, &mut map);
                    // Ignoring error for now
                    let _ = resp.send(value);
                }
            }
        }
    });
    tx
}

fn set(key: &String, value: &String, flags: Option<&HashMap<String, String>>, map: &mut KvMap) {
    info!("Setting key: {} with value: {}", key, value);
    let exp_time = match flags {
        None => None,
        Some(flags) => flags
            .get("px")
            .map(|v| Instant::now() + Duration::from_millis(v.parse::<u64>().unwrap())),
    };
    let value = value.to_owned();
    let key = key.to_owned();
    map.insert(key, KvStore { value, exp_time });
}

fn get(key: &String, map: &mut KvMap) -> Option<String> {
    info!("Getting value for key: {}", key);
    let value = map.get(key);
    let value = match value {
        None => None,
        Some(kv) => match kv.exp_time {
            None => Some(kv.value.clone()),
            Some(exp_time) => {
                if exp_time > Instant::now() {
                    Some(kv.value.clone())
                } else {
                    map.remove(key);
                    None
                }
            }
        },
    };
    value
}
