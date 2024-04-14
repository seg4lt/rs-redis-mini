use std::{
    collections::HashMap,
    sync::OnceLock,
    time::{Duration, Instant},
};

use tokio::sync::{
    mpsc::{self, channel},
    oneshot,
};
use tracing::info;
use DatabaseEvent::*;

pub type DatabaseEventEmitter = mpsc::Sender<DatabaseEvent>;
// Probably shouldn't have this as static, but this makes program bit easier to write
static LISTENER: OnceLock<DatabaseEventEmitter> = OnceLock::new();

#[derive(Debug)]
pub enum DatabaseEvent {
    Set {
        key: String,
        value: String,
        flags: HashMap<String, String>,
    },
    Get {
        resp: oneshot::Sender<Option<String>>,
        key: String,
    },
    Keys {
        resp: oneshot::Sender<Vec<String>>,
        flag: String,
    },
    WasLastCommandSet {
        resp: oneshot::Sender<bool>,
    },
}
pub struct DatabaseValue {
    value: String,
    exp_time: Option<Instant>,
}
pub struct Database {
    db: HashMap<String, DatabaseValue>,
}

impl Database {
    pub fn new() -> DatabaseEventEmitter {
        let (db_event_listener, mut db_event_receiver) = channel::<DatabaseEvent>(100);
        LISTENER.get_or_init(|| db_event_listener.clone());
        tokio::spawn(async move {
            let mut db = Database { db: HashMap::new() };
            let mut last_command_was_set = false;
            while let Some(cmd) = db_event_receiver.recv().await {
                match cmd {
                    Set { key, value, flags } => {
                        db.set(&key, &value, Some(&flags));
                        // TODO: Better way to set this command
                        last_command_was_set = true;
                    }
                    Get { key, resp } => {
                        let value = db.get(&key);
                        resp.send(value)
                            .expect("Unable to send value back to caller");
                        last_command_was_set = false;
                    }
                    WasLastCommandSet { resp } => {
                        resp.send(last_command_was_set)
                            .expect("Unable to send WasLastCommandSet back to caller");
                        last_command_was_set = false;
                    }
                    Keys { resp, flag } => {
                        tracing::debug!("Getting keys with flag: {}", flag);
                        if flag != "*" {
                            resp.send(vec![])
                                .expect("Unable to send keys back to caller");
                            continue;
                        }
                        let keys = db.keys();
                        resp.send(keys).expect("Unable to send keys back to caller");
                        last_command_was_set = false;
                    }
                }
            }
        });
        db_event_listener
    }
    pub async fn emit(event: DatabaseEvent) -> anyhow::Result<()> {
        let Some(emitter) = LISTENER.get() else {
            panic!("DatabaseEventEmitter not initialized");
        };
        emitter.send(event).await?;
        Ok(())
    }
    fn keys(&self) -> Vec<String> {
        let value = self.db.keys().map(|k| k.to_owned()).collect();
        value
    }
    fn set(&mut self, key: &String, value: &String, flags: Option<&HashMap<String, String>>) {
        info!("Setting key: {} with value: {}", key, value);
        let exp_time = match flags {
            None => None,
            Some(flags) => flags
                .get("px")
                .map(|v| Instant::now() + Duration::from_millis(v.parse::<u64>().unwrap())),
        };
        let value = value.to_owned();
        let key = key.to_owned();
        self.db.insert(key, DatabaseValue { value, exp_time });
    }
    fn get(&mut self, key: &String) -> Option<String> {
        info!("Getting value for key: {}", key);
        let value = self.db.get(key);
        let value = match value {
            None => None,
            Some(kv) => match kv.exp_time {
                None => Some(kv.value.clone()),
                Some(exp_time) => {
                    if exp_time > Instant::now() {
                        Some(kv.value.clone())
                    } else {
                        self.db.remove(key);
                        None
                    }
                }
            },
        };
        value
    }
}
