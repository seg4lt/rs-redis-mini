use std::{
    collections::HashMap,
    sync::OnceLock,
    time::{Duration, Instant},
};

use self::db_event::DatabaseEvent::*;
use self::db_event::{DatabaseEvent, DatabaseValue, DbValueType, StreamDbValueType};
use tokio::sync::mpsc::{self, channel};
use tracing::{debug, info};

pub(crate) mod db_event;

pub type DatabaseEventEmitter = mpsc::Sender<DatabaseEvent>;

// Probably shouldn't have this as static, but this makes program bit easier to write
static LISTENER: OnceLock<DatabaseEventEmitter> = OnceLock::new();
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
                    Type { resp, key } => {
                        let value = db.get_type(&key);
                        resp.send(value.to_string())
                            .expect("Unable to send type back to caller");
                        last_command_was_set = false;
                    }
                    XAdd {
                        resp,
                        stream_key,
                        stream_id,
                        key,
                        value,
                    } => {
                        let r = db.set_stream(&stream_key, &stream_id, &key, &value);
                        resp.send(r)
                            .expect("Unable to send stream key back to caller");
                        last_command_was_set = true;
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

    fn set_stream(
        &mut self,
        stream_key: &String,
        stream_id: &String,
        key: &String,
        value: &String,
    ) -> Result<String, String> {
        info!("Setting stream: {} with value: {}", stream_key, value);
        let Some((ms_part, seq_part)) = stream_id.split_once("-") else {
            return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
        };
        let ms_part = ms_part.parse::<u64>().unwrap();
        let seq_part = seq_part.parse::<u64>().unwrap();
        debug!("Seq part --> {seq_part}");
        if seq_part <= 0 {
            return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
        }
        let get_stream = self.db.get(stream_key);
        match get_stream {
            None => {}
            Some(db_value) => {
                let DbValueType::Stream(stream) = &db_value.value else {
                    return Err("NOT A STREAM".to_string());
                };
                let last_stream = stream.last().unwrap();
                let Some((last_ms_part, last_seq_part)) = last_stream.stream_id.split_once("-")
                else {
                    return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
                };
                let last_ms_part = last_ms_part.parse::<u64>().unwrap();
                let last_seq_part = last_seq_part.parse::<u64>().unwrap();

                debug!("ms_part {ms_part} -- last_ms_part {last_ms_part}");
                if ms_part < last_ms_part {
                    return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string());
                }
                debug!("seq_part {seq_part} -- last_seq_part {last_seq_part}");
                if ms_part == last_ms_part && seq_part <= last_seq_part {
                    return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string());
                }
            }
        };

        self.db.insert(
            stream_key.to_owned(),
            DatabaseValue {
                value: DbValueType::Stream(vec![StreamDbValueType {
                    stream_id: stream_id.to_owned(),
                    key: key.to_owned(),
                    value: value.to_owned(),
                }]),
                exp_time: None,
            },
        );
        Ok(stream_id.to_owned())
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
        self.db.insert(
            key,
            DatabaseValue {
                value: DbValueType::String(value),
                exp_time,
            },
        );
    }

    fn get_type(&mut self, key: &String) -> &str {
        let value = self.db.get(key);
        match value {
            None => "none",
            Some(kv) => match kv.value {
                DbValueType::String(_) => "string",
                DbValueType::Stream(_) => "stream",
            },
        }
    }

    fn get(&mut self, key: &String) -> Option<String> {
        info!("Getting value for key: {}", key);
        let value = self.db.get(key);
        let value = match value {
            None => None,
            Some(kv) => match kv.exp_time {
                None => {
                    let value = match &kv.value {
                        DbValueType::String(v) => v.clone(),
                        _ => unimplemented!("Only string value can invoke GET for now"),
                    };
                    Some(value.clone())
                }
                Some(exp_time) => {
                    if exp_time > Instant::now() {
                        let value = match &kv.value {
                            DbValueType::String(v) => v.clone(),
                            _ => unimplemented!("Only string value can invoke GET for now"),
                        };
                        Some(value.clone())
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
