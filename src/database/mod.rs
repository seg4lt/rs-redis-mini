use std::{
    collections::HashMap,
    sync::OnceLock,
    time::{Duration, Instant, SystemTime},
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
                    XRange {
                        resp,
                        stream_key,
                        start,
                        end,
                    } => {
                        last_command_was_set = false;
                        let value = db.get_stream_range(&stream_key, start, end);
                        let _ = resp.send(value);
                    }
                }
            }
        });
        db_event_listener
    }

    fn get_stream_range(
        &self,
        stream_key: &String,
        start: String,
        end: String,
    ) -> Vec<StreamDbValueType> {
        let (start_ms, start_sq) = if start == "-" {
            ("0", "0")
        } else {
            start.split_once("-").unwrap()
        };
        let (last_ms, last_sq) = if end == "+" {
            ("999999999999999", "999999999")
        } else {
            end.split_once("-").unwrap()
        };
        let start_ms = start_ms.parse::<u128>().unwrap_or(0);
        let end_ms = last_ms.parse::<u128>().unwrap_or(0);
        let start_sq = start_sq.parse::<usize>().unwrap_or(0);
        let end_sq = last_sq.parse::<usize>().unwrap_or(9999999);
        debug!(
            "start_ms {} start_sq {} end_ms {} end_sq {}",
            start_ms, start_sq, end_ms, end_sq
        );
        match self.db.get(stream_key) {
            None => {
                return vec![];
            }
            Some(value) => {
                let DbValueType::Stream(stream) = &value.value else {
                    return vec![];
                };
                let value = stream
                    .iter()
                    .filter(|value| {
                        debug!(
                            "value.stream_id_ms_part {} start_ms {} end_ms {}",
                            value.stream_id_ms_part, start_ms, end_ms
                        );
                        value.stream_id_ms_part >= start_ms && value.stream_id_ms_part <= end_ms
                    })
                    .filter(|value| {
                        debug!(
                            "value.stream_id_seq_part {} start_sq {} end_sq {}",
                            value.stream_id_seq_part, start_sq, end_sq
                        );
                        value.stream_id_seq_part >= start_sq && value.stream_id_seq_part <= end_sq
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                return value;
            }
        }
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
        let (ms_part, seq_part) = self.get_stream_id(stream_key, stream_id)?;
        match self.db.get_mut(stream_key) {
            None => {
                // Need to refactor so this duplicate code is used only once.
                self.db.insert(
                    stream_key.to_owned(),
                    DatabaseValue {
                        value: DbValueType::Stream(vec![StreamDbValueType {
                            stream_id_ms_part: ms_part,
                            stream_id_seq_part: seq_part,
                            key: key.to_owned(),
                            value: value.to_owned(),
                        }]),
                        exp_time: None,
                    },
                );
            }
            Some(db_value) => match db_value.value {
                DbValueType::Stream(ref mut stream) => {
                    stream.push(StreamDbValueType {
                        stream_id_ms_part: ms_part,
                        stream_id_seq_part: seq_part,
                        key: key.to_owned(),
                        value: value.to_owned(),
                    });
                }
                _ => {
                    self.db.insert(
                        stream_key.to_owned(),
                        DatabaseValue {
                            value: DbValueType::Stream(vec![StreamDbValueType {
                                stream_id_ms_part: ms_part,
                                stream_id_seq_part: seq_part,
                                key: key.to_owned(),
                                value: value.to_owned(),
                            }]),
                            exp_time: None,
                        },
                    );
                }
            },
        }

        Ok(format!("{ms_part}-{seq_part}"))
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
    fn get_stream_id(
        &mut self,
        stream_key: &String,
        stream_id: &String,
    ) -> Result<(u128, usize), String> {
        if stream_id == "*" {
            // if ms_part is * then we need to dynamically generate stream_id
            let ms_part = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            return Ok((ms_part, 0));
        }
        let Some((ms_part, seq_part)) = stream_id.split_once("-") else {
            debug!("ERR The ID specified in XADD must be greater than 0-0");
            return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
        };

        let ms_part = ms_part.parse::<u128>().unwrap();
        let default_seq_part = if ms_part == 0 { 1 } else { 0 };
        let db_stream = self
            .db
            .get(stream_key)
            .and_then(|stream_v| match &stream_v.value {
                DbValueType::Stream(stream) => {
                    let stream_id = stream
                        .iter()
                        .filter(|stream| stream.stream_id_ms_part == ms_part)
                        .map(|stream| (stream.stream_id_ms_part, stream.stream_id_seq_part))
                        .collect::<Vec<(u128, usize)>>();
                    let last = stream_id.last().copied();
                    last
                }
                _ => None,
            });
        let seq_part = match seq_part {
            "*" => match db_stream {
                None => default_seq_part,
                Some((_ms_part, last_seq_part)) => last_seq_part + 1,
            },
            _ => seq_part.parse::<usize>().unwrap(),
        };
        let last = self
            .db
            .get(stream_key)
            .and_then(|stream_v| match &stream_v.value {
                DbValueType::Stream(stream) => stream.last(),
                _ => None,
            });
        let (last_ms, last_seq) = last
            .map(|stream| (stream.stream_id_ms_part, stream.stream_id_seq_part))
            .unwrap_or((0, 0));

        if ms_part == 0 && seq_part <= 0 {
            debug!("ERR The ID specified in XADD must be greater than 0-0");
            return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
        }
        if ms_part < last_ms {
            return Err(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_string(),
            );
        }
        if ms_part == last_ms && seq_part <= last_seq {
            return Err(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_string(),
            );
        }
        return Ok((ms_part, seq_part));
    }
}
