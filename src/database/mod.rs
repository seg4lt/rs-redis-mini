use crate::fdbg;
use std::num::ParseIntError;
use std::{
    collections::HashMap,
    sync::OnceLock,
    time::{Duration, Instant, SystemTime},
};

use self::db_event::DatabaseEvent::*;
use self::db_event::{DatabaseEvent, DatabaseValue, DbValueType, StreamDbValueType};
use anyhow::Context;
use db_event::DbError;
use tokio::sync::{
    mpsc::{self, channel},
    oneshot,
};
use tracing::{debug, info};

pub(crate) mod db_event;

pub type DatabaseEventEmitter = mpsc::Sender<DatabaseEvent>;

// Probably shouldn't have this as static, but this makes program a bit easier to write
// TODO: Find better way? How to not pass this everywhere?
static LISTENER: OnceLock<DatabaseEventEmitter> = OnceLock::new();

pub struct Database {
    db: HashMap<String, DatabaseValue>,
}

impl Database {
    pub fn new() -> DatabaseEventEmitter {
        let (db_event_listener, db_event_receiver) = channel::<DatabaseEvent>(100);
        LISTENER.get_or_init(|| db_event_listener.clone());
        tokio::spawn(async move {
            Database::_setup_db_event_listener(db_event_receiver).await;
        });
        db_event_listener
    }

    pub async fn set(
        key: &String,
        value: &String,
        flags: &HashMap<String, String>,
    ) -> anyhow::Result<()> {
        let set_event = Set {
            key: key.clone(),
            value: value.clone(),
            flags: flags.clone(),
        };
        Database::emit(set_event).await
    }

    pub async fn get(key: &String) -> anyhow::Result<Option<DbValueType>> {
        let (resp_emitter, listener) = oneshot::channel::<Option<DbValueType>>();
        let kv_cmd = Get {
            emitter: resp_emitter,
            key: key.to_owned(),
        };
        Database::emit(kv_cmd).await?;
        Ok(listener.await?)
    }

    pub async fn incr(key: &String) -> anyhow::Result<DbValueType> {
        let (resp_emitter, listener) = oneshot::channel::<Result<DbValueType, String>>();
        let kv_cmd = Incr {
            emitter: resp_emitter,
            key: key.to_owned(),
        };
        Database::emit(kv_cmd).await?;
        listener.await?.map_err(|s| anyhow::anyhow!(s))
    }

    pub async fn keys(flag: &String) -> anyhow::Result<Vec<String>> {
        let (emitter, listener) = oneshot::channel::<Vec<String>>();
        let keys_event = Keys {
            emitter,
            flag: flag.clone(),
        };
        Database::emit(keys_event).await?;
        Ok(listener.await?)
    }

    pub async fn get_type(key: &String) -> anyhow::Result<String> {
        let (emitter, listener) = oneshot::channel::<String>();
        let type_event = DatabaseEvent::Type {
            emitter,
            key: key.clone(),
        };
        Database::emit(type_event).await?;
        Ok(listener.await?)
    }

    pub async fn get_last_stream_id(stream_key: &String) -> anyhow::Result<String> {
        let (emitter, listener) = oneshot::channel::<String>();
        let event = DatabaseEvent::_GetLastStreamId {
            emitter,
            stream_key: stream_key.clone(),
        };
        Database::emit(event).await?;
        Ok(listener.await?)
    }

    pub async fn xadd(
        stream_key: &String,
        stream_id: &String,
        key: &String,
        value: &String,
    ) -> anyhow::Result<String, String> {
        let (emitter, listener) = oneshot::channel::<Result<String, String>>();
        Database::emit(DatabaseEvent::XAdd {
            emitter,
            stream_key: stream_key.clone(),
            stream_id: stream_id.clone(),
            key: key.clone(),
            value: value.clone(),
        })
        .await
        .map_err(|e| e.to_string())?;
        listener.await.map_err(|e| e.to_string())?
    }

    pub async fn xread(
        filters: &Vec<(String, String)>,
    ) -> anyhow::Result<Vec<(String, Vec<StreamDbValueType>)>> {
        let (tx, rx) = oneshot::channel::<Vec<(String, Vec<StreamDbValueType>)>>();
        Database::emit(DatabaseEvent::XRead {
            emitter: tx,
            filters: filters.clone(),
        })
        .await
        .context(fdbg!("Something went wrong when sending read event"))?;
        let db_value = rx.await.context(fdbg!("Unable to receive"))?;
        Ok(db_value)
    }

    pub async fn xrange(
        stream_key: &String,
        start: &String,
        end: &String,
    ) -> anyhow::Result<Vec<StreamDbValueType>> {
        let (emitter, listener) = oneshot::channel::<Vec<StreamDbValueType>>();
        Database::emit(DatabaseEvent::XRange {
            emitter,
            stream_key: stream_key.clone(),
            start: start.clone(),
            end: end.clone(),
        })
        .await?;
        Ok(listener.await?)
    }

    pub async fn was_last_command_set() -> anyhow::Result<bool> {
        let (emitter, listener) = oneshot::channel::<bool>();
        Database::emit(DatabaseEvent::WasLastCommandSet { emitter }).await?;
        Ok(listener.await?)
    }

    pub async fn emit(event: DatabaseEvent) -> anyhow::Result<()> {
        let Some(emitter) = LISTENER.get() else {
            panic!("DatabaseEventEmitter not initialized");
        };
        emitter.send(event).await?;
        Ok(())
    }

    async fn _setup_db_event_listener(mut receiver: mpsc::Receiver<DatabaseEvent>) {
        let mut db = Database { db: HashMap::new() };
        let mut last_command_was_set = false;
        while let Some(cmd) = receiver.recv().await {
            match cmd {
                Set { key, value, flags } => {
                    let value = match value.parse::<i64>() {
                        Ok(i) => DbValueType::Integer(i),
                        Err(_) => DbValueType::String(value),
                    };
                    db._set(&key, value, Some(&flags));
                    // TODO: Better way to set this command
                    last_command_was_set = true;
                }
                Get { key, emitter } => {
                    let value = db._get(&key);
                    emitter
                        .send(value)
                        .expect("Unable to send value back to caller");
                    last_command_was_set = false;
                }
                Incr { key, emitter } => {
                    match db._incr(&key) {
                        Ok(v) => {
                            emitter
                                .send(Ok(v))
                                .expect("Unable to send value back to caller");
                            last_command_was_set = false;
                        }
                        _ => emitter
                            .send(Err(
                                "ERR value is not an integer or out of range".to_string()
                            ))
                            .unwrap(),
                    };
                }
                WasLastCommandSet { emitter } => {
                    emitter
                        .send(last_command_was_set)
                        .expect("Unable to send WasLastCommandSet back to caller");
                    last_command_was_set = false;
                }
                Keys { emitter, flag } => {
                    tracing::debug!("Getting keys with flag: {}", flag);
                    if flag != "*" {
                        emitter
                            .send(vec![])
                            .expect("Unable to send keys back to caller");
                        continue;
                    }
                    let keys = db._keys();
                    emitter
                        .send(keys)
                        .expect("Unable to send keys back to caller");
                    last_command_was_set = false;
                }
                Type { emitter, key } => {
                    let value = db._get_type(&key);
                    emitter
                        .send(value.to_string())
                        .expect("Unable to send type back to caller");
                    last_command_was_set = false;
                }
                XAdd {
                    emitter,
                    stream_key,
                    stream_id,
                    key,
                    value,
                } => {
                    let r = db._set_stream(&stream_key, &stream_id, &key, &value);
                    emitter
                        .send(r)
                        .expect("Unable to send stream key back to caller");
                    debug!(?stream_key, ?stream_id, ?key, ?value, "XAdd -- ");
                    last_command_was_set = true;
                }
                XRange {
                    emitter,
                    stream_key,
                    start,
                    end,
                } => {
                    last_command_was_set = false;
                    let value = db._get_stream_range(&stream_key, start, end);
                    let _ = emitter.send(value);
                }
                _GetLastStreamId {
                    emitter,
                    stream_key,
                } => {
                    last_command_was_set = false;
                    let value = db._get_latest_stream_id(&stream_key);
                    let _ = emitter.send(value);
                }
                XRead { emitter, filters } => {
                    last_command_was_set = false;
                    let mut result: Vec<(String, Vec<StreamDbValueType>)> = vec![];

                    debug!(?filters, "THIS IS ON XREAD");

                    filters.iter().for_each(|(stream_key, stream_id)| {
                        let (ms, sq) = stream_id.split_once("-").unwrap();
                        let mut seq = sq.parse::<usize>().unwrap();
                        seq += 1;
                        let new_stream_id = format!("{}-{}", ms, seq.to_string());
                        debug!(
                            ?stream_key,
                            ?stream_id,
                            ?new_stream_id,
                            "Getting stream range"
                        );
                        let value = db._get_stream_range(
                            stream_key,
                            new_stream_id.clone(),
                            "+".to_string(),
                        );
                        result.push((stream_key.clone(), value));
                    });
                    debug!(?result, "XRead -- ");
                    let _ = emitter.send(result);
                }
            }
        }
    }

    fn _get_stream_range(
        &self,
        stream_key: &String,
        start: String,
        end: String,
    ) -> Vec<StreamDbValueType> {
        let (start_ms, start_sq, end_ms, end_sq) = get_start_end_ms_seq(&start, &end);
        match self.db.get(stream_key) {
            None => vec![],
            Some(value) => {
                let DbValueType::Stream(stream) = &value.value else {
                    return vec![];
                };
                let value = stream
                    .iter()
                    .filter(|value| {
                        value.stream_id_ms_part >= start_ms && value.stream_id_ms_part <= end_ms
                    })
                    .filter(|value| {
                        value.stream_id_seq_part >= start_sq && value.stream_id_seq_part <= end_sq
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                value
            }
        }
    }

    fn _keys(&self) -> Vec<String> {
        let value = self.db.keys().map(|k| k.to_owned()).collect();
        value
    }

    fn _set_stream(
        &mut self,
        stream_key: &String,
        stream_id: &String,
        key: &String,
        value: &String,
    ) -> Result<String, String> {
        info!("Setting stream: {} with value: {}", stream_key, value);
        let (ms_part, seq_part) = self._get_stream_id(stream_key, stream_id)?;
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

    fn _set(&mut self, key: &String, value: DbValueType, flags: Option<&HashMap<String, String>>) {
        info!("Setting key: {key} with value: {value:?}");
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

    fn _get_type(&mut self, key: &String) -> &str {
        let value = self.db.get(key);
        match value {
            None => "none",
            Some(kv) => match kv.value {
                DbValueType::String(_) => "string",
                DbValueType::Integer(_) => "integer",
                DbValueType::Stream(_) => "stream",
            },
        }
    }

    fn _get_latest_stream_id(&mut self, stream_key: &String) -> String {
        let stream = self
            .db
            .get(stream_key)
            .and_then(|stream_v| match &stream_v.value {
                DbValueType::Stream(stream) => stream.last(),
                _ => None,
            });
        let stream_id = match stream {
            None => "0-0".to_string(),
            Some(stream) => format!("{}-{}", stream.stream_id_ms_part, stream.stream_id_seq_part),
        };
        stream_id
    }
    fn _incr(&mut self, key: &String) -> Result<DbValueType, DbError> {
        let value = self._get(key);
        let Some(value) = value else {
            self.db.insert(
                key.to_owned(),
                DatabaseValue {
                    value: DbValueType::Integer(1),
                    exp_time: None,
                },
            );
            return Ok(DbValueType::Integer(1));
        };

        match value {
            DbValueType::Integer(v) => {
                let v = v + 1;
                self._set(key, DbValueType::Integer(v), None); // setting flag to None, probably shouldn't do this
                Ok(DbValueType::Integer(v))
            }
            DbValueType::String(v) => match v.parse::<i64>() {
                Ok(v) => {
                    let v = v + 1;
                    self._set(key, DbValueType::String(v.to_string()), None); // setting flag to None, probably shouldn't do this
                    Ok(DbValueType::String(v.to_string()))
                }
                Err(_) => Err(DbError::UnableToPerformAction(
                    "ERR value is not an integer or out of range".to_string(),
                )),
            },
            DbValueType::Stream(_) => Err(DbError::UnableToPerformAction(
                "ERR value is not an integer or out of range".to_string(),
            )),
        }

        // match value.parse::<i64>() {
        //     Ok(v) => {
        //         let v = v + 1;
        //         self._set(key, v.to_string(), None); // setting flag to None, probably shouldn't do this
        //         Ok(v)
        //     }
        //     Err(_) => Err(DbError::UnableToPerformAction(
        //         "ERR value is not an integer or out of range".to_string(),
        //     )),
        // }
    }

    fn _get(&mut self, key: &String) -> Option<DbValueType> {
        info!("Getting value for key: {}", key);
        let value = self.db.get(key);
        let Some(db_value) = value else {
            return None;
        };
        let value = match &db_value.value {
            DbValueType::Stream(..) => unimplemented!("GET not supported for streams"),
            e => e.clone(),
            // DbValueType::String(e) => e.clone(),
        };
        let Some(exp_time) = db_value.exp_time else {
            return Some(value);
        };
        if exp_time <= Instant::now() {
            self.db.remove(key);
            return None;
        }
        Some(value)
    }

    fn _get_stream_id(
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
        Ok((ms_part, seq_part))
    }
}
fn get_start_end_ms_seq(start: &String, end: &String) -> (u128, usize, u128, usize) {
    let min_usize = usize::MIN.to_string();
    let max_u128 = u128::MAX.to_string();
    let max_usize = usize::MAX.to_string();

    let (start_ms, start_sq) = if start == "-" {
        (min_usize.as_str(), min_usize.as_str())
    } else {
        start.split_once("-").unwrap()
    };
    let (last_ms, last_sq) = if end == "+" {
        (max_u128.as_str(), max_usize.as_str())
    } else {
        end.split_once("-").unwrap()
    };
    let start_ms = start_ms.parse::<u128>().unwrap_or(0);
    let start_sq = start_sq.parse::<usize>().unwrap_or(0);
    let end_ms = last_ms.parse::<u128>().unwrap_or(0);
    let end_sq = last_sq.parse::<usize>().unwrap_or(usize::MAX);
    (start_ms, start_sq, end_ms, end_sq)
}
