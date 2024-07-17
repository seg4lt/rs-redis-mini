use std::{collections::HashMap, time::Instant};

use tokio::sync::oneshot;

#[derive(Debug)]
pub enum DatabaseEvent {
    Set {
        key: String,
        value: String,
        flags: HashMap<String, String>,
    },
    Get {
        emitter: oneshot::Sender<Option<String>>,
        key: String,
    },
    Keys {
        emitter: oneshot::Sender<Vec<String>>,
        flag: String,
    },
    Type {
        emitter: oneshot::Sender<String>,
        key: String,
    },
    XAdd {
        emitter: oneshot::Sender<Result<String, String>>,
        stream_key: String,
        stream_id: String,
        key: String,
        value: String,
    },
    XRange {
        emitter: oneshot::Sender<Vec<StreamDbValueType>>,
        stream_key: String,
        start: String,
        end: String,
    },
    XRead {
        emitter: oneshot::Sender<Vec<(String, Vec<StreamDbValueType>)>>,
        filters: Vec<(String, String)>,
    },
    WasLastCommandSet {
        emitter: oneshot::Sender<bool>,
    },
    _GetLastStreamId {
        emitter: oneshot::Sender<String>,
        stream_key: String,
    },
}
pub struct DatabaseValue {
    pub value: DbValueType,
    pub exp_time: Option<Instant>,
}

pub enum DbValueType {
    String(String),
    Stream(Vec<StreamDbValueType>),
}

#[derive(Clone, Debug)]
pub struct StreamDbValueType {
    pub stream_id_ms_part: u128,
    pub stream_id_seq_part: usize,
    pub key: String,
    pub value: String,
}
