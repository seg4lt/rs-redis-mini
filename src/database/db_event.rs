use std::{collections::HashMap, time::Instant};

use thiserror::Error;
use tokio::sync::oneshot::Sender;

#[derive(Debug)]
pub enum DatabaseEvent {
    Set {
        key: String,
        value: String,
        flags: HashMap<String, String>,
    },
    Get {
        emitter: Sender<Option<String>>,
        key: String,
    },
    Incr {
        emitter: Sender<Result<i64, String>>,
        key: String,
    },
    Keys {
        emitter: Sender<Vec<String>>,
        flag: String,
    },
    Type {
        emitter: Sender<String>,
        key: String,
    },
    XAdd {
        emitter: Sender<Result<String, String>>,
        stream_key: String,
        stream_id: String,
        key: String,
        value: String,
    },
    XRange {
        emitter: Sender<Vec<StreamDbValueType>>,
        stream_key: String,
        start: String,
        end: String,
    },
    XRead {
        emitter: Sender<Vec<(String, Vec<StreamDbValueType>)>>,
        filters: Vec<(String, String)>,
    },
    WasLastCommandSet {
        emitter: Sender<bool>,
    },
    _GetLastStreamId {
        emitter: Sender<String>,
        stream_key: String,
    },
}

#[derive(Debug, Clone)]
pub struct DatabaseValue {
    pub value: DbValueType,
    pub exp_time: Option<Instant>,
}

#[derive(Clone, Debug)]
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

#[derive(Error, Debug, Clone)]
pub enum DbError {
    #[error("{0}: unknown error")]
    Unknown(String),

    #[error("{0}")]
    UnableToPerformAction(String),
}
