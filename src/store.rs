use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, SystemTime},
};

pub const KEY_MASTER_REPLID: &str = "__$$__master_replid";
pub const KEY_MASTER_REPL_OFFSET: &str = "__$$__master_repl_offset";
pub const KEY_IS_MASTER: &str = "__$$__is_master";
pub const KEY_REPLICA_PORT: &str = "__$$__replica_port";
pub const KEY_REPLCONF_ACK_OFFSET: &str = "__$$__replconf_ack_offset";
pub const KEY_IS_WAIT_RUNNING: &str = "__$$__is_wait_running";

pub struct Store {
    map: Mutex<HashMap<String, StoreValue>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }
    pub fn set(&self, key: String, value: String, expiry_duration: Option<Duration>) {
        let expiry_time = expiry_duration.map(|time| {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                + time
        });
        let mut map = self.map.lock().unwrap();
        let store_value = StoreValue { value, expiry_time };
        map.insert(key, store_value);
    }
    pub fn get(&self, key: String) -> Option<String> {
        let mut map = self.map.lock().unwrap();
        let store_value = map.get(&key)?;
        if let Some(exp_time) = store_value.expiry_time {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();
            if exp_time < now {
                map.remove(&key);
                return None;
            }
        }
        Some(store_value.value.clone())
    }
}

pub struct StoreValue {
    value: String,
    expiry_time: Option<Duration>,
}
