use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

pub struct Store {
    map: HashMap<String, StoreValue>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    pub fn set(&mut self, key: String, value: String, expiry_time: Option<Duration>) {
        let expiry_time = expiry_time.map(|time| {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                + time
        });
        let store_value = StoreValue { value, expiry_time };
        self.map.insert(key, store_value);
    }
    pub fn get(&mut self, key: String) -> Option<String> {
        let store_value = self.map.get(&key)?;
        if let Some(exp_time) = store_value.expiry_time {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();
            if exp_time < now {
                self.map.remove(&key);
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
