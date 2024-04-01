use std::{net::TcpStream, sync::RwLock};

use anyhow::Context;

use crate::fdbg;

pub struct Replicas {
    replicas: RwLock<Vec<TcpStream>>,
}
impl Replicas {
    pub fn new() -> Self {
        Self {
            replicas: RwLock::new(Vec::new()),
        }
    }
    pub fn add(&self, replica: TcpStream) {
        let mut replicas = self.replicas.write().unwrap();
        replicas.push(replica);
    }
    pub fn get(&self, i: usize) -> Option<TcpStream> {
        let replicas = self.replicas.read().unwrap();
        replicas.get(i).map(|r| {
            r.try_clone()
                .context(fdbg!("Unable to clone tcp stream"))
                .unwrap()
        })
    }
    pub fn len(&self) -> usize {
        self.replicas.read().unwrap().len()
    }
}
