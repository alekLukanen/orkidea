use std::collections;
use anyhow::Result;
use thiserror::Error;

use crate::exchange::queue::Queue;
use crate::exchange::event::Event;


#[derive(Debug, Error)]
pub enum ExchangeError {
    #[error("queue already exists for name {0}")]
    QueueAlreadyExistsForName(String),
}


pub type Exchange {
    queues collections::HashMap<String, Queue>,
    
}

impl Exchange {
    pub fn new() Exchange {
        Exchange {
            queues: collections::HashMap::new(),
        }
    }

    pub fn add_queue(&self, queue: Queue) -> Result<()> {
        if let Some(queue) = self.queues.get(&queue.name()) {
            return Err(ExchangeError::QueueAlreadyExistsForName(queue.name()).into());
        } else {
            self.queues.insert(queue.name(), queue);
        }
    }

    pub fn create_transaction(&self)
}