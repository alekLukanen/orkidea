use anyhow::Result;
use std::collections;
use thiserror::Error;

use crate::exchange::event::Event;

use super::{event::EventStatus, transaction::Transaction};

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("event not found: {0}")]
    EventNotFound(u64),
}

#[derive(Debug)]
pub struct Queue {
    name: String,

    events: collections::HashMap<u64, Event>,
    event_idx: u64,

    // indexed by event_id
    transactions: collections::HashMap<u64, Transaction>,
    transaction_idx: std::sync::atomic::AtomicU64,
}

impl Queue {
    pub fn new(name: String) -> Queue {
        Queue {
            name,
            events: collections::HashMap::new(),
            event_idx: 0,
            transactions: collections::HashMap::new(),
            transaction_idx: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn add_event(&mut self, mut event: Event) -> u64 {
        let event_idx = self.event_idx;
        event.set_id(event_idx);
        self.events.insert(event_idx, event);
        self.event_idx += 1;
        event_idx
    }

    pub fn update_event_status(
        &mut self,
        event_id: &u64,
        status: EventStatus,
    ) -> (Option<Event>, bool) {
        match status {
            EventStatus::Complete
            | EventStatus::Errored
            | EventStatus::MissedHeartbeat
            | EventStatus::Timedout => {
                let mut event = if let Some(event) = self.events.remove(event_id) {
                    event
                } else {
                    return (None, false);
                };
                event.set_status(status);
                (Some(event), true)
            }
            EventStatus::Running | EventStatus::Queued => {
                let event = if let Some(event) = self.events.get_mut(event_id) {
                    event
                } else {
                    return (None, false);
                };
                event.set_status(status);
                (None, true)
            }
        }
    }

    pub fn create_transaction(&mut self, event_id: u64) -> Result<u64> {
        let trans_id = self
            .transaction_idx
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.transactions.insert(
            event_id.clone(),
            Transaction::new(trans_id.clone(), event_id.clone()),
        );
        Ok(trans_id)
    }

    pub fn remove_transaction(&mut self, event_id: &u64) -> Option<Transaction> {
        self.transactions.remove(event_id)
    }
}
