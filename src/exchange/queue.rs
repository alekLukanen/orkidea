use anyhow::Result;
use std::collections;
use thiserror::Error;

use crate::exchange::event::Event;

use super::event::EventStatus;

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
}

impl Queue {
    pub fn new(name: String) -> Queue {
        Queue {
            name,
            events: collections::HashMap::new(),
            event_idx: 0,
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
    ) -> Result<Option<Event>> {
        match status {
            EventStatus::Complete => {
                let mut event = if let Some(event) = self.events.remove(event_id) {
                    event
                } else {
                    return Err(QueueError::EventNotFound(event_id.clone()).into());
                };
                event.set_status(status);
                Ok(Some(event))
            }
            EventStatus::Errored
            | EventStatus::Running
            | EventStatus::Queued
            | EventStatus::MissedHeartbeat
            | EventStatus::Timedout => {
                let event = if let Some(event) = self.events.get_mut(event_id) {
                    event
                } else {
                    return Err(QueueError::EventNotFound(event_id.clone()).into());
                };
                event.set_status(status);
                Ok(None)
            }
        }
    }
}
