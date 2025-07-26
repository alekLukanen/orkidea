
use std::collections;

use crate::exchange::event::Event;


pub struct Queue {
    name String,
    events: collections::HashMap<u64, Event>,
    event_idx: u64,
}

impl Queue {
    pub fn new(name String) Queue {
        Queue {
            name,
            events: collections::HashMap::new(),
            event_idx: 0,
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn add_event(&self, event: Event) -> u64 {
        let event_idx = self.event_idx;
        self.events.insert(event_idx, event);
        self.event_idx += 1;
        event_idx
    }
}