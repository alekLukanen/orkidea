use anyhow::Result;
use std::collections;
use std::sync::{Mutex, atomic};
use thiserror::Error;

use crate::exchange::queue::Queue;

use super::event::EventStatus;
use super::transaction::{Command, CommandTrigger, Transaction};

#[derive(Debug, Error)]
pub enum ExchangeError {
    #[error("queue already exists for name {0}")]
    QueueAlreadyExistsForName(String),
    #[error("transaction not found: {0}")]
    TransactionNotFound(u64),
    #[error("queue not found: {0}")]
    QueueNotFound(String),
    #[error("lock error")]
    LockError,
}

pub struct Exchange {
    queues: collections::HashMap<String, Mutex<Queue>>,

    // indexed by event_id
    transactions: Mutex<collections::HashMap<u64, Transaction>>,
    transaction_idx: atomic::AtomicU64,
}

impl Exchange {
    pub fn new() -> Exchange {
        Exchange {
            queues: collections::HashMap::new(),
            transactions: Mutex::new(collections::HashMap::new()),
            transaction_idx: atomic::AtomicU64::new(0),
        }
    }

    pub fn add_queue(&mut self, queue: Queue) -> Result<()> {
        if let Some(_) = self.queues.get(&queue.name()) {
            return Err(ExchangeError::QueueAlreadyExistsForName(queue.name()).into());
        } else {
            self.queues.insert(queue.name(), Mutex::new(queue));
        }
        Ok(())
    }

    fn create_transaction(&self, event_id: u64) -> Result<u64> {
        let trans_id = self.transaction_idx.fetch_add(1, atomic::Ordering::Relaxed);
        self.transactions
            .lock()
            .map_err(|_| ExchangeError::LockError)?
            .insert(
                event_id.clone(),
                Transaction::new(trans_id.clone(), event_id.clone()),
            );
        Ok(trans_id)
    }

    pub fn update_event_status(
        &mut self,
        queue_name: &String,
        event_id: &u64,
        status: EventStatus,
    ) -> Result<()> {
        // complete the event
        let queue = if let Some(queue) = self.queues.get(queue_name) {
            queue
        } else {
            return Err(ExchangeError::QueueNotFound(queue_name.clone()).into());
        };

        let _ = queue
            .lock()
            .map_err(|_| ExchangeError::LockError)?
            .update_event_status(event_id, status.clone())?;

        // get the transaction and apply the trigger commands
        // as a result of the event status change
        let transaction = if let Some(transaction) = self
            .transactions
            .lock()
            .map_err(|_| ExchangeError::LockError)?
            .remove(event_id)
        {
            transaction
        } else {
            return Err(ExchangeError::TransactionNotFound(event_id.clone()).into());
        };

        for command_trigger in transaction.get_command_triggers() {
            if !command_trigger.triggered_by_event_status_change(status.clone()) {
                return Ok(());
            }

            for command in command_trigger.get_commands() {
                self.apply_command(command)?;
            }
        }

        Ok(())
    }

    pub fn apply_command(&mut self, command: &Command) -> Result<()> {
        match command {
            Command::AddEvent { queue_name, event } => {
                let queue = if let Some(queue) = self.queues.get(queue_name) {
                    queue
                } else {
                    return Err(ExchangeError::QueueNotFound(queue_name.clone()).into());
                };
                queue
                    .lock()
                    .map_err(|_| ExchangeError::LockError)?
                    .add_event(event.clone());
            }
            Command::AddEvents { queue_name, events } => {}
            Command::UpdateEventStatus {
                queue_name,
                event_id,
                status,
            } => {}
        }

        Ok(())
    }
}
