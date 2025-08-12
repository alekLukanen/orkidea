use anyhow::Result;
use std::collections;
use std::sync::{Mutex, atomic};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::exchange::queue::Queue;

use super::event::EventStatus;
use super::transaction::{Command, CommandResp, Transaction};

pub struct ExchangeReq {
    command: Command,
    resp: oneshot::Sender<ExchangeResp>,
}

pub struct ExchangeResp {
    command_resp: CommandResp,
}

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

    pub fn run(&mut self, mut receiver: mpsc::Receiver<ExchangeReq>) -> Result<()> {
        while let Some(msg) = receiver.blocking_recv() {
            let resp = self.execute_command(&msg.command)?;
            let res = msg.resp.send(ExchangeResp { command_resp: resp });
            if let Err(_) = res {
                error!("unabled to response message from exchange");
            }
        }
        info!("exiting exchange event-loop");
        Ok(())
    }

    fn add_queue(&mut self, queue: Queue) -> Result<()> {
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

    fn update_event_status(
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
                self.execute_command(command)?;
            }
        }

        Ok(())
    }

    fn execute_command(&mut self, command: &Command) -> Result<CommandResp> {
        match command {
            Command::AddEvent { queue_name, event } => {
                let queue = if let Some(queue) = self.queues.get(queue_name) {
                    queue
                } else {
                    return Err(ExchangeError::QueueNotFound(queue_name.clone()).into());
                };
                let event_id = queue
                    .lock()
                    .map_err(|_| ExchangeError::LockError)?
                    .add_event(event.clone());
                Ok(CommandResp::AddEvent { id: event_id })
            }
            Command::AddEvents { queue_name, events } => {
                let queue = if let Some(queue) = self.queues.get(queue_name) {
                    queue
                } else {
                    return Err(ExchangeError::QueueNotFound(queue_name.clone()).into());
                };

                let event_ids: Vec<u64> = Vec::new();
                let mut queue = queue.lock().map_err(|_| ExchangeError::LockError)?;
                for event in events {
                    queue.add_event(event.clone());
                }
                Ok(CommandResp::AddEvents { ids: event_ids })
            }
            Command::UpdateEventStatus {
                queue_name,
                event_id,
                status,
            } => {
                let queue = if let Some(queue) = self.queues.get(queue_name) {
                    queue
                } else {
                    return Err(ExchangeError::QueueNotFound(queue_name.clone()).into());
                };
                queue
                    .lock()
                    .map_err(|_| ExchangeError::LockError)?
                    .update_event_status(event_id, status.clone())?;
                Ok(CommandResp::UpdateEventStatus {})
            }
        }
    }
}
