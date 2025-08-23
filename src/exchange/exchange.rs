use anyhow::Result;
use std::collections;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::exchange::transaction::Command;
use crate::{exchange::queue::Queue, rpc::proto};

use super::event::EventStatus;
use super::transaction::CommandResp;

pub struct ExchangeReq {
    command: proto::exchange::Command,
    resp: oneshot::Sender<ExchangeResp>,
}

impl ExchangeReq {
    pub fn new(
        command: proto::exchange::Command,
    ) -> (ExchangeReq, oneshot::Receiver<ExchangeResp>) {
        let (sender, receiver) = oneshot::channel();
        (
            ExchangeReq {
                command,
                resp: sender,
            },
            receiver,
        )
    }
}

pub struct ExchangeResp {
    pub command_resp: proto::exchange::CommandResp,
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
    #[error("add event request missing event")]
    AddEventRequestMissingEvent,
    #[error("update event status missing status")]
    UpdateEventStatusMissingStatus,
    #[error("command not provided")]
    CommandNotProvided,
}

pub struct Exchange {
    queues: collections::HashMap<String, Queue>,
}

impl Exchange {
    pub fn new() -> Exchange {
        Exchange {
            queues: collections::HashMap::new(),
        }
    }

    pub fn run(&mut self, mut receiver: mpsc::Receiver<ExchangeReq>) -> Result<()> {
        while let Some(msg) = receiver.blocking_recv() {
            let com = Command::try_from(msg.command)?;
            let com_resp = self.execute_command(&com)?;
            let com_resp = proto::exchange::CommandResp::try_from(com_resp)?;
            let res = msg.resp.send(ExchangeResp {
                command_resp: com_resp,
            });
            if let Err(_) = res {
                error!("unabled to send response message from exchange");
            }
        }
        info!("exiting exchange event-loop");
        Ok(())
    }

    fn add_queue(&mut self, queue: Queue) -> Result<()> {
        if let Some(_) = self.queues.get(&queue.name()) {
            return Err(ExchangeError::QueueAlreadyExistsForName(queue.name()).into());
        } else {
            self.queues.insert(queue.name(), queue);
        }
        Ok(())
    }

    fn update_event_status(
        &mut self,
        queue_name: &String,
        event_id: &u64,
        status: EventStatus,
    ) -> Result<()> {
        let queue = if let Some(queue) = self.queues.get_mut(queue_name) {
            queue
        } else {
            return Err(ExchangeError::QueueNotFound(queue_name.clone()).into());
        };

        let (event, event_exists) = queue.update_event_status(event_id, status.clone());

        if !event_exists || event.is_none() {
            return Ok(());
        }

        let transaction = if let Some(transaction) = queue.remove_transaction(event_id) {
            transaction
        } else {
            return Ok(());
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
            Command::AddQueue { name } => {
                let queue = Queue::new(name.clone());
                self.add_queue(queue)?;
                Ok(CommandResp::AddQueue {})
            }
            Command::AddEvent { queue_name, event } => {
                let queue = if let Some(queue) = self.queues.get_mut(queue_name) {
                    queue
                } else {
                    return Err(ExchangeError::QueueNotFound(queue_name.clone()).into());
                };
                let event_id = queue.add_event(event.clone());
                Ok(CommandResp::AddEvent { id: event_id })
            }
            Command::AddEvents { queue_name, events } => {
                let queue = if let Some(queue) = self.queues.get_mut(queue_name) {
                    queue
                } else {
                    return Err(ExchangeError::QueueNotFound(queue_name.clone()).into());
                };

                let event_ids: Vec<u64> = Vec::new();
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
                self.update_event_status(queue_name, event_id, status.clone())?;

                Ok(CommandResp::UpdateEventStatus {})
            }
            Command::CreateTransaction {
                queue_name,
                event_id,
            } => {
                let queue = if let Some(queue) = self.queues.get_mut(queue_name) {
                    queue
                } else {
                    return Err(ExchangeError::QueueNotFound(queue_name.clone()).into());
                };

                Ok(CommandResp::CreateTransaction { id: 0 })
            }
        }
    }
}
