use anyhow::Result;
use std::collections;
use std::sync::{Mutex, atomic};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::exchange::queue::Queue;
use crate::rpc::proto::exchange::{
    AddEventResp, AddEventsResp, AddQueueResp, Command, CommandResp, CreateTransactionResp,
    ErrorResp, UpdateEventStatusResp, command, command_resp,
};

use super::event::{Event, EventStatus};
use super::transaction::Transaction;

pub struct ExchangeReq {
    command: Command,
    resp: oneshot::Sender<ExchangeResp>,
}

impl ExchangeReq {
    pub fn new(command: Command) -> (ExchangeReq, oneshot::Receiver<ExchangeResp>) {
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
    pub command_resp: CommandResp,
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
            let resp = self.execute_command(&msg.command)?;
            let res = msg.resp.send(ExchangeResp { command_resp: resp });
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
        match &command.command {
            Some(command::Command::AddQueue(obj)) => {
                let queue = Queue::new(obj.name.clone());
                self.add_queue(queue)?;
                Ok(CommandResp {
                    command_resp: Some(command_resp::CommandResp::AddQueueResp(AddQueueResp {})),
                })
            }
            Some(command::Command::AddEvent(obj)) => {
                let queue = if let Some(queue) = self.queues.get_mut(&obj.queue_name) {
                    queue
                } else {
                    return Err(ExchangeError::QueueNotFound(obj.queue_name.clone()).into());
                };
                let event = Event::try_from(
                    obj.event
                        .clone()
                        .ok_or(ExchangeError::AddEventRequestMissingEvent)?,
                )?;
                let event_id = queue.add_event(event);
                Ok(CommandResp {
                    command_resp: Some(command_resp::CommandResp::AddEventResp(AddEventResp {
                        id: event_id,
                    })),
                })
            }
            Some(command::Command::AddEvents(obj)) => {
                let queue = if let Some(queue) = self.queues.get_mut(&obj.queue_name) {
                    queue
                } else {
                    return Err(ExchangeError::QueueNotFound(obj.queue_name.clone()).into());
                };

                let mut events: Vec<Event> = Vec::new();
                for event in &obj.events {
                    let event = Event::try_from(event.clone())?;
                    events.push(event);
                }

                let event_ids: Vec<u64> = Vec::new();
                for event in events {
                    queue.add_event(event.clone());
                }

                Ok(CommandResp {
                    command_resp: Some(command_resp::CommandResp::AddEventsResp(AddEventsResp {
                        ids: event_ids,
                    })),
                })
            }
            Some(command::Command::UpdateEventStatus(obj)) => {
                let status = EventStatus::try_from(
                    obj.status
                        .ok_or(ExchangeError::UpdateEventStatusMissingStatus)?,
                )?;
                self.update_event_status(&obj.queue_name, &obj.event_id, status)?;
                Ok(CommandResp {
                    command_resp: Some(command_resp::CommandResp::UpdateEventStatusResp(
                        UpdateEventStatusResp {},
                    )),
                })
            }
            Some(command::Command::CreateTransaction(obj)) => {
                let queue = if let Some(queue) = self.queues.get_mut(&obj.queue_name) {
                    queue
                } else {
                    return Err(ExchangeError::QueueNotFound(obj.queue_name.clone()).into());
                };

                Ok(CommandResp {
                    command_resp: Some(command_resp::CommandResp::CreateTransactionResp(
                        CreateTransactionResp { id: 0 },
                    )),
                })
            }
            None => Err(ExchangeError::CommandNotProvided.into()),
        }
    }
}
