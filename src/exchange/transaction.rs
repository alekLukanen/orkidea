use crate::{exchange::event::EventStatus, rpc::proto};

use super::event::Event;

#[derive(Debug)]
pub struct Transaction {
    id: u64,
    event_id: u64,
    command_triggers: Vec<CommandTrigger>,

    last_heartbeat_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl Transaction {
    pub fn new(id: u64, event_id: u64) -> Transaction {
        Transaction {
            id,
            event_id,
            command_triggers: Vec::new(),
            last_heartbeat_time: None,
        }
    }

    pub fn add_command_trigger(&mut self, trigger: Trigger, command: Command) {
        match self
            .command_triggers
            .iter_mut()
            .find(|item| item.trigger == trigger)
        {
            Some(command_trigger) => {
                command_trigger.commands.push(command);
            }
            None => {
                let command_trigger = CommandTrigger {
                    trigger,
                    commands: vec![command],
                };
                self.command_triggers.push(command_trigger);
            }
        }
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat_time = Some(chrono::Utc::now());
    }

    pub fn get_command_triggers(&self) -> &Vec<CommandTrigger> {
        &self.command_triggers
    }
}

#[derive(Debug)]
pub struct CommandTrigger {
    trigger: Trigger,
    commands: Vec<Command>,
}

impl CommandTrigger {
    pub fn triggered_by_event_status_change(&self, event_status: EventStatus) -> bool {
        match self.trigger {
            Trigger::OnEventComplete(_) if event_status == EventStatus::Complete => true,
            Trigger::OnEventError(_) if event_status == EventStatus::Errored => true,
            Trigger::OnEventMissedHeartbeat(_) if event_status == EventStatus::MissedHeartbeat => {
                true
            }
            Trigger::OnEventTimedout(_) if event_status == EventStatus::Timedout => true,
            _ => false,
        }
    }

    pub fn get_commands(&self) -> &Vec<Command> {
        &self.commands
    }
}

#[derive(Debug, PartialEq)]
pub enum Trigger {
    OnEventComplete(u64),
    OnEventError(u64),
    OnEventMissedHeartbeat(u64),
    OnEventTimedout(u64),
}

#[derive(Debug)]
pub enum Command {
    AddQueue {
        name: String,
    },
    AddEvent {
        queue_name: String,
        event: Event,
    },
    AddEvents {
        queue_name: String,
        events: Vec<Event>,
    },
    UpdateEventStatus {
        queue_name: String,
        event_id: u64,
        status: EventStatus,
    },
    CreateTransaction {
        queue_name: String,
        event_id: u64,
    },
}

#[derive(Debug)]
pub enum CommandResp {
    AddQueue {},
    AddEvent { id: u64 },
    AddEvents { ids: Vec<u64> },
    UpdateEventStatus {},
    CreateTransaction { id: u64 },
}

impl TryFrom<CommandResp> for proto::exchange::CommandResp {
    type Error = anyhow::Error;

    fn try_from(value: CommandResp) -> Result<Self, Self::Error> {
        match value {
            CommandResp::AddQueue {} => Ok(proto::exchange::CommandResp {
                command_resp: Some(proto::exchange::command_resp::CommandResp::AddQueueResp(
                    proto::exchange::AddQueueResp {},
                )),
            }),
            CommandResp::AddEvent { id } => Ok(proto::exchange::CommandResp {
                command_resp: Some(proto::exchange::command_resp::CommandResp::AddEventResp(
                    proto::exchange::AddEventResp { id },
                )),
            }),
            CommandResp::AddEvents { ids } => Ok(proto::exchange::CommandResp {
                command_resp: Some(proto::exchange::command_resp::CommandResp::AddEventsResp(
                    proto::exchange::AddEventsResp { ids },
                )),
            }),
            CommandResp::UpdateEventStatus {} => Ok(proto::exchange::CommandResp {
                command_resp: Some(
                    proto::exchange::command_resp::CommandResp::UpdateEventStatusResp(
                        proto::exchange::UpdateEventStatusResp {},
                    ),
                ),
            }),
            CommandResp::CreateTransaction { id } => Ok(proto::exchange::CommandResp {
                command_resp: Some(
                    proto::exchange::command_resp::CommandResp::CreateTransactionResp(
                        proto::exchange::CreateTransactionResp { id },
                    ),
                ),
            }),
        }
    }
}

impl TryFrom<proto::exchange::Command> for Command {
    type Error = anyhow::Error;

    fn try_from(value: proto::exchange::Command) -> Result<Self, Self::Error> {
        match value.command.ok_or(anyhow::anyhow!("received None"))? {
            proto::exchange::command::Command::AddQueue(obj) => {
                Ok(Command::AddQueue { name: obj.name })
            }
            proto::exchange::command::Command::AddEvent(obj) => {
                let event = Event::try_from(obj.event.ok_or(anyhow::anyhow!("event was None"))?)?;
                Ok(Command::AddEvent {
                    queue_name: obj.queue_name,
                    event,
                })
            }
            proto::exchange::command::Command::AddEvents(obj) => {
                let mut events: Vec<Event> = Vec::new();
                for e in obj.events {
                    events.push(Event::try_from(e)?);
                }
                Ok(Command::AddEvents {
                    queue_name: obj.queue_name,
                    events,
                })
            }
            proto::exchange::command::Command::UpdateEventStatus(obj) => {
                let status =
                    EventStatus::try_from(obj.status.ok_or(anyhow::anyhow!("status was None"))?)?;
                Ok(Command::UpdateEventStatus {
                    queue_name: obj.queue_name,
                    event_id: obj.event_id,
                    status,
                })
            }
            proto::exchange::command::Command::CreateTransaction(obj) => {
                Ok(Command::CreateTransaction {
                    queue_name: obj.queue_name,
                    event_id: obj.event_id,
                })
            }
        }
    }
}
