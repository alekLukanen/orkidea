use crate::exchange::event::{Event, EventStatus};

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
}

#[derive(Debug)]
pub enum CommandResp {
    AddQueue {},
    AddEvent { id: u64 },
    AddEvents { ids: Vec<u64> },
    UpdateEventStatus {},
}
