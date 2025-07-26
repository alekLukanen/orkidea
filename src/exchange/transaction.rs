
use crate::exchange::event::{Event, EventStatus};


pub struct Transaction {
    id: u64,
    command_triggers: Vec<CommandTrigger>,

    last_heartbeat_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl Transaction {
    pub fn new(id: u64) -> Transaction {
        Transaction {
            id,
            on_completion_commands: Vec::new(),
            on_error_commands: Vec::new(),
        }
    }

    pub fn add_command_trigger(&mut self, trigger: Trigger, command: Command) {
        match self.command_triggers.iter_mut().find(|item| item.trigger == trigger) {
            Some(command_trigger) => {
                command_trigger.commands.push(command);
            }
            None => {
                let command_trigger = CommandTrigger {
                    trigger, commands: vec![command],
                };
                self.command_triggers.push(command_trigger);
            }
        }
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat_time = Some(chrono::Utc::new());
    }
}

#[derive(Debug)]
pub struct CommandTrigger {
    trigger: Trigger,
    commands: Vec<Command>,
}

#[derive(Debug, PartialEq)]
pub enum Trigger {
    OnEventSuccess(u64),
    OnEventError(u64),
    OnEventMissedHeartbeat(u64),
    OnEventTimeout(u64),
}

#[derive(Debug)]
pub enum Command {
    AddEvent(Event),
    AddEvents(Vec<Event>),
    UpdateEventStatus{
        event_id: u64,
        status: EventStatus,
    }
}