use anyhow::Result;

use crate::rpc::proto;

#[derive(Debug, Clone)]
pub struct Attribute {
    name: String,
    value: String,
}

impl Attribute {
    pub fn new(name: String, value: String) -> Attribute {
        Attribute { name, value }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum EventStatus {
    Queued,
    Running,
    Complete,
    Errored,
    MissedHeartbeat,
    Timedout,
}

#[derive(Debug, Clone)]
pub struct Event {
    id: u64,
    data: Vec<u8>,
    attributes: Vec<Attribute>,
    status: EventStatus,
}

impl Event {
    pub fn new(id: u64, data: Vec<u8>, attributes: Vec<Attribute>) -> Event {
        Event {
            id,
            data,
            attributes,
            status: EventStatus::Queued,
        }
    }

    pub fn set_id(&mut self, id: u64) {
        self.id = id;
    }

    pub fn set_status(&mut self, status: EventStatus) {
        self.status = status;
    }
}

impl TryFrom<proto::exchange::Event> for Event {
    type Error = anyhow::Error;

    fn try_from(obj: proto::exchange::Event) -> Result<Event, Self::Error> {
        let status = if let Some(status) = obj.status {
            EventStatus::try_from(status)?
        } else {
            EventStatus::Queued
        };

        let mut attributes: Vec<Attribute> = Vec::new();
        for item in obj.attributes {
            attributes.push(Attribute::try_from(item)?);
        }

        let event = Event {
            id: obj.id,
            status,
            attributes,
            data: obj.data.clone(),
        };

        Ok(event)
    }
}

impl TryFrom<proto::exchange::Status> for EventStatus {
    type Error = anyhow::Error;

    fn try_from(obj: proto::exchange::Status) -> Result<EventStatus, Self::Error> {
        match obj.data_type {
            Some(proto::exchange::status::DataType::Queued(_)) => Ok(EventStatus::Queued),
            Some(proto::exchange::status::DataType::Running(_)) => Ok(EventStatus::Running),
            Some(proto::exchange::status::DataType::Complete(_)) => Ok(EventStatus::Complete),
            Some(proto::exchange::status::DataType::Errored(_)) => Ok(EventStatus::Errored),
            Some(proto::exchange::status::DataType::MissedHeartbeat(_)) => {
                Ok(EventStatus::MissedHeartbeat)
            }
            Some(proto::exchange::status::DataType::Timedout(_)) => Ok(EventStatus::Timedout),
            None => Err(anyhow::anyhow!("received None")),
        }
    }
}

impl TryFrom<proto::exchange::Attribute> for Attribute {
    type Error = anyhow::Error;

    fn try_from(obj: proto::exchange::Attribute) -> Result<Attribute, Self::Error> {
        Ok(Attribute {
            name: obj.name,
            value: obj.value,
        })
    }
}
