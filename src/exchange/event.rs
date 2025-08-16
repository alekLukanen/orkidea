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
