use anyhow::Result;
use rand::Rng;
use tracing::{Level, info};

use orkidea::rpc::proto::exchange::{
    AddEvent, AddQueue, Attribute, Command, Event, ExecCommandReq, command,
    exchange_client::ExchangeClient,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(Level::INFO)
        .init();

    info!("--- Starting Orkidea ---");

    let mut client = ExchangeClient::connect("http://[::1]:5051").await?;

    let queue_idx: u32 = rand::rng().random_range(1..=1_000_000);
    let queue_name = format!("queue_{}", queue_idx);

    let resp = client
        .exec_command(tonic::Request::new(ExecCommandReq {
            command: Some(Command {
                command: Some(command::Command::AddQueue(AddQueue {
                    name: queue_name.clone(),
                })),
            }),
        }))
        .await?;

    info!("resp {:?}", resp);

    for event_idx in 0..3 {
        let resp = client
            .exec_command(tonic::Request::new(ExecCommandReq {
                command: Some(Command {
                    command: Some(command::Command::AddEvent(AddEvent {
                        queue_name: queue_name.clone(),
                        event: Some(Event {
                            id: 0,
                            data: "my data".into(),
                            attributes: vec![Attribute {
                                name: "key_1".to_string(),
                                value: "value_1".to_string(),
                            }],
                            status: None,
                        }),
                    })),
                }),
            }))
            .await?;

        info!("[{}] resp {:?}", event_idx, resp);
    }

    Ok(())
}
