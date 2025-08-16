use thiserror::Error;

use crate::exchange::event::{Attribute, Event};
use crate::exchange::exchange::ExchangeReq;
use crate::exchange::transaction::{Command, CommandResp};
use crate::rpc::proto::exchange::exchange_server::{Exchange, ExchangeServer};
use crate::rpc::proto::exchange::{AddEventReq, AddEventResp, AddQueueReq, AddQueueResp};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, transport::Server};

#[derive(Error, Debug)]
pub enum ExchangeError {
    #[error("request missing event message")]
    RequestMissingEventMessage,
    #[error("unable to send request to internal exchange: {0}")]
    UnableToSendRequestToInternalExchange(String),
    #[error("exchange response error: {0}")]
    ExchangeResponseError(String),
}

#[derive(Debug)]
pub struct ExchangeImpl {
    ct: CancellationToken,
    exchange_chan: mpsc::Sender<ExchangeReq>,
}

impl ExchangeImpl {
    pub fn new(ct: CancellationToken, exchange_chan: mpsc::Sender<ExchangeReq>) -> ExchangeImpl {
        ExchangeImpl { ct, exchange_chan }
    }
}

#[tonic::async_trait]
impl Exchange for ExchangeImpl {
    async fn add_queue(
        &self,
        request: Request<AddQueueReq>,
    ) -> Result<Response<AddQueueResp>, Status> {
        let msg = request.into_inner();

        let (req, recv) = ExchangeReq::new(Command::AddQueue { name: msg.name });

        if let Err(err) = self
            .exchange_chan
            .send_timeout(req, std::time::Duration::from_secs(10))
            .await
        {
            return Err(Status::internal(
                ExchangeError::UnableToSendRequestToInternalExchange(err.to_string()).to_string(),
            ));
        };

        match tokio::time::timeout(std::time::Duration::from_secs(5), recv).await {
            Ok(Ok(resp)) => match resp.command_resp {
                CommandResp::AddQueue {} => Ok(Response::new(AddQueueResp {})),
                _ => Err(Status::internal(
                    ExchangeError::ExchangeResponseError("invalid response type".to_string())
                        .to_string(),
                )),
            },
            Ok(Err(err)) => Err(Status::internal(
                ExchangeError::ExchangeResponseError(err.to_string()).to_string(),
            )),
            Err(_) => Err(Status::internal(
                ExchangeError::ExchangeResponseError("timeout waiting for response".to_string())
                    .to_string(),
            )),
        }
    }

    async fn add_event(
        &self,
        request: Request<AddEventReq>, // Accept request of type HelloRequest
    ) -> Result<Response<AddEventResp>, Status> {
        // Return an instance of type HelloReply
        println!("Got a request: {:?}", request);

        let msg = request.into_inner();
        let msg_event = if let Some(event) = msg.event {
            event
        } else {
            return Err(Status::invalid_argument(
                ExchangeError::RequestMissingEventMessage.to_string(),
            ));
        };

        let attributes = msg_event
            .attributes
            .iter()
            .map(|item| Attribute::new(item.name.clone(), item.value.clone()))
            .collect::<Vec<_>>();
        let event = Event::new(0, msg_event.data, attributes);

        let (req, recv) = ExchangeReq::new(Command::AddEvent {
            queue_name: msg.queue_name,
            event,
        });

        if let Err(err) = self
            .exchange_chan
            .send_timeout(req, std::time::Duration::from_secs(10))
            .await
        {
            return Err(Status::internal(
                ExchangeError::UnableToSendRequestToInternalExchange(err.to_string()).to_string(),
            ));
        };

        match tokio::time::timeout(std::time::Duration::from_secs(5), recv).await {
            Ok(Ok(resp)) => match resp.command_resp {
                CommandResp::AddEvent { id } => Ok(Response::new(AddEventResp { id })),
                _ => Err(Status::internal(
                    ExchangeError::ExchangeResponseError("invalid response type".to_string())
                        .to_string(),
                )),
            },
            Ok(Err(err)) => Err(Status::internal(
                ExchangeError::ExchangeResponseError(err.to_string()).to_string(),
            )),
            Err(_) => Err(Status::internal(
                ExchangeError::ExchangeResponseError("timeout waiting for response".to_string())
                    .to_string(),
            )),
        }
    }
}
