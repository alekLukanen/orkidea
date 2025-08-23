use thiserror::Error;

use crate::exchange::exchange::ExchangeReq;
use crate::rpc::proto::exchange::exchange_server::Exchange;
use crate::rpc::proto::exchange::{ExecCommandReq, ExecCommandResp};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

#[derive(Error, Debug)]
pub enum ExchangeError {
    #[error("request missing event message")]
    RequestMissingEventMessage,
    #[error("unable to send request to internal exchange: {0}")]
    UnableToSendRequestToInternalExchange(String),
    #[error("exchange response error: {0}")]
    ExchangeResponseError(String),
    #[error("request missing command")]
    RequestMissingCommand,
}

#[derive(Debug)]
pub struct ExchangeImpl {
    exchange_chan: mpsc::Sender<ExchangeReq>,
}

impl ExchangeImpl {
    pub fn new(exchange_chan: mpsc::Sender<ExchangeReq>) -> ExchangeImpl {
        ExchangeImpl { exchange_chan }
    }
}

#[tonic::async_trait]
impl Exchange for ExchangeImpl {
    async fn exec_command(
        &self,
        request: Request<ExecCommandReq>,
    ) -> Result<Response<ExecCommandResp>, Status> {
        let msg = request.into_inner();

        let com = if let Some(com) = msg.command {
            com
        } else {
            return Err(Status::invalid_argument(
                ExchangeError::RequestMissingCommand.to_string(),
            ));
        };

        let (req, recv) = ExchangeReq::new(com);

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
            Ok(Ok(resp)) => Ok(Response::new(ExecCommandResp {
                command_resp: Some(resp.command_resp),
            })),
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
