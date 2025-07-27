use crate::exchange::exchange::ExchangeReq;
use crate::rpc::proto::exchange::exchange_server::{Exchange, ExchangeServer};
use crate::rpc::proto::exchange::{AddEventReq, AddEventResp};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, transport::Server};

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
    async fn add_event(
        &self,
        request: Request<AddEventReq>, // Accept request of type HelloRequest
    ) -> Result<Response<AddEventResp>, Status> {
        // Return an instance of type HelloReply
        println!("Got a request: {:?}", request);

        let reply = AddEventResp { id: 0 };

        Ok(Response::new(reply)) // Send back our formatted greeting
    }
}
