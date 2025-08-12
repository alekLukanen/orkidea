use anyhow::Result;
use tokio::{runtime::Runtime, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, transport::Server};
use tower_http::trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::error;
use tracing::{Level, info};

use crate::{
    exchange::exchange::{Exchange, ExchangeReq},
    rpc::{proto::exchange::exchange_server::ExchangeServer, server::exchange::ExchangeImpl},
};

pub struct Worker {
    runtime: Runtime,
    ct: CancellationToken,
}

impl Worker {
    pub fn new() -> Result<Worker> {
        Ok(Worker {
            runtime: Runtime::new()?,
            ct: CancellationToken::new(),
        })
    }

    pub fn run(&self) -> Result<()> {
        let (sender, receiver) = mpsc::channel::<ExchangeReq>(4);

        let address = "[::1]:5051".parse()?;
        let ct = self.ct.child_token();

        // start the exchange
        let mut exchange = Exchange::new();
        let exchange_handle = std::thread::spawn(move || {
            if let Err(err) = exchange.run(receiver) {
                error!(error = format!("{:?}", err), "error running exchange");
            }
        });

        // start the exchange rpc server
        let exchange_svc = ExchangeServer::new(ExchangeImpl::new(ct.clone(), sender.clone()));
        self.runtime.spawn(async move {
            let res = Server::builder()
                .layer(
                    TraceLayer::new_for_grpc()
                        .on_request(DefaultOnRequest::new().level(tracing::Level::INFO))
                        .on_response(DefaultOnResponse::new().level(tracing::Level::INFO)),
                )
                .add_service(exchange_svc)
                .serve(address)
                .await;

            if let Err(err) = res {
                error!("RPC server error: {:?}", err);
            }

            ct.cancel();
        });

        // wait for the exchange to shutdown
        if let Err(_) = exchange_handle.join() {
            error!("error joining the exchange thread");
        };

        // wait the for the async componenets to shutdown
        let ct = self.ct.child_token();
        self.runtime.block_on(async move {
            ct.cancelled().await;
        });

        Ok(())
    }
}
