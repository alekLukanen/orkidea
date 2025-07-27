use anyhow::Result;
use tokio::{runtime::Runtime, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::error;

use crate::{
    exchange::exchange::ExchangeReq,
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

        let address = "[::1]::5051".parse()?;
        let ct = self.ct.child_token();

        let exchange_svc = ExchangeServer::new(ExchangeImpl::new(ct.clone(), sender.clone()));

        self.runtime.spawn(async move {
            let res = Server::builder()
                .add_service(exchange_svc)
                .serve(address)
                .await;

            if let Err(err) = res {
                error!("RPC server error: {:?}", err);
            }

            ct.cancel();
        });

        // wait the for the worker to shutdown
        let ct = self.ct.child_token();
        self.runtime.block_on(async move {
            ct.cancelled().await;
        });

        Ok(())
    }
}
