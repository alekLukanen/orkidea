use anyhow::Result;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

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
        // wait the for the worker to shutdown
        let ct = self.ct.child_token();
        self.runtime.block_on(async move {
            ct.cancelled().await;
        });

        Ok(())
    }
}
