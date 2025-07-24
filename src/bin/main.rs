use anyhow::Result;
use tracing::{Level, info};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(Level::DEBUG)
        .init();

    info!("--- Starting Orkidea ---");

    Ok(())
}
