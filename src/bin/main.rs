use anyhow::Result;
use tracing::{Level, info};

use orkidea::worker::worker::Worker;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_line_number(true)
        .with_max_level(Level::DEBUG)
        .init();

    info!("--- Starting Orkidea ---");

    Worker::new()?.run()?;

    Ok(())
}
