use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{info};

pub fn start<T: Into<String>>(service_id: T, interval: Duration) -> JoinHandle<()> {
    let id = service_id.into();

    tokio::spawn(worker(id, interval))
}

async fn worker(id: String, _interval: Duration) {
    info!("Heartbeat worker started for {}", id);

    // write your code here
}
