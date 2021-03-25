#[macro_use]
mod common;
mod frame;
mod service;
mod util;

pub mod collector {
    tonic::include_proto!("collector");
}
use crate::collector::log_collector_server::{LogCollector, LogCollectorServer};
use service::CustomLogCollector;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    let collector = CustomLogCollector {};

    Server::builder()
        .add_service(LogCollectorServer::new(collector))
        .serve(addr)
        .await?;
    Ok(())
}
