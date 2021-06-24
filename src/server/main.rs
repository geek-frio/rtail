#![feature(async_await)]
use env_logger;
mod transport;
mod common;
mod conn;
mod codec;

#[tokio::main]
async fn main() {
    env_logger::init();
}