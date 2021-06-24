#![feature(async_await)]
use async_trait::async_trait;
use tokio::net::TcpSocket;
use super::common::Result;
use tokio::net::TcpListener;
use log::{debug, error, log_enabled, info, Level};


struct TcpLogTransport {
    cfg: TransportConfig,
}

impl Default for TcpLogTransport {
    fn default() -> Self {
        TcpLogTransport {
            cfg: TransportConfig::default()
        }
    }
}
struct TransportConfig {
    port: u32,
    ip: String,
}

impl TransportConfig {
    fn pair_addr(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}

impl Default for TransportConfig {
    fn default() -> Self {
        TransportConfig {
            port: 3456,
            ip: "127.0.0.1".to_string(),
        }
    }
}

#[async_trait]
trait Transport {
    async fn start_listen(&self) -> Result<()>;
}

#[async_trait]
impl Transport for TcpLogTransport{
    async fn start_listen(&self) -> Result<()>{
        let addr = self.cfg.pair_addr();
        let listener = TcpListener::bind(addr).await?;
        loop {
            let accept_res = listener.accept().await;
            match accept_res {
                Ok((socket, addr)) => {
                    info!("Accept a new connection {:?}", addr);
                    tokio::spawn(async {

                    });
                }
                Err(e) => {
                    error!("accept client failed! {:?}", e)
                }
            }

        }

        Ok(())
    }
}

impl TcpLogTransport {
}