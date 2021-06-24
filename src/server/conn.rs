use tokio::net::TcpStream;
use std::future::Future;
use std::task::{Context, Poll};
use async_trait::async_trait;
use std::pin::Pin;

#[async_trait]
trait ConnService {
   async fn serve(&mut self);
}

struct Connector {
   stream: TcpStream,
}

#[async_trait]
impl ConnService for Connector {
   async fn serve(&mut self) {
       let (reader, _) = self.stream.split();

   }
}
