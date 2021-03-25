use bytes::BytesMut;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tonic::Code;
use tonic::{Request, Response, Status};

pub type AgentId = String;
pub type ErrorLogSender = UnboundedSender<BytesMut>;

pub struct OriginErrData {
    data: BytesMut,
}

lazy_static! {
    pub static ref AGENT_MAP: Arc<Mutex<HashMap<AgentId, ErrorLogSender>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

macro_rules! rpc_ok {
    ($t:ident) => {
        Ok(Response::new($t {
            status: "ok".to_string(),
            description: "".to_string(),
        }));
    };
}

macro_rules! rpc_err {
    ($m:literal, $t:ty) => {
        Ok(Response::new($t {
            status: "ok".to_string(),
            description: $m.to_string(),
        }));
    };
}
