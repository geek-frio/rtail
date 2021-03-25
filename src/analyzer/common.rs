use bytes::BytesMut;
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tonic::Code;
use tonic::{Request, Response, Status};

pub type AgentId = String;
pub type ErrorLogSender = UnboundedSender<OriginErrData>;

pub struct OriginErrData {
    data: BytesMut,
}

lazy_static! {
    pub static ref AGENT_MAP: Arc<DashMap<AgentId, ErrorLogSender>> = Arc::new(DashMap::new());
}

macro_rules! rpc_ok {
    ($t:ident) => {
        Ok(Response::new($t {
            status: "ok".to_string(),
            description: "".to_string(),
        }))
    };
}

macro_rules! rpc_err {
    ($m:literal, $t:ident) => {
        Ok(Response::new($t {
            status: "error".to_string(),
            description: $m.to_string(),
        }))
    };
}
