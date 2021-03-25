use super::collector::{CollectStatus, LineData, ShakeMeta, ShakeStatus};
use super::common::AgentId;
use super::{LogCollector, LogCollectorServer};
use tokio::sync::mpsc::unbounded_channel;
use tonic::metadata::errors::ToStrError;
use tonic::Code;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct CustomLogCollector;

struct WrapStrError(ToStrError);

impl From<ToStrError> for WrapStrError {
    fn from(error: ToStrError) -> Self {
        WrapStrError(error)
    }
}

impl From<WrapStrError> for Status {
    fn from(error: WrapStrError) -> Self {
        Status::new(Code::Unknown, error.0.to_string())
    }
}

impl CustomLogCollector {
    async fn init() -> CustomLogCollector {
        CustomLogCollector {}
    }

    fn get_agent_id<T>(request: &Request<T>) -> Result<Option<AgentId>, WrapStrError> {
        let agent_id = request.metadata().get("agent_id");
        if let Some(id) = agent_id {
            return Ok(Some(id.to_str()?.to_string()));
        }
        Ok(None)
    }
}

#[tonic::async_trait]
impl LogCollector for CustomLogCollector {
    async fn log_data_lines(
        &self,
        request: Request<tonic::Streaming<LineData>>,
    ) -> Result<Response<CollectStatus>, Status> {
        let agent_id = Self::get_agent_id(&request)?;
        //return match agent_id {
        //    None => Ok(Response::new(CollectStatus {
        //        status: "false".to_string(),
        //        description: "no agent_id set, not collected".to_string(),
        //    })),
        //    Some(agent_id) => {
        //        if !self.processor_map.contains_key(&agent_id) {
        //            let map = (*self.processor_map).clone();
        //        }
        //        let mut stream = request.into_inner();
        //        while let Some(line_data) = stream.next().await {
        //            let line_data = line_data?;
        //            println!("line data:{:?}", line_data);
        //        }
        //        Ok(Response::new(CollectStatus {
        //            status: "ok".to_string(),
        //            description: "".to_string(),
        //        }))
        //    }
        //};
        return rpc_ok!(CollectStatus);
    }

    async fn hand_shake(
        &self,
        request: Request<ShakeMeta>,
    ) -> Result<Response<ShakeStatus>, Status> {
        let shake_meta = request.into_inner();
        let mut map = AGENT_MAP.lock().await;
        if map.contains_key(&shake_meta.agentid) {
            return rpc_ok!(ShakeStatus);
        }
        let (sender, receiver) = unbounded_channel();
        map.insert(shake_meta.agentid, sender);
        return rpc_ok!(ShakeStatus);
    }
}
