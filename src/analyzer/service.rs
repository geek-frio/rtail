use super::collector::{CollectStatus, LineData, ShakeMeta, ShakeStatus};
use super::common::*;
use super::{LogCollector, LogCollectorServer};
use futures_util::StreamExt;
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

impl From<LineData> for OriginLogData {
    fn from(data: LineData) -> Self {
        let bytes = data.linedata.into_bytes().as_slice().into();
        OriginLogData { data: bytes }
    }
}

impl CustomLogCollector {
    async fn init() -> CustomLogCollector {
        CustomLogCollector {}
    }

    fn get_agent_id<T>(request: &Request<T>) -> Result<Option<AgentId>, WrapStrError> {
        let agent_id = request.metadata().get("agentid");
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
        return match agent_id {
            Some(agent_id) => {
                let sender = AGENT_MAP.get(&agent_id);
                match sender {
                    Some(s) => {
                        let mut stream = request.into_inner();
                        while let Some(line_data) = stream.next().await {
                            let origin = line_data?;
                            let res = s.send(OriginLogData::from(origin));
                            if let Err(e) = res {
                                println!("send msg failed! err:{:?}", e.to_string());
                            }
                        }
                        rpc_ok!(CollectStatus)
                    }
                    None => {
                        rpc_err!("handshake should be made first", CollectStatus)
                    }
                }
            }
            None => {
                rpc_err!(
                    "agent id not exists, this log will be rejected",
                    CollectStatus
                )
            }
        };
    }

    // Every time a new file is being watched, log agent will try to do handshake operation
    async fn hand_shake(
        &self,
        request: Request<ShakeMeta>,
    ) -> Result<Response<ShakeStatus>, Status> {
        let shake_meta = request.into_inner();
        if AGENT_MAP.contains_key(&shake_meta.agentid) {
            return rpc_ok!(ShakeStatus);
        }
        let (sender, mut receiver) = unbounded_channel();
        AGENT_MAP.insert(shake_meta.agentid.clone(), sender);

        // we generate a job for each log agent
        tokio::spawn(async move {
            let agentid = shake_meta.agentid;
            // do receving job
            while let Some(origin_data) = receiver.recv().await {}
        });
        return rpc_ok!(ShakeStatus);
    }
}
