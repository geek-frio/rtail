use super::common::OriginLogData;
use super::common::*;
use bytes::Bytes;
use bytes::BytesMut;
use std::error::Error;
use tokio::sync::mpsc::UnboundedReceiver;

struct FrameLogData {
    bytes_ary: Vec<Bytes>,
}

// Frame match过程红的两种日志格式：
// 1.正常业务日志格式,以时间开头的日志
//   Start_tag ["^日期" | "\n日期"]
//   End_tag ["\n日期"]（End_tag不真实消费) （match后字符不消费）
// 2.RPC等json日志格式
//   Start_tag ["^{" | "\n{"]
//   End_tag ["\n"] （消费掉）默认json格式无换行符号
struct FrameAnalyzer {
    buffer: BytesMut,
    agent_id: AgentId,
    rx: UnboundedReceiver<OriginLogData>,
    log_batch_data: Option<Bytes>,
}

struct Frame {
    agent_id: AgentId,
    buffer: Bytes,
}

impl FrameAnalyzer {
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, Box<dyn Error>> {
        loop {
            if let Some(frame) = self.parse_frame().await? {
                return Ok(Some(frame));
            }
        }
    }

    pub async fn parse_frame(&mut self) -> Result<Option<Frame>, Box<dyn Error>> {
        if self.log_batch_data.is_none() {
            let origin_log_data = self.rx.recv().await;
        }
        Ok(None)
    }
}
