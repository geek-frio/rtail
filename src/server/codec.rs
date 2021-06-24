use tokio_util::codec::Decoder;
use bytes::{Buf, BytesMut};
use std::io::Error;
use super::common::ErrorMsg;
use bytes::Bytes;
use std::ops::Index;

const PKT_LEN: usize = 2;

pub(crate) struct RawLogDecoder {
    read_num: usize,
    spli_sym: u8,
}

impl Decoder for RawLogDecoder {
    type Item = RawLogFrame;
    type Error = ErrorMsg;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < PKT_LEN {
            return Ok(None);
        }
        let size = src.get_u16();
        if src.len() < size as usize {
            src.reserve(size as usize - src.len())
        }

        Ok(None)
    }
}