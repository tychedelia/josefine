use std::convert::TryFrom;
use std::io::Error;

use bytes::{Bytes, BytesMut, Buf};
use kafka_protocol::{Decodable, DecodeError, Decoder, Encodable, EncodeError, HeaderVersion};
use kafka_protocol::buf::ByteBuf;
use kafka_protocol::messages::{ApiKey, RequestHeader, RequestKind, ResponseHeader, ResponseKind, ApiVersionsRequest};
use kafka_protocol::messages::api_versions_response::ApiVersionsResponse;
use tokio_util::codec;

use crate::error::ErrorKind;

pub struct KafkaServerCodec {
    length_codec: codec::LengthDelimitedCodec,
}

impl KafkaServerCodec {
    pub fn new() -> Self {
        Self {
            length_codec: codec::LengthDelimitedCodec::builder()
                .max_frame_length(i32::MAX as usize)
                .length_field_length(4)
                .new_codec()
        }
    }

    fn read_version(src: &mut BytesMut) -> Result<i16, ErrorKind> {
        let mut bytes = src.peek_bytes(2..4);
        bytes.try_get_i16().map_err(|_| ErrorKind::DecodeError)
    }
}

impl codec::Decoder for KafkaServerCodec {
    type Item = (RequestHeader, RequestKind);
    type Error = ErrorKind;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(mut bytes) = self.length_codec.decode(src).map_err(|_| ErrorKind::DecodeError)? {
            let version = Self::read_version(&mut bytes)?;
            let header = RequestHeader::decode(&mut bytes, version).map_err(|_| ErrorKind::DecodeError)?;
            let api_key =  ApiKey::try_from(header.request_api_key).map_err(|_| ErrorKind::DecodeError)?;
            let request = decode(&mut bytes, api_key, version)?;
            Ok(Some((header, request)))
        } else {
            Ok(None)
        }
    }
}

impl codec::Encoder<(i16, ResponseHeader, ResponseKind)> for KafkaServerCodec {
    type Error = ErrorKind;

    fn encode(&mut self, item: (i16, ResponseHeader, ResponseKind), dst: &mut BytesMut) -> Result<(), Self::Error> {
        let (version, header, response) = item;
        let mut bytes = BytesMut::new();
        header.encode(&mut bytes, version);
        encode(&mut bytes, response, version);
        self.length_codec.encode(bytes.get_bytes(bytes.len()), dst)?;
        Ok(())
    }
}

fn encode(bytes: &mut BytesMut, response_kind: ResponseKind, version: i16) -> Result<(), ErrorKind> {
    match response_kind {
        ResponseKind::ApiVersionsResponse(res) => res.encode(bytes, ApiVersionsResponse::header_version(version)).map_err(|_| ErrorKind::EncodeError),
        _ => Err(ErrorKind::UnsupportedOperation)
    }
}

fn decode(bytes: &mut BytesMut, api_key: ApiKey, version: i16) -> Result<RequestKind, ErrorKind> {
    match api_key {
        ApiKey::ApiVersionsKey => {
             let req = ApiVersionsRequest::decode(bytes, version)
                 .map_err(|err| {
                     ErrorKind::DecodeError
                 })?;
            Ok(RequestKind::ApiVersionsRequest(req))
        }
        _ => {
            Err(ErrorKind::UnsupportedOperation)
        }
    }
}