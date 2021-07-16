use std::convert::TryFrom;
use std::io::Error;

use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersionsResponse;
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, CreateTopicsRequest, CreateTopicsResponse, MetadataRequest,
    MetadataResponse, RequestHeader, RequestKind, ResponseHeader, ResponseKind,
};
use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::{Decodable, DecodeError, Encodable, EncodeError, HeaderVersion};
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
                .new_codec(),
        }
    }

    fn read_version(src: &mut BytesMut) -> Result<i16, ErrorKind> {
        let mut bytes = src.peek_bytes(2..4);
        Ok(bytes.try_get_i16()?)
    }
}

impl codec::Decoder for KafkaServerCodec {
    type Item = (RequestHeader, RequestKind);
    type Error = ErrorKind;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(mut bytes) = self.length_codec.decode(src)? {
            let version = Self::read_version(&mut bytes)?;
            let header = RequestHeader::decode(&mut bytes, version)?;
            let api_key = ApiKey::try_from(header.request_api_key)?;
            let request = decode(&mut bytes, api_key, version)?;
            Ok(Some((header, request)))
        } else {
            Ok(None)
        }
    }
}

impl codec::Encoder<(i16, ResponseHeader, ResponseKind)> for KafkaServerCodec {
    type Error = ErrorKind;

    fn encode(
        &mut self,
        item: (i16, ResponseHeader, ResponseKind),
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let (version, header, response) = item;
        let mut bytes = BytesMut::new();
        encode(&mut bytes, header, response, version);
        self.length_codec
            .encode(bytes.get_bytes(bytes.len()), dst)?;
        Ok(())
    }
}

fn encode(
    bytes: &mut BytesMut,
    header: ResponseHeader,
    response_kind: ResponseKind,
    version: i16,
) -> Result<(), ErrorKind> {
    match response_kind {
        ResponseKind::ApiVersionsResponse(res) => {
            header.encode(bytes, ApiVersionsResponse::header_version(version))?;
            res.encode(bytes, version)?;
        }
        ResponseKind::MetadataResponse(res) => {
            header.encode(bytes, MetadataResponse::header_version(version))?;
            res.encode(bytes, version);
        }
        ResponseKind::CreateTopicsResponse(res) => {
            header.encode(bytes, CreateTopicsResponse::header_version(version))?;
            res.encode(bytes, version);
        }
        _ => return Err(ErrorKind::UnsupportedOperation),
    };

    Ok(())
}

fn decode(bytes: &mut BytesMut, api_key: ApiKey, version: i16) -> Result<RequestKind, ErrorKind> {
    match api_key {
        ApiKey::ApiVersionsKey => {
            let req = ApiVersionsRequest::decode(bytes, version)?;
            Ok(RequestKind::ApiVersionsRequest(req))
        }
        ApiKey::MetadataKey => {
            let req = MetadataRequest::decode(bytes, version)?;
            Ok(RequestKind::MetadataRequest(req))
        }
        ApiKey::CreateTopicsKey => {
            let req = CreateTopicsRequest::decode(bytes, version)?;
            Ok(RequestKind::CreateTopicsRequest(req))
        }
        _ => Err(ErrorKind::UnsupportedOperation),
    }
}
