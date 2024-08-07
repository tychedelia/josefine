use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use kafka_protocol::messages::api_versions_response::ApiVersionsResponse;
use kafka_protocol::messages::*;

use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};
use tokio_util::codec;

use crate::kafka::error::ErrorKind;
use crate::kafka::error::ErrorKind::{DecodeError, EncodeError};

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
            let header =
                RequestHeader::decode(&mut bytes, version).map_err(|_| ErrorKind::DecodeError)?;
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
        encode(&mut bytes, header, response, version)?;
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
        ResponseKind::ApiVersions(res) => {
            header
                .encode(bytes, ApiVersionsResponse::header_version(version))
                .map_err(|_| ErrorKind::EncodeError)?;
            res.encode(bytes, version)
                .map_err(|_| ErrorKind::EncodeError)?;
        }
        ResponseKind::Metadata(res) => {
            header
                .encode(bytes, MetadataResponse::header_version(version))
                .map_err(|_| ErrorKind::EncodeError)?;
            res.encode(bytes, version)
                .map_err(|_| ErrorKind::EncodeError)?;
        }
        ResponseKind::CreateTopics(res) => {
            header
                .encode(bytes, CreateTopicsResponse::header_version(version))
                .map_err(|_| ErrorKind::EncodeError)?;
            res.encode(bytes, version)
                .map_err(|_| ErrorKind::EncodeError)?;
        }
        ResponseKind::ListGroups(res) => {
            header
                .encode(bytes, ListGroupsResponse::header_version(version))
                .map_err(|_| ErrorKind::EncodeError)?;
            res.encode(bytes, version)
                .map_err(|_| ErrorKind::EncodeError)?;
        }
        ResponseKind::FindCoordinator(res) => {
            header
                .encode(bytes, FindCoordinatorResponse::header_version(version))
                .map_err(|_| ErrorKind::EncodeError)?;
            res.encode(bytes, version)
                .map_err(|_| ErrorKind::EncodeError)?;
        }
        _ => return Err(ErrorKind::UnsupportedOperation),
    };

    Ok(())
}

fn decode(bytes: &mut BytesMut, api_key: ApiKey, version: i16) -> Result<RequestKind, ErrorKind> {
    match api_key {
        ApiKey::ApiVersionsKey => {
            let req =
                ApiVersionsRequest::decode(bytes, version).map_err(|_| ErrorKind::DecodeError)?;
            Ok(RequestKind::ApiVersions(req))
        }
        ApiKey::MetadataKey => {
            let req =
                MetadataRequest::decode(bytes, version).map_err(|_| ErrorKind::DecodeError)?;
            Ok(RequestKind::Metadata(req))
        }
        ApiKey::CreateTopicsKey => {
            let req =
                CreateTopicsRequest::decode(bytes, version).map_err(|_| ErrorKind::DecodeError)?;
            Ok(RequestKind::CreateTopics(req))
        }
        ApiKey::ListGroupsKey => {
            let req =
                ListGroupsRequest::decode(bytes, version).map_err(|_| ErrorKind::DecodeError)?;
            Ok(RequestKind::ListGroups(req))
        }
        ApiKey::FindCoordinatorKey => {
            let req = FindCoordinatorRequest::decode(bytes, version)
                .map_err(|_| ErrorKind::DecodeError)?;
            Ok(RequestKind::FindCoordinator(req))
        }
        _ => Err(ErrorKind::UnsupportedOperation),
    }
}

#[derive(Debug)]
pub struct KafkaClientCodec {
    correlation_id: AtomicI32,
    requests: Arc<Mutex<HashMap<i32, RequestHeader>>>,
    length_codec: codec::LengthDelimitedCodec,
}

impl KafkaClientCodec {
    pub fn new(requests: Arc<Mutex<HashMap<i32, RequestHeader>>>) -> Self {
        Self {
            correlation_id: Default::default(),
            requests,
            length_codec: codec::LengthDelimitedCodec::builder()
                .max_frame_length(i32::MAX as usize)
                .length_field_length(4)
                .new_codec(),
        }
    }
}

impl codec::Decoder for KafkaClientCodec {
    type Item = (ResponseHeader, ResponseKind);
    type Error = ErrorKind;

    #[tracing::instrument]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(mut bytes) = self.length_codec.decode(src)? {
            let header =
                ResponseHeader::decode(&mut bytes, 1).map_err(|_| ErrorKind::DecodeError)?;
            let mut request_header = self.requests.lock().unwrap();
            let request_header = request_header
                .remove(&header.correlation_id)
                .ok_or(DecodeError)?;
            let api_key = ApiKey::try_from(request_header.request_api_key)?;
            let response =
                decode_response(&mut bytes, api_key, request_header.request_api_version)?;
            Ok(Some((header, response)))
        } else {
            Ok(None)
        }
    }
}

fn decode_response(
    bytes: &mut BytesMut,
    api_key: ApiKey,
    version: i16,
) -> Result<ResponseKind, ErrorKind> {
    match api_key {
        ApiKey::ApiVersionsKey => {
            let res =
                ApiVersionsResponse::decode(bytes, CreateTopicsResponse::header_version(version))
                    .map_err(|_| ErrorKind::DecodeError)?;
            Ok(ResponseKind::ApiVersions(res))
        }
        ApiKey::LeaderAndIsrKey => {
            let res =
                LeaderAndIsrResponse::decode(bytes, LeaderAndIsrResponse::header_version(version))
                    .map_err(|_| ErrorKind::DecodeError)?;
            Ok(ResponseKind::LeaderAndIsr(res))
        }
        ApiKey::CreateTopicsKey => {
            let res =
                CreateTopicsResponse::decode(bytes, CreateTopicsResponse::header_version(version))
                    .map_err(|_| ErrorKind::DecodeError)?;
            Ok(ResponseKind::CreateTopics(res))
        }
        _ => Err(ErrorKind::UnsupportedOperation),
    }
}

impl codec::Encoder<(RequestHeader, RequestKind)> for KafkaClientCodec {
    type Error = ErrorKind;

    #[tracing::instrument]
    fn encode(
        &mut self,
        item: (RequestHeader, RequestKind),
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let (mut header, request) = item;
        header.correlation_id = self.correlation_id.fetch_add(1, Ordering::SeqCst);
        let mut bytes = BytesMut::new();
        let api_version = header.request_api_version;
        let mut requests = self.requests.lock().unwrap();
        requests.insert(header.correlation_id, header.clone());
        encode_request(&mut bytes, header, request, api_version)?;
        self.length_codec
            .encode(bytes.get_bytes(bytes.len()), dst)?;
        Ok(())
    }
}

fn encode_request(
    bytes: &mut BytesMut,
    header: RequestHeader,
    request: RequestKind,
    version: i16,
) -> Result<(), ErrorKind> {
    match request {
        RequestKind::ApiVersions(req) => {
            header
                .encode(bytes, LeaderAndIsrRequest::header_version(version))
                .map_err(|_| ErrorKind::EncodeError)?;
            req.encode(bytes, version)
                .map_err(|_| ErrorKind::EncodeError)?;
        }
        RequestKind::LeaderAndIsr(req) => {
            header
                .encode(bytes, LeaderAndIsrRequest::header_version(version))
                .map_err(|_| ErrorKind::EncodeError)?;
            req.encode(bytes, version)
                .map_err(|_| ErrorKind::EncodeError)?;
        }
        RequestKind::CreateTopics(req) => {
            header
                .encode(bytes, CreateTopicsRequest::header_version(version))
                .map_err(|_| ErrorKind::EncodeError)?;
            req.encode(bytes, version)
                .map_err(|_| ErrorKind::EncodeError)?;
        }
        _ => return Err(EncodeError),
    };

    Ok(())
}
