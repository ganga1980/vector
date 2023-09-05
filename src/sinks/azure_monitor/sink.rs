use std::{fmt::Debug, io};

use bytes::Bytes;
use codecs::{encoding::Framer, CharacterDelimitedEncoder, JsonSerializerConfig};
// use lookup::{OwnedValuePath, PathPrefix};

use crate::sinks::prelude::*;

use super::service::AzureMonitorRequest;

pub struct AzureMonitorSink<S> {
    batch_settings: BatcherSettings,
    encoding: JsonEncoding,
    compression: Compression,
    service: S,
    protocol: String,
}

impl<S> AzureMonitorSink<S>
where
    S: Service<AzureMonitorRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: Debug + Into<crate::Error> + Send,
{
    pub fn new(
        batch_settings: BatcherSettings,
        transformer: Transformer,
        compression: Compression,
        service: S,
        protocol: String,
    ) -> Self {
        Self {
            batch_settings,
            encoding: JsonEncoding::new(transformer),
            compression,
            service,
            protocol,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        input
            .batched(self.batch_settings.into_byte_size_config())
            .request_builder(
                None,
                AzureMonitorRequestBuilder {
                    encoding: self.encoding,
                    compression: self.compression,
                },
            )
            .filter_map(|request| async {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service)
            .protocol(self.protocol.clone())
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl<S> StreamSink<Event> for AzureMonitorSink<S>
where
    S: Service<AzureMonitorRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: Debug + Into<crate::Error> + Send,
{
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

/// Customized encoding specific to the Azure Monitor Logs sink, as the API does not support full
/// 9-digit nanosecond precision timestamps.
#[derive(Clone, Debug)]
pub(super) struct JsonEncoding {
    encoder: (Transformer, Encoder<Framer>),
}

impl JsonEncoding {
    pub fn new(transformer: Transformer) -> Self {
        Self {
            encoder: (
                transformer,
                Encoder::<Framer>::new(
                    CharacterDelimitedEncoder::new(b',').into(),
                    JsonSerializerConfig::default().build().into(),
                ),
            ),
        }
    }
}

impl crate::sinks::util::encoding::Encoder<Vec<Event>> for JsonEncoding {
    fn encode_input(
        &self,
        input: Vec<Event>, // mut input: Vec<Event>,
        writer: &mut dyn io::Write,
    ) -> io::Result<(usize, GroupedCountByteSize)> {
        self.encoder.encode_input(input, writer)
    }
}

struct AzureMonitorRequestBuilder {
    encoding: JsonEncoding,
    compression: Compression,
}

impl RequestBuilder<Vec<Event>> for AzureMonitorRequestBuilder {
    type Metadata = EventFinalizers;
    type Events = Vec<Event>;
    type Encoder = JsonEncoding;
    type Payload = Bytes;
    type Request = AzureMonitorRequest;
    type Error = std::io::Error;

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoding
    }

    fn split_input(
        &self,
        mut events: Vec<Event>,
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let finalizers = events.take_finalizers();
        let builder = RequestMetadataBuilder::from_events(&events);
        (finalizers, builder, events)
    }

    fn build_request(
        &self,
        finalizers: Self::Metadata,
        request_metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        AzureMonitorRequest {
            body: payload.into_payload(),
            finalizers,
            metadata: request_metadata,
            compression: self.compression
        }
    }
}
