//! `Basic` sink.
//! A sink that will send it's output to standard out for pedagogical purposes.

use vector_config::configurable_component;

use vector_config::component::GenerateConfig;
use vector_core::config::AcknowledgementsConfig;
// use vector_common::internal_event::EventsSent;
// use vector_common::finalization::EventStatus;
use crate::sinks::prelude::SinkConfig;
use crate::sinks::prelude::SinkContext;
use crate::sinks::Healthcheck;
use futures::StreamExt;
use vector_core::sink::VectorSink;
use vector_core::config::Input;
// use vector_core::sink::StreamSink;
// use crate::event::proto::Event;
use futures::stream::BoxStream;
// use vector_common::internal_event::BytesSent;


use crate::{
    event::Event,
    sinks::util::StreamSink,
};


#[configurable_component(sink("basic"))]
#[derive(Clone, Debug)]
/// A basic sink that dumps its output to stdout.
pub struct BasicConfig {
    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl GenerateConfig for BasicConfig {
    fn generate_config() -> toml::Value {
        toml::from_str("").unwrap()
    }
}


#[async_trait::async_trait]
#[typetag::serde(name = "basic")]
impl SinkConfig for BasicConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let healthcheck = Box::pin(async move { Ok(()) });
        let sink = VectorSink::from_event_streamsink(BasicSink);

        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

struct BasicSink;

#[async_trait::async_trait]
impl StreamSink<Event> for BasicSink {
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

impl BasicSink {
    async fn run_inner(self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
         while let Some(event) = input.next().await {
           let bytes = format!("{:#?}", event);
           println!("{}", bytes);
        //    emit!(BytesSent{ byte_size: bytes.len(), protocol: "none".into()});


        //    emit!(BytesSent {
        //          byte_size: bytes.len(),
        //          protocol: "none".into()});

        //   let event_byte_size = event.estimated_json_encoded_size_of();
        //   emit!(EventsSent {
        //   events: 1,
        //   event_bytes: event_byte_size,
        //   output: None, });
        //    let finalizers = event.take_finalizers();
        //    finalizers.update_status(EventStatus::Delivered);
         }

        Ok(())
    }
}