//! The Azure Monitor [`vector_core::sink::VectorSink`]
//!
//! This module contains the [`vector_core::sink::VectorSink`] instance that is responsible for
//! taking a stream of [`vector_core::event::Event`] instances and forwarding them to the Azure
//! Monitor Logs service using GIG-LA endpoint.

mod config;
mod service;
mod sink;

pub use config::AzureMonitorConfig;
