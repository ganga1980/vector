use bytes::Bytes;
use http::{
    header::{self, HeaderMap},
    HeaderValue, Request, StatusCode, Uri,
};
// use http::{
//     header::{self, HeaderMap},
//     HeaderName, HeaderValue, Request, StatusCode, Uri,
// };
use hyper::Body;
// use lookup::lookup_v2::OwnedValuePath;
// use once_cell::sync::Lazy;
// use openssl::{base64, hash, pkey, sign};
// use regex::Regex;
use std::task::{Context, Poll};
use tracing::Instrument;

use crate::{http::HttpClient, sinks::prelude::*};

/// For Azure Monitor, the resource ID of the resource emitting the logs
// static X_MS_AZURE_RESOURCE_HEADER: Lazy<HeaderName> = Lazy::new(|| HeaderName::from_static("azure-monitor-source-resourceId"));

// static CONTENT_TYPE_VALUE: Lazy<HeaderValue> = Lazy::new(|| HeaderValue::from_static(CONTENT_TYPE));

/// JSON content type of logs
// const CONTENT_TYPE: &str = "application/json";

/// API version for GIG-LA ingestion
// const API_VERSION: &str = "2021-11-01-preview";


#[derive(Debug, Clone)]
pub struct AzureMonitorRequest {
    pub body: Bytes,
    pub finalizers: EventFinalizers,
    pub metadata: RequestMetadata,
}

impl MetaDescriptive for AzureMonitorRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

impl Finalizable for AzureMonitorRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.finalizers.take_finalizers()
    }
}

pub struct AzureMonitorResponse {
    pub http_status: StatusCode,
    pub events_byte_size: GroupedCountByteSize,
    pub raw_byte_size: usize,
}

impl DriverResponse for AzureMonitorResponse {
    fn event_status(&self) -> EventStatus {
        match self.http_status.is_success() {
            true => EventStatus::Delivered,
            false => EventStatus::Rejected,
        }
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.events_byte_size
    }

    fn bytes_sent(&self) -> Option<usize> {
        Some(self.raw_byte_size)
    }
}

/// `AzureMonitorService` is a `Tower` service used to send logs to Azure.
#[derive(Debug, Clone)]
pub struct AzureMonitorService {
    client: HttpClient,
    endpoint: Uri,
    default_headers: HeaderMap,
}

impl AzureMonitorService {
    /// Creates a new `AzureMonitorService`.
    pub fn new(
        client: HttpClient,
        endpoint: Uri,
        azure_resource_id: Option<&str>,
        token: &str,
    ) -> crate::Result<Self> {
        println!("AzureMonitorService::new: endpoint: {:?}, azure_resource_id: {:?}", endpoint, azure_resource_id);

        println!("AzureMonitorService::new: token: {:?}", token);

        let default_headers = {
            let mut headers = HeaderMap::new();
            if let Some(azure_resource_id) = azure_resource_id {
                if azure_resource_id.is_empty() {
                    return Err("azure_resource_id can't be an empty string".into());
                }

                 headers.insert(
                    "azure-monitor-source-resourceId",
                    HeaderValue::from_str(azure_resource_id)?,
                );
            }

            let content_type = format!("{}", "application/json");
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_str(&content_type)?);

            let bearer_token = format!("Bearer {}", token);
            headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(&bearer_token)?,
            );

            headers
        };

        println!("AzureMonitorService::new:  default_headers: {:?}", default_headers);

        Ok(Self {
            client,
            endpoint,
            default_headers,
        })
    }

    fn build_request(&self, body: Bytes) -> crate::Result<Request<Body>> {
        let mut request = Request::post(&self.endpoint).body(Body::from(body))?;
        *request.headers_mut() = self.default_headers.clone();
        Ok(request)
    }

    pub fn healthcheck(&self) -> Healthcheck {
        let mut client = self.client.clone();
        let request = self.build_request(Bytes::from("[]"));
        Box::pin(async move {
            let request = request?;
            let res = client.call(request).in_current_span().await?;

            if res.status().is_server_error() {
                return Err("Server returned a server error".into());
            }

            if res.status() == StatusCode::FORBIDDEN {
                return Err("The service failed to authenticate the request. Verify that the workspace ID and connection key are valid".into());
            }

            if res.status() == StatusCode::NOT_FOUND {
                return Err(
                    "Either the URL provided is incorrect, or the request is too large".into(),
                );
            }

            if res.status() == StatusCode::BAD_REQUEST {
                return Err("The workspace has been closed or the request was invalid".into());
            }

            Ok(())
        })
    }
}

impl Service<AzureMonitorRequest> for AzureMonitorService {
    type Response = AzureMonitorResponse;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    // Emission of Error internal event is handled upstream by the caller.
    fn poll_ready(&mut self, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    // Emission of Error internal event is handled upstream by the caller.
    fn call(&mut self, request: AzureMonitorRequest) -> Self::Future {
        let mut client = self.client.clone();
        let http_request = self.build_request(request.body);
        Box::pin(async move {
            let http_request = http_request?;
            let response = client.call(http_request).in_current_span().await?;
            Ok(AzureMonitorResponse {
                http_status: response.status(),
                raw_byte_size: request.metadata.request_encoded_size(),
                events_byte_size: request
                    .metadata
                    .into_events_estimated_json_encoded_byte_size(),
            })
        })
    }
}
