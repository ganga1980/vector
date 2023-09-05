//use lookup::{lookup_v2::OptionalValuePath, OwnedValuePath};
// use openssl::{base64, pkey};

// use vector_common::sensitive_string::SensitiveString;
use vector_config::configurable_component;
// use vector_core::{config::log_schema, schema};
// use vrl::value::Kind;

use crate::{
    http::{get_http_scheme_from_uri, HttpClient},
    sinks::{
        prelude::*,
        util::{http::HttpStatusRetryLogic, RealtimeSizeBasedDefaultBatchSettings, UriSerde},
    },
};

use super::{
    service::{AzureMonitorResponse, AzureMonitorService},
    sink::AzureMonitorSink,
};

// use reqwest::Client;
use reqwest::{self, StatusCode};
use serde::Deserialize;
use std::env;
// use std::time::{Duration};
// use tokio::time;
// use chrono::Utc;
use serde::Serialize;
use serde_json;


#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct IMDSResponse {
    access_token: String,
    expires_on: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AgentConfiguration {
    configurations: Vec<Configuration>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct Configuration {
    configurationId: String,
    eTag: String,
    op: String,
    content: Content,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct Content {
    dataSources: Vec<DataSource>,
    channels: Vec<Channel>,
    extensionConfigurations: Option<ExtensionConfigurations>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct DataSource {
    configuration: Configuration2,
    id: String,
    kind: String,
    streams: Vec<Stream>,
    sendToChannels: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct Configuration2 {
    extensionName: Option<String>,
    filePatterns: Option<Vec<String>>,
    format: Option<String>,
    settings: Option<Settings>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Settings {
    text: Text,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct Text {
    recordStartTimestampFormat: String,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct Stream {
    stream: String,
    solution: Option<String>,
    extensionOutputStream: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct Channel {
    endpoint: Option<String>,
    tokenEndpointUri: String,
    id: String,
    protocol: String,
    endpointUriTemplate: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct ExtensionConfigurations {
    ContainerInsights: Vec<ContainerInsight>,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct ContainerInsight {
    id: String,
    originIds: Vec<String>,
    outputStreams: OutputStreams,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct OutputStreams {
    CONTAINER_LOG_BLOB: String,
    CONTAINERINSIGHTS_CONTAINERLOGV2: String,
    KUBE_EVENTS_BLOB: String,
    KUBE_POD_INVENTORY_BLOB: String,
    KUBE_NODE_INVENTORY_BLOB: String,
    KUBE_PV_INVENTORY_BLOB: String,
    KUBE_SERVICES_BLOB: String,
    KUBE_MON_AGENT_EVENTS_BLOB: String,
    INSIGHTS_METRICS_BLOB: String,
    CONTAINER_INVENTORY_BLOB: String,
    CONTAINER_NODE_INVENTORY_BLOB: String,
    LINUX_PERF_BLOB: String,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct IngestionTokenResponse {
    configurationId: String,
    ingestionAuthToken: String,
}



#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
#[allow(dead_code)]
struct DataItemLAv2 {
    #[serde(rename = "TimeGenerated")]
    time_generated: String,
    #[serde(rename = "Computer")]
    computer: String,
    #[serde(rename = "ContainerId")]
    container_id: String,
    #[serde(rename = "ContainerName")]
    container_name: String,
    #[serde(rename = "PodName")]
    pod_name: String,
    #[serde(rename = "PodNamespace")]
    pod_namespace: String,
    #[serde(rename = "LogMessage")]
    log_message: String,
    #[serde(rename = "LogSource")]
    log_source: String,
}

/// Max number of bytes (1MB max supported in Gig-LA) in request body
const MAX_BATCH_SIZE: usize = 1 * 1024 * 1024;

pub(super) fn default_host() -> String {
    "global.handler.control.monitor.azure.com".into()
}

pub(super) fn default_resource_id() -> Option<String> {
    let resource_id: Option<String> = match env::var("AKS_RESOURCE_ID") {
        Ok(value) => Some(value),
        Err(_) => None,
    };
    resource_id
}

/// Configuration for the `azure_monitor` sink.
#[configurable_component(sink(
    "azure_monitor",
    "Publish logs to the Azure Monitor through GIG-LA endpoint."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct AzureMonitorConfig {
    /// The [Resource ID][resource_id] of the Azure resource the data should be associated with.
    ///
    /// [resource_id]: https://docs.microsoft.com/en-us/azure/azure-monitor/platform/data-collector-api#request-headers
    #[configurable(metadata(
        docs::examples = "/subscriptions/11111111-1111-1111-1111-111111111111/resourcegroups/examplegroup/providers/Microsoft.ContainerService/managedClusters/examplecluster"
    ))]
    #[configurable(metadata(
        docs::examples = "/subscriptions/11111111-1111-1111-1111-111111111111/resourcegroups/rg/providers/Microsoft.ContainerService/managedClusters/name"
    ))]
    #[serde(default = "default_resource_id")]
    pub azure_resource_id: Option<String>,

    /// [Alternative host][alt_host] for dedicated Azure regions.
    ///
    /// [alt_host]: https://docs.azure.cn/en-us/articles/guidance/developerdifferences#check-endpoints-in-azure
    #[configurable(metadata(docs::examples = "global.handler.control.monitor.azure.us"))]
    #[configurable(metadata(docs::examples = "global.handler.control.monitor.azure.cn"))]
    #[serde(default = "default_host")]
    pub(super) host: String,

    #[configurable(derived)]
    #[serde(
        default,
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    pub encoding: Transformer,

    #[configurable(derived)]
    #[serde(default = "Compression::gzip_default")]
    pub compression: Compression,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    pub tls: Option<TlsConfig>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for AzureMonitorConfig {
    fn default() -> Self {
        Self {
            azure_resource_id: default_resource_id(),
            host: default_host(),
            encoding: Default::default(),
            compression: Default::default(),
            batch: Default::default(),
            request: Default::default(),
            tls: None,
            acknowledgements: Default::default(),
        }
    }
}

impl AzureMonitorConfig {
    pub(super) async fn build_inner(
        &self,
        cx: SinkContext,
        endpoint: UriSerde,
        token: &str,
    ) -> crate::Result<(VectorSink, Healthcheck)> {
        let endpoint = endpoint.with_default_parts().uri;
        let protocol = get_http_scheme_from_uri(&endpoint).to_string();

        let batch_settings = self
            .batch
            .validate()?
            .limit_max_bytes(MAX_BATCH_SIZE)?
            .into_batcher_settings()?;

        let tls_settings = TlsSettings::from_options(&self.tls)?;
        let client = HttpClient::new(Some(tls_settings), &cx.proxy)?;

        let service = AzureMonitorService::new(
            client,
            endpoint,
            self.azure_resource_id.as_deref(),
            token,
        )?;
        let healthcheck = service.healthcheck();

        let retry_logic =
            HttpStatusRetryLogic::new(|res: &AzureMonitorResponse| res.http_status);
        let request_settings = self.request.unwrap_with(&Default::default());
        let service = ServiceBuilder::new()
            .settings(request_settings, retry_logic)
            .service(service);

        let sink = AzureMonitorSink::new(
            batch_settings,
            self.encoding.clone(),
            self.compression.clone(),
            service,
            protocol,
        );

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }
}

impl_generate_config_from_default!(AzureMonitorConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "azure_monitor")]
impl SinkConfig for AzureMonitorConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        // let endpoint = format!("https://{}", self.host).parse()?;
        let mut imds_access_token = String::new();
        let mut gig_token_endpoint = String::new();
        let mut endpoint_uritemplate = String::new();
        let mut ingestion_token = String::new();

        match get_imds_token().await {
            Ok((access_token, expires_on)) => {
                imds_access_token = access_token;
                println!("IMDS Access Token: {}", imds_access_token);
                println!("IMDS Access Token Expiry: {}", expires_on);
            }
            Err(err) => {
                println!("Error: {}", err);
            }
        }

        println!("Invoking get_gig_endpoints()");
        match get_gig_endpoints(&imds_access_token).await {
            Ok((gig_token_endpoint_uri, gig_endpoint_uritemplate)) => {
                gig_token_endpoint = gig_token_endpoint_uri;
                endpoint_uritemplate = gig_endpoint_uritemplate;
                println!("GIG Token Endpoint: {}", gig_token_endpoint);
                println!("Endpoint Uri Template: {}", endpoint_uritemplate);
            }
            Err(err) => {
                println!("Error: {}", err);
            }
        }

        println!("Invoking get_gig_ingestion_token()");
        match get_gig_ingestion_token(&imds_access_token, &gig_token_endpoint).await {
            Ok(ingestion_auth_token) => {
                ingestion_token = ingestion_auth_token;
                println!("GIG Ingestion Auth Token: {}", ingestion_token);
            }
            Err(err) => {
                println!("Error: {}", err);
            }
        }

        let gig_endpoint_url =
            endpoint_uritemplate.replace("<STREAM>", "CONTAINERINSIGHTS_CONTAINERLOGV2");
        println!("gig_endpoint_url: {}", gig_endpoint_url);

        let endpoint = format!("{}", gig_endpoint_url).parse()?;

        self.build_inner(cx, endpoint, &ingestion_token).await
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

async fn get_imds_token() -> Result<(String, String), reqwest::Error> {
    let token_url =
        "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2019-08-01&resource=https://monitor.azure.com/";
    let client = reqwest::Client::new();
    let mut access_token = String::new();
    let mut token_expiry = String::new();

    let response = client
        .get(token_url)
        .header("Metadata", "true")
        .send()
        .await?;

    if response.status().is_success() {
        let token_response: IMDSResponse = response.json().await?;
        access_token = token_response.access_token;
        token_expiry = token_response.expires_on;
        println!("IMDS Access Token: {}", access_token);
    } else {
        println!("Response status: {}", response.status());
    }

    Ok((access_token, token_expiry))
}

async fn get_gig_endpoints(imds_access_token: &str) -> Result<(String, String), reqwest::Error> {
    let mut endpoint_uritemplate = String::new();
    let mut gig_token_endpoint: String = String::new();

    let resource_id = env::var("AKS_RESOURCE_ID").unwrap_or_default();
    let resource_region = env::var("AKS_REGION").unwrap_or_default();
    let client = reqwest::Client::new();

    println!("get_gig_endpoints::resource_id: {}", resource_id);
    println!("get_gig_endpoints::resource_region: {}", resource_region);

    let amcs_endpoint = format!("https://global.handler.control.monitor.azure.com{}/agentConfigurations?platform=linux&api-version=2022-06-02", resource_id);
    println!("get_gig_endpoints::amcs_endpoint: {}", amcs_endpoint);

    let bearer = format!("Bearer {}", imds_access_token);
    println!("get_gig_endpoints::bearer: {}", bearer);
    let mut agentconfig_redirected_endpoint = String::new();

    let mut response = client
        .get(amcs_endpoint)
        .header("Authorization", bearer)
        .send()
        .await?;

    println!("get_gig_endpoints::Response status: {}", response.status());
    if response.status() == StatusCode::MISDIRECTED_REQUEST {
        if let Some(agentconfig_endpoint) = response.headers().get("x-ms-agent-config-endpoint") {
            if let Ok(agentconfig_endpoint_value) = agentconfig_endpoint.to_str() {
                agentconfig_redirected_endpoint = agentconfig_endpoint_value.to_string();
                println!("x-ms-agent-config-endpoint: {}", agentconfig_endpoint_value);
            }
            let amcs_redirected_endpoint = format!("{}{}/agentConfigurations?operatingLocation={}&platform=linux&api-version=2022-06-02", agentconfig_redirected_endpoint, resource_id, resource_region);
            println!("amcs_redirected_endpoint: {}", amcs_redirected_endpoint);
            let bearer_clone = format!("Bearer {}", imds_access_token);
            response = client
                .get(amcs_redirected_endpoint)
                .header("Authorization", bearer_clone)
                .send()
                .await?;

            println!("get_gig_endpoints: Response status: {}", response.status());
            if response.status().is_success() {
                let json_string = response.text().await?;
                let agent_configuration: AgentConfiguration =
                    serde_json::from_str(&json_string).unwrap();
                if agent_configuration.configurations.len() > 0 {
                    for i in 0..agent_configuration.configurations[0].content.channels.len() {
                        let channel = &agent_configuration.configurations[0].content.channels[i];
                        if channel.protocol == "gig" {
                            gig_token_endpoint = channel.tokenEndpointUri.clone();
                            endpoint_uritemplate =
                                channel.endpointUriTemplate.clone().unwrap_or_default();
                        }
                    }
                }
            }
        } else {
            println!(
                "x-ms-agent-config-endpoint header not found: {:?}",
                response.headers()
            );
        }
    }

    Ok((gig_token_endpoint, endpoint_uritemplate))
}

async fn get_gig_ingestion_token(
    imds_access_token: &str,
    gig_token_endpoint: &str,
) -> Result<String, reqwest::Error> {
    let client = reqwest::Client::new();
    let mut ingestion_token = String::new();
    let bearer = format!("Bearer {}", imds_access_token);

    println!(
        "get_gig_ingestion_token::gig_token_endpoint: {}",
        gig_token_endpoint
    );
    println!("get_gig_ingestion_token::bearer: {}", bearer);

    let response = client
        .get(gig_token_endpoint)
        .header("Authorization", bearer)
        .send()
        .await?;

    println!(
        "get_gig_ingestion_token: response status: {}",
        response.status()
    );

    if response.status().is_success() {
        let json_string = response.text().await?;
        let ingestion_token_response: IngestionTokenResponse =
            serde_json::from_str(&json_string).unwrap();
        ingestion_token = ingestion_token_response.ingestionAuthToken;
        println!(
            "get_gig_ingestion_token::ingestionAuthToken: {}",
            ingestion_token
        );
    }

    Ok(ingestion_token)
}
