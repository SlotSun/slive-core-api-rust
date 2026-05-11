use std::sync::{Mutex, OnceLock, RwLock};
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Method, Response, StatusCode};
use serde::de::DeserializeOwned;
use tracing::warn;

use crate::extractor::error::ExtractorError;
use crate::extractor::live_extractor::Result;

/// Ensure a rustls crypto provider is installed (once per process).
fn ensure_crypto_provider() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Default connect timeout.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Default read/response timeout.
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(15);
/// Maximum retry attempts for transient errors.
const DEFAULT_MAX_RETRIES: u32 = 3;
/// Base delay between retries (exponential backoff).
const DEFAULT_RETRY_BASE_DELAY: Duration = Duration::from_millis(500);

/// Stored builder configuration so the client can be rebuilt with a new user-agent.
struct BuilderConfig {
    connect_timeout: Duration,
    read_timeout: Duration,
    default_headers: HeaderMap,
}

/// A shared HTTP client with cookie management, default headers, timeouts, and
/// automatic retry for transient errors.
pub struct HttpClient {
    client: RwLock<Client>,
    cookies: Mutex<String>,
    builder_config: BuilderConfig,
}

impl HttpClient {
    /// Create a new builder.
    pub fn builder() -> HttpClientBuilder {
        HttpClientBuilder::default()
    }

    /// Set cookies that will be sent with every request.
    pub fn set_cookies(&self, cookies: &str) {
        *self.cookies.lock().unwrap() = cookies.to_string();
    }

    /// Get the current cookies.
    pub fn cookies(&self) -> String {
        self.cookies.lock().unwrap().clone()
    }

    /// Replace the inner `reqwest::Client` with one using a different user-agent.
    ///
    /// Preserves connect timeout, read timeout, and default headers from the
    /// original builder configuration.
    pub fn set_user_agent(&self, ua: &str) -> Result<()> {
        ensure_crypto_provider();
        let new_client = Client::builder()
            .user_agent(ua)
            .connect_timeout(self.builder_config.connect_timeout)
            .timeout(self.builder_config.read_timeout)
            .default_headers(self.builder_config.default_headers.clone())
            .build()
            .map_err(ExtractorError::HttpError)?;
        *self.client.write().unwrap() = new_client;
        Ok(())
    }

    /// Get a reference to the inner `reqwest::Client` for advanced use cases.
    pub fn inner(&self) -> Client {
        self.client.read().unwrap().clone()
    }

    // ------------------------------------------------------------------
    // High-level fetch methods
    // ------------------------------------------------------------------

    /// GET request returning response text.
    pub async fn get_text(&self, url: &str) -> Result<String> {
        let resp = self
            .execute_with_retry(|| self.attach_cookies(self.client.read().unwrap().get(url)))
            .await?;
        Ok(resp.text().await?)
    }

    /// GET request returning deserialized JSON.
    pub async fn get_json<T: DeserializeOwned>(&self, url: &str) -> Result<T> {
        let text = self.get_text(url).await?;
        serde_json::from_str(&text).map_err(ExtractorError::JsonError)
    }

    /// GET request with extra headers, returning deserialized JSON.
    pub async fn get_json_with_headers<T: DeserializeOwned>(
        &self,
        url: &str,
        extra_headers: &HeaderMap,
    ) -> Result<T> {
        let text = self.get_text_with_headers(url, extra_headers).await?;
        serde_json::from_str(&text).map_err(ExtractorError::JsonError)
    }

    /// GET request with extra headers, returning response text.
    pub async fn get_text_with_headers(
        &self,
        url: &str,
        extra_headers: &HeaderMap,
    ) -> Result<String> {
        let resp = self
            .execute_with_retry(|| {
                let mut req = self.client.read().unwrap().get(url);
                req = self.attach_cookies(req);
                for (key, value) in extra_headers.iter() {
                    req = req.header(key.clone(), value.clone());
                }
                req
            })
            .await?;
        Ok(resp.text().await?)
    }

    /// POST with form-encoded body, returning response text.
    pub async fn post_form_text<T: serde::Serialize + ?Sized>(
        &self,
        url: &str,
        form: &T,
    ) -> Result<String> {
        let resp = self
            .execute_with_retry(|| {
                let req = self.client.read().unwrap().post(url);
                self.attach_cookies(req).form(form)
            })
            .await?;
        Ok(resp.text().await?)
    }

    /// POST with form-encoded body, returning deserialized JSON.
    pub async fn post_form_json<T: DeserializeOwned, F: serde::Serialize + ?Sized>(
        &self,
        url: &str,
        form: &F,
    ) -> Result<T> {
        let text = self.post_form_text(url, form).await?;
        serde_json::from_str(&text).map_err(ExtractorError::JsonError)
    }

    /// POST with JSON body, returning response text.
    pub async fn post_json_text<B: serde::Serialize>(&self, url: &str, body: &B) -> Result<String> {
        let resp = self
            .execute_with_retry(|| {
                let req = self.client.read().unwrap().post(url);
                self.attach_cookies(req).json(body)
            })
            .await?;
        Ok(resp.text().await?)
    }

    /// POST with JSON body, returning deserialized JSON.
    pub async fn post_json_json<T: DeserializeOwned, B: serde::Serialize>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T> {
        let text = self.post_json_text(url, body).await?;
        serde_json::from_str(&text).map_err(ExtractorError::JsonError)
    }

    /// Low-level: build a request with the given method and URL, with cookies
    /// and default headers already attached. The caller can add more headers
    /// or body before calling `.send()`.
    pub fn request(&self, method: Method, url: &str) -> reqwest::RequestBuilder {
        let req = self.client.read().unwrap().request(method, url);
        self.attach_cookies(req)
    }

    /// Convenience: build a GET request with cookies attached.
    pub fn get(&self, url: &str) -> reqwest::RequestBuilder {
        self.attach_cookies(self.client.read().unwrap().get(url))
    }

    /// Convenience: build a POST request with cookies attached.
    pub fn post(&self, url: &str) -> reqwest::RequestBuilder {
        self.attach_cookies(self.client.read().unwrap().post(url))
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    /// Attach cookies to a request builder if any are set.
    fn attach_cookies(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let cookies = self.cookies.lock().unwrap().clone();
        if cookies.is_empty() {
            req
        } else {
            req.header(reqwest::header::COOKIE, cookies)
        }
    }

    /// Execute a request with automatic retry for transient errors.
    async fn execute_with_retry<F>(&self, build_request: F) -> Result<Response>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let mut last_err: Option<ExtractorError> = None;

        for attempt in 0..=DEFAULT_MAX_RETRIES {
            if attempt > 0 {
                let delay = DEFAULT_RETRY_BASE_DELAY * 2u32.pow(attempt - 1);
                tokio::time::sleep(delay).await;
            }

            match build_request().send().await {
                Ok(resp) => {
                    let status = resp.status();
                    // Retry on 5xx server errors (except 501 Not Implemented).
                    if status.is_server_error() && status != StatusCode::NOT_IMPLEMENTED {
                        warn!(
                            attempt = attempt + 1,
                            status = %status,
                            "Server error, retrying"
                        );
                        last_err = Some(ExtractorError::HttpError(
                            resp.error_for_status().unwrap_err(),
                        ));
                        continue;
                    }
                    // For 4xx and success, return as-is (error_for_status will
                    // convert 4xx to an error if the caller wants).
                    return Ok(resp);
                }
                Err(e) => {
                    if e.is_timeout() || e.is_connect() {
                        warn!(
                            attempt = attempt + 1,
                            error = %e,
                            "Network error, retrying"
                        );
                        last_err = Some(ExtractorError::HttpError(e));
                        continue;
                    }
                    // Non-retryable error (DNS, TLS, etc.)
                    return Err(ExtractorError::HttpError(e));
                }
            }
        }

        Err(last_err.unwrap_or_else(|| ExtractorError::Other("request failed".into())))
    }
}

// ===========================================================================
// Builder
// ===========================================================================

/// Builder for [`HttpClient`].
pub struct HttpClientBuilder {
    user_agent: String,
    connect_timeout: Duration,
    read_timeout: Duration,
    default_headers: HeaderMap,
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self {
            user_agent: crate::USER_AGENT.to_string(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            read_timeout: DEFAULT_READ_TIMEOUT,
            default_headers: HeaderMap::new(),
        }
    }
}

impl HttpClientBuilder {
    /// Set the User-Agent header.
    pub fn user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = ua.into();
        self
    }

    /// Set the connect timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the read/response timeout.
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout;
        self
    }

    /// Add a default header that will be sent with every request.
    pub fn default_header(
        mut self,
        name: impl TryInto<HeaderName>,
        value: impl TryInto<HeaderValue>,
    ) -> Self {
        if let (Ok(name), Ok(value)) = (name.try_into(), value.try_into()) {
            self.default_headers.insert(name, value);
        }
        self
    }

    /// Build the [`HttpClient`].
    pub fn build(self) -> Result<HttpClient> {
        ensure_crypto_provider();
        let client = Client::builder()
            .user_agent(&self.user_agent)
            .connect_timeout(self.connect_timeout)
            .timeout(self.read_timeout)
            .default_headers(self.default_headers.clone())
            .build()
            .map_err(ExtractorError::HttpError)?;

        Ok(HttpClient {
            client: RwLock::new(client),
            cookies: Mutex::new(String::new()),
            builder_config: BuilderConfig {
                connect_timeout: self.connect_timeout,
                read_timeout: self.read_timeout,
                default_headers: self.default_headers,
            },
        })
    }
}
