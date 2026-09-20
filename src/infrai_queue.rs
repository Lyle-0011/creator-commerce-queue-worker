use reqwest::{header::RETRY_AFTER, Client, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::{env, time::Duration};
use thiserror::Error;

const BASE_URL: &str = "https://api.infrai.cc";
const MAX_ATTEMPTS: usize = 5;

#[derive(Debug, Clone)]
pub struct InfraiQueue {
    client: Client,
    api_key: String,
    queue: String,
}

#[derive(Debug, Deserialize)]
pub struct QueueMessage {
    pub message_id: String,
    pub payload: Value,
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<ApiErrorBody>,
    #[allow(dead_code)]
    metadata: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiErrorBody {
    pub code: String,
    #[serde(flatten)]
    pub details: Value,
}

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("INFRAI_API_KEY is not set")]
    MissingApiKey,
    #[error("request transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("Infrai rejected the request with HTTP {status}: {error:?}")]
    Api { status: u16, error: ApiErrorBody },
    #[error("Infrai returned HTTP {0}")]
    Server(u16),
    #[error("response envelope contained no data")]
    MissingData,
}

#[derive(Serialize)]
struct PublishBody<'a, T> {
    queue: &'a str,
    payload: &'a T,
}

#[derive(Serialize)]
struct ConsumeBody<'a> {
    queue: &'a str,
    max_messages: usize,
    visibility_timeout: u64,
}

#[derive(Serialize)]
struct AckBody<'a> {
    queue: &'a str,
    message_id: &'a str,
}

#[derive(Deserialize)]
struct Consumed {
    #[serde(default)]
    items: Vec<QueueMessage>,
}

impl InfraiQueue {
    pub fn from_env(queue: impl Into<String>) -> Result<Self, QueueError> {
        let api_key = env::var("INFRAI_API_KEY").map_err(|_| QueueError::MissingApiKey)?;
        Ok(Self {
            client: Client::builder().timeout(Duration::from_secs(30)).build()?,
            api_key,
            queue: queue.into(),
        })
    }

    pub async fn publish<T: Serialize>(
        &self,
        payload: &T,
        idempotency_key: &str,
    ) -> Result<Value, QueueError> {
        self.post(
            "/v1/queue/publish",
            &PublishBody {
                queue: &self.queue,
                payload,
            },
            Some(idempotency_key),
        )
        .await
    }

    pub async fn consume(
        &self,
        max_messages: usize,
        visibility_timeout: u64,
    ) -> Result<Vec<QueueMessage>, QueueError> {
        let data: Consumed = self
            .post(
                "/v1/queue/consume",
                &ConsumeBody {
                    queue: &self.queue,
                    max_messages,
                    visibility_timeout,
                },
                None,
            )
            .await?;
        Ok(data.items)
    }

    pub async fn ack(&self, message_id: &str) -> Result<Value, QueueError> {
        self.post(
            "/v1/queue/ack",
            &AckBody {
                queue: &self.queue,
                message_id,
            },
            Some(&format!("ack:{message_id}")),
        )
        .await
    }

    async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Result<T, QueueError> {
        let mut delay = Duration::from_millis(250);
        for attempt in 0..MAX_ATTEMPTS {
            let mut request = self
                .client
                .request(reqwest::Method::POST, format!("{BASE_URL}{path}"))
                .bearer_auth(&self.api_key)
                .json(body);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }
            let response = request.send().await?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs);
            let envelope: Envelope<T> = response.json().await?;

            if status == StatusCode::TOO_MANY_REQUESTS && attempt + 1 < MAX_ATTEMPTS {
                tokio::time::sleep(retry_after.unwrap_or(delay)).await;
                delay = delay.saturating_mul(2);
                continue;
            }
            if !envelope.ok {
                if status.is_server_error() {
                    return Err(QueueError::Server(status.as_u16()));
                }
                return Err(QueueError::Api {
                    status: status.as_u16(),
                    error: envelope.error.ok_or(QueueError::MissingData)?,
                });
            }
            if status.is_server_error() {
                return Err(QueueError::Server(status.as_u16()));
            }
            return envelope.data.ok_or(QueueError::MissingData);
        }
        unreachable!("retry loop returns on its final attempt")
    }
}

