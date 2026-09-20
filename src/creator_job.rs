use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CreatorJob {
    DeliverDigitalAsset {
        order_id: String,
        asset_id: String,
        buyer_id: String,
    },
    UpdateSubscriber {
        creator_id: String,
        subscriber_id: String,
        active: bool,
    },
    ProcessContent {
        creator_id: String,
        content_id: String,
        source: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobOutcome {
    pub event: &'static str,
    pub subject_id: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum JobError {
    #[error("required job identifier is empty")]
    MissingIdentifier,
    #[error("content source must use https")]
    InvalidContentSource,
}

pub async fn process(job: &CreatorJob) -> Result<JobOutcome, JobError> {
    match job {
        CreatorJob::DeliverDigitalAsset {
            order_id,
            asset_id,
            buyer_id,
        } => {
            require_ids([order_id, asset_id, buyer_id])?;
            Ok(JobOutcome {
                event: "digital_asset_delivered",
                subject_id: order_id.clone(),
            })
        }
        CreatorJob::UpdateSubscriber {
            creator_id,
            subscriber_id,
            active,
        } => {
            require_ids([creator_id, subscriber_id])?;
            Ok(JobOutcome {
                event: if *active {
                    "subscriber_activated"
                } else {
                    "subscriber_deactivated"
                },
                subject_id: subscriber_id.clone(),
            })
        }
        CreatorJob::ProcessContent {
            creator_id,
            content_id,
            source,
        } => {
            require_ids([creator_id, content_id])?;
            if !source.starts_with("https://") {
                return Err(JobError::InvalidContentSource);
            }
            Ok(JobOutcome {
                event: "content_processed",
                subject_id: content_id.clone(),
            })
        }
    }
}

fn require_ids<const N: usize>(ids: [&String; N]) -> Result<(), JobError> {
    if ids.iter().any(|id| id.trim().is_empty()) {
        Err(JobError::MissingIdentifier)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_delivery_without_asset_before_it_can_be_acknowledged() {
        let job = CreatorJob::DeliverDigitalAsset {
            order_id: "order-1042".into(),
            asset_id: "".into(),
            buyer_id: "buyer-7".into(),
        };

        assert_eq!(process(&job).await, Err(JobError::MissingIdentifier));
    }
}

