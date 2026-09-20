use crate::{
    creator_job::{self, CreatorJob, JobError},
    infrai_queue::{InfraiQueue, QueueError, QueueMessage},
};
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{sync::Semaphore, task::JoinSet, time::MissedTickBehavior};

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    Queue(#[from] QueueError),
    #[error("message {message_id} has an invalid payload: {source}")]
    Payload {
        message_id: String,
        source: serde_json::Error,
    },
    #[error("message {message_id} was not acknowledged: {source}")]
    Job {
        message_id: String,
        source: JobError,
    },
    #[error("worker task did not complete: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub async fn run_batch(
    queue: InfraiQueue,
    concurrency: usize,
    starts_per_second: u32,
) -> Result<usize, WorkerError> {
    let concurrency = concurrency.max(1);
    let messages = queue.consume(concurrency, 60).await?;
    let processed = messages.len();
    let permits = Arc::new(Semaphore::new(concurrency));
    let period = Duration::from_secs_f64(1.0 / starts_per_second.max(1) as f64);
    let mut starts = tokio::time::interval(period);
    starts.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut tasks = JoinSet::new();

    for message in messages {
        starts.tick().await;
        let permit = permits.clone().acquire_owned().await.expect("semaphore open");
        let queue = queue.clone();
        tasks.spawn(async move {
            let _permit = permit;
            handle_message(queue, message).await
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(processed)
}

async fn handle_message(queue: InfraiQueue, message: QueueMessage) -> Result<(), WorkerError> {
    let job: CreatorJob = serde_json::from_value(message.payload).map_err(|source| {
        WorkerError::Payload {
            message_id: message.message_id.clone(),
            source,
        }
    })?;
    let outcome = creator_job::process(&job)
        .await
        .map_err(|source| WorkerError::Job {
            message_id: message.message_id.clone(),
            source,
        })?;
    queue.ack(&message.message_id).await?;
    println!("{} subject={}", outcome.event, outcome.subject_id);
    Ok(())
}

