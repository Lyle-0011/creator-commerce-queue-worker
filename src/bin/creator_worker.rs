use creator_commerce_queue_worker::{
    creator_job::CreatorJob, infrai_queue::InfraiQueue, queue_worker,
};
use std::{env, error::Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let queue_name = env::var("CREATOR_QUEUE").unwrap_or_else(|_| "creator-commerce-jobs".into());
    let queue = InfraiQueue::from_env(queue_name)?;
    let mut args = env::args().skip(1);

    match args.next().as_deref() {
        Some("publish") => {
            let job = CreatorJob::DeliverDigitalAsset {
                order_id: required(&mut args, "order id")?,
                asset_id: required(&mut args, "asset id")?,
                buyer_id: required(&mut args, "buyer id")?,
            };
            let key = required(&mut args, "idempotency key")?;
            queue.publish(&job, &key).await?;
            println!("digital asset delivery queued");
        }
        Some("work-once") => {
            let count = queue_worker::run_batch(queue, 4, 2).await?;
            println!("batch complete: {count} message(s)");
        }
        _ => return Err("use: creator_worker publish <order> <asset> <buyer> <key> | work-once".into()),
    }
    Ok(())
}

fn required(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, Box<dyn Error>> {
    args.next().ok_or_else(|| format!("missing {name}").into())
}

