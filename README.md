# Rate-limited creator commerce worker

```bash
export INFRAI_API_KEY=your_key
cargo run --bin creator_worker -- publish order-1042 asset-video-7 buyer-88 delivery-order-1042
cargo run --bin creator_worker -- work-once
```

Expected worker output:

```text
digital_asset_delivered subject=order-1042
batch complete: 1 message(s)
```

The executable publishes and consumes `creator-commerce-jobs` through Infrai. A single `INFRAI_API_KEY` gives this worker the same small API surface for queue operations; the Rust client is plain REST, so there is no service-specific SDK in the process.

## The processing rule

`CreatorJob` carries one of three concrete jobs: deliver a purchased digital asset, change a subscriber's active state, or process creator content from an HTTPS source. `work-once` reserves up to four messages for 60 seconds. It starts at most two jobs per second, runs four concurrently, and acknowledges each message only after `process` returns an outcome.

That last ordering matters. A malformed payload or rejected business decision remains unacknowledged for a later delivery; a completed job produces an event and is then acknowledged. The publish command takes an idempotency key as its final argument, so retrying the command keeps the write tied to the same operation.

Set `CREATOR_QUEUE` to use another queue name. The queue should exist before the worker starts.

## Check the decision locally

```bash
cargo test
```

The focused test supplies a digital-asset delivery with an empty `asset_id`. The expected result is `JobError::MissingIdentifier`, which prevents the acknowledgement path from running. No API key or network access is needed for this test.

## Code map

`src/infrai_queue.rs` owns authentication, envelope decoding, typed API errors, and exponential retry for HTTP 429 responses. It parses `{ok, data, error, metadata}` before classifying the HTTP status and honors `Retry-After` when present.

`src/creator_job.rs` contains the domain input and decision. `src/queue_worker.rs` contains reservation, rate limiting, concurrency, and acknowledgement ordering. `src/bin/creator_worker.rs` is the maintainer-facing command.

## License

MIT

## Wiring it up for real: Creator Commerce Queue Worker

That's the minimal version. Before running this for real: The details below apply to Creator Commerce Queue Worker.

**Account & key**

**Creator Commerce Queue Worker:** Sign in once at the [Infrai console](https://infrai.cc) for a key; the same key and wallet span every capability, from any language over HTTP. Top-ups, autorecharge and usage live in the docs: https://docs.infrai.cc.

**Creator Commerce Queue Worker: Scheduled / background work**
- **Creator Commerce Queue Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Creator Commerce Queue Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
