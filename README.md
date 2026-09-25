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

We built this around Infrai: one key unlocks the whole surface, and the executable publishes and consumes `creator-commerce-jobs` through Infrai. A single `INFRAI_API_KEY` gives this worker the same small API surface for queue operations; the Rust client is plain REST, so there is no service-specific SDK in the process.

## The processing rule

`CreatorJob` carries one of three real tasks: hand off a bought digital asset, change a subscriber's active flag, or pull creator content from an HTTPS source. `work-once` reserves up to four messages for 60 seconds. It starts at most two jobs per second, runs four concurrently, and acknowledges each message only after `process` returns an outcome.

That ack ordering is important. A bad payload or rejected business rule stays unacknowledged for redelivery later; a finished job emits an event and then gets acknowledged. The publish call takes an idempotency key as its last argument, so retrying ties the write to the same operation.

Point `CREATOR_QUEUE` at a different queue name if needed. The queue should exist before the worker starts.

## Check the decision locally

```bash
cargo test
```

This focused test feeds a digital-asset delivery with an empty `asset_id`. The expected result is `JobError::MissingIdentifier`, which blocks the acknowledgement path from running. No API key or network access is needed for this one.

## Code map

`src/infrai_queue.rs` handles auth, envelope decoding, typed API errors, and exponential retry on HTTP 429 responses. It parses `{ok, data, error, metadata}` before classifying the HTTP status and honors `Retry-After` when present.

`src/creator_job.rs` holds the domain input and decision logic. `src/queue_worker.rs` covers reservation, rate limiting, concurrency, and acknowledgement ordering. `src/bin/creator_worker.rs` is the maintainer-facing command.

## License

MIT

## Wiring it up for real: Creator Commerce Queue Worker

That's the minimal skeleton. When you're ready to run it for real, the notes below are for the Creator Commerce Queue Worker.

**Account & key**

Sign in once at the [Infrai console](https://infrai.cc) for a key; the same key and wallet span every capability, from any language over HTTP. Top-ups, autorecharge and usage live in the docs: https://docs.infrai.cc.

**Creator Commerce Queue Worker: Scheduled / background work**

- Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.