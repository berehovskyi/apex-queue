# Framework Roadmap

Future feature notes for BullMQ alignment and framework maturity. This file is
not an invariant; it records likely sequencing and features to avoid unless the
trade-off changes.

## Already Added

- Backoff jitter. Retry eligibility is randomized by updating `AvailableAt__c`;
  execution pickup remains transport-dependent.

- Terminal cleanup retention. Queue-level `TerminalCleanupPolicy__c` defaults to
  `KEEP`; `DELETE` removes eligible terminal `Job__c` records from scheduled or
  manual maintenance after per-state retention windows. The v1 retention clock
  is conservative: terminal `State__c` plus `LastModifiedDate`. Blank retention
  values omit that state, `0` means eligible on the next cleanup cycle, and
  positive values are measured as `N * 24 * 60 * 60` seconds. `JobRun__c`
  records cascade through master-detail, while surviving `QueueError__c`
  records keep operational history and lose their job lookup through SetNull.

- Queryable job dimensions. `JobOptions.category(...)`, `groupKey(...)`, and
  `correlationId(...)` populate indexed non-unique `Job__c` text fields for
  operational SOQL lookups. These fields are for live/hot lookup, not permanent
  analytics when terminal cleanup deletes history.

- Queue events. `PublishedEventTypes__c` opts a queue into `QueueEvent__e`
  lifecycle signals: active claim/handoff, completed, failed, canceled,
  stalled/recovered, scheduler materialized, and cleanup events. Events are
  published after commit and are best-effort integration signals, not durable
  history. Do not add Slack-specific hooks or callouts to maintenance;
  integrations should subscribe to events and own delivery, formatting,
  credentials, retries, and rate limits outside the core framework.

## Worth Adding Soon

- Better enqueue dedupe modes. Scope these to job enqueue semantics, not
  dispatch semantics:
  simple dedupe by id, replace delayed job with the same dedupe key, and
  throttle-style TTL dedupe.

## Later Milestones

- Rate limiter. Valuable but large. Start with fixed-window, per-queue global
  limiting backed by durable locked state. Grouped or sliding-window limiting is
  not v1.

- Inspection APIs and dashboard helpers:
  `getFailedJobs`, `getActiveJobs`, `getDelayedJobs`, `getJobRuns`, and related
  admin views. Existing stats are useful, but richer inspection would make the
  framework feel mature.

- Named job logs. Avoid a new `JobLog__c` until there is demand. Consider a
  lightweight `ctx.log(...)` later if processor debugging becomes painful.

- Terminal archival. `ARCHIVE` is intentionally deferred. If it becomes
  necessary, treat it as a separate disposition from `DELETE`: atomically create
  archive records and delete the hot `Job__c` / `JobRun__c` records in bounded
  maintenance chunks. Big Objects are schema-heavy and query-limited, so do not
  add them until real retention/search requirements justify the cost.

## Probably Skip

- LIFO. It conflicts with predictable priority and availability ordering.

- Custom repeat strategy. Native cron pattern plus every-N-minute scheduler
  variants are a better Salesforce fit than user-defined cron/calendar
  calculation.

- Grouped limiter. This is BullMQ Pro territory and becomes lock-heavy in Apex.
  Revisit only after a simple queue-level limiter proves necessary and stable.

## Recommended Sequence

1. Inspection APIs.
2. Rate limiter.
3. Better enqueue dedupe modes.
