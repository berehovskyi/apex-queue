---
name: apex-queue
description: Architecture, invariants, current behavior, operational constraints, validation entrypoints, and roadmap context for the Apex Queue framework. Use when changing or reviewing queue runtime behavior, durable job state, worker, queueable, invocable, scheduler, maintenance, dependency graph, lifecycle event, locking, security, or async handoff code in this repository.
---

# Apex Queue

Use the repository source and tests as the current authority. Treat measured
platform behavior in the references as evidence tied to this project and
reverify it when a change depends on org-specific behavior.

## Required reading

- Read [references/invariants.md](references/invariants.md) before changing
  runtime ownership, durable state, locking, async handoffs, recovery,
  scheduling, dependency resolution, or lifecycle telemetry. If a proposed
  change conflicts with an invariant, stop and make that decision explicit.
- Read [references/context.md](references/context.md) when work requires the
  package map, current execution-lane behavior, operational rules, test
  entrypoints, or recent validation context.
- Read [references/roadmap.md](references/roadmap.md) when proposing features,
  choosing scope, or deciding whether an idea is planned, deferred, or
  intentionally skipped.

## Working rules

- Verify behavior in current production code and focused tests before accepting
  a review finding or changing implementation.
- Preserve `Job__c` and the documented durable framework records as sources of
  truth; treat native async artifacts as wake or handoff mechanisms.
- Validate changes with the narrowest relevant tests first, then broaden in
  proportion to risk.
