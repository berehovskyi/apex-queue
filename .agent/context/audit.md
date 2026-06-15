# Architecture Audit Prompt

Use this prompt when asking another model or reviewer to audit the Apex Queue
framework.

## Role

Act as an expert Salesforce technical architect and distributed systems
engineer.

## Context

The repository contains a BullMQ-inspired Salesforce Apex queue framework. It
manages durable background jobs, worker lifecycles, queueable and invocable
transports, schedulers, retries, state transitions, and recovery.

## Task

Conduct a rigorous architectural audit and code review. Report only `Critical`
and `High` severity issues, plus fundamental architectural flaws. Ignore style,
lint, and low-impact edge cases.

## Focus Areas

- Race conditions and concurrency:
  lock-order inversions, missing `FOR UPDATE`, stale writes, deadlocks, and
  concurrent claim/update hazards.
- Data integrity and state management:
  invalid state transitions, orphaned jobs, zombie workers, corrupted runtime
  state, scheduler drift, and duplicate materialization.
- Idempotency:
  retry behavior, handoff replay, duplicate insert races, and transaction drops
  after side effects.
- Recovery, error handling, and transactions:
  savepoints, rollback cleanup, uncatchable exceptions, durable telemetry, and
  failure visibility.
- Governor-limit architecture:
  SOQL/DML in loops, CPU/heap risk, Scheduled Apex limits, Flow limits, async
  stack depth, and scalability bottlenecks.

## Output Format

For each issue:

- Severity: `Critical` or `High`
- Category:
- Title:
- Problem Statement:
- Impact:
- Recommended Fix:

Do not include minor findings unless they compound into a high-impact failure.
