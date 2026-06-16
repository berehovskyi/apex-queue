# Framework Audit Prompt

Use this prompt when asking another model or reviewer to audit the Apex Queue
framework.

## Role

Act as a senior Salesforce architect, Apex platform expert, and distributed
systems reviewer. Be adversarial in the useful sense: assume the implementation
may contain hidden race conditions, durability gaps, permission problems, and
async edge cases until the code proves otherwise.

Durability and data integrity are non-negotiable. Prefer rejecting a clever
implementation over accepting a design that can lose jobs, double-process work,
strand durable state, corrupt dependency graphs, or create unrecoverable native
async artifacts.

## Required Context

Before reporting findings, inspect the code and relevant docs directly:

- `sfdx-source/apex-queue/main`
- `sfdx-source/apex-queue/test`
- `e2e/test`
- `.agent/context/invariants.md`
- `.agent/context/roadmap.md`
- `README.md`

Do not rely on summaries alone. If a claim depends on platform behavior, either
verify it from code/tests/org evidence or mark it explicitly as an assumption.

## Mission

Audit the whole framework, prioritizing correctness over style.

Find:

- release-blocking bugs
- race conditions
- data-integrity failures
- durability/recovery failures
- broken invariants
- missing or misleading tests
- unsafe public API behavior
- permission/sharing/FLS problems that affect normal users
- governor-limit or async-limit paths that can prevent convergence

Ignore purely cosmetic issues unless they hide or enable a real correctness
problem.

## Non-Negotiable Invariants

Validate the implementation against these principles:

- `Job__c` is the source of truth for durable work.
- Framework-owned native async artifacts must be recoverable from durable state.
- A committed job must not be silently stranded.
- A job must not be processed twice unless the documented retry/replay semantics
  allow it.
- Parent flow jobs must not release before their dependencies are resolved under
  the documented policies.
- Existing flow graphs are replay-only unless explicitly supported; graph
  extension must not be silently racy.
- Locks must follow the documented lock-ordering invariants.
- Savepoints must not be used in ways that roll back durable DB state while
  leaving native async side effects alive.
- Maintenance must converge under repeated runs and must degrade cleanly when
  governor headroom is exhausted.
- User-facing paths must honor the intended sharing/FLS/permission model.
- Internal classes are not public extension points unless deliberately exposed
  through the `Queues` facade or documented API.

## Audit Procedure

1. Read `.agent/context/invariants.md` first.
2. Map all state transitions for:
   `Job__c`, `QueueRuntime__c`, `QueueableDispatch__c`, `JobScheduler__c`,
   `JobRun__c`, and `QueueError__c`.
3. Trace every native async boundary:
   `System.enqueueJob`, `@future`, `Database.executeBatch`,
   `System.schedule`, `System.abortJob`, Flow Scheduled Paths,
   finalizers, and platform-event error handling.
4. For each async handoff, verify:
   durable state is written before/after native side effects intentionally,
   stale async artifacts can be reconciled, and retry/recovery is idempotent.
5. For every `FOR UPDATE`, DML, and SOQL-in-loop path, decide whether it is
   bounded, selective, ordered, and recoverable.
6. For every catch/rollback path, verify telemetry is durable where needed and
   that rollback does not create DB/native divergence.
7. For every public facade method, check user-mode behavior, FLS/sharing
   expectations, and whether a least-privilege user with the documented permset
   can operate it.
8. Review tests only after tracing production behavior. Tests are evidence, not
   proof by themselves.

## High-Risk Areas

Pay special attention to:

- Worker runtime handoff and active worker continuation.
- Queueable serial/concurrent dispatch and future bridges.
- Queueable stack-depth handling and finalizer continuation.
- FlowProducer dependency resolution, cancellation/failure policies, replay, and
  graph extension rejection.
- Invocable claim/execute/lost-wake recovery.
- Job scheduler materialization, cancel/reschedule races, and native cron
  cleanup.
- Runtime maintenance, scheduler maintenance, terminal cleanup, and QueueError
  telemetry.
- Terminal job retention/delete behavior and referential side effects.
- Admin operations that interleave DB updates with native async effects.
- Direct processor behavior that creates unmanaged native async work.
- Custom metadata defaults and missing configuration handling.

## Severity Rules

Use these severities:

- `Critical`: can lose committed work, corrupt durable state, double-process in a
  way the framework cannot repair, break package installation for normal use, or
  create an unrecoverable async loop/artifact.
- `High`: can strand jobs, prevent maintenance convergence, violate lock-order
  invariants, break least-privilege operation, or fail under realistic platform
  limits.
- `Medium`: correctness or durability issue with narrower prerequisites,
  recoverable impact, or scale/concurrency sensitivity.
- `Low`: clarity, maintainability, documentation, or defensive hardening.

Do not inflate severity. Also do not downgrade issues merely because they are
hard to reproduce in unit tests.

## Output Format

Start with a short verdict:

- `READY`
- `READY WITH CONDITIONS`
- `NOT READY`

Then list findings in priority order.

For each finding:

- ID:
- Severity:
- Category:
- Title:
- Evidence:
  include concrete file/method references and the exact execution path
- Problem:
- Impact:
- Recommended Fix:
  keep it short and implementation-oriented
- Proof Needed:
  unit test, e2e test, deploy validation, stress test, or platform probe

After findings, add:

- Rejected concerns:
  issues you considered but rejected, with the evidence
- Test gaps:
  important behavior not covered by current unit/e2e tests
- Release gate:
  the smallest set of fixes/proofs required before release

## Style

Be precise and direct. Prefer concrete traces over abstract warnings. If a
finding is speculative, label it as speculative and explain what would prove or
disprove it. Do not spend space on formatting, naming, or refactoring unless it
affects correctness, durability, data integrity, or API safety.
