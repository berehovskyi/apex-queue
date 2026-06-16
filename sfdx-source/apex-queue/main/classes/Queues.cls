/**
 * Durable, BullMQ-inspired background job framework for Salesforce Apex. `Queues` is the single
 * supported public facade: it creates queue handles, job handles, and dependency-graph producers,
 * and exposes org-level maintenance. All committed work is stored durably in `Job__c`, the
 * framework's source of truth; native async artifacts (Batch, Queueable, Scheduled Apex, Flow)
 * are treated as wake or handoff hints, never as the source of truth.
 *
 * Application code should depend only on this class and its nested value objects. Classes under
 * `classes/internal` are framework internals and may change between releases.
 *
 * @see Queues.Queue
 * @see Queues.Job
 * @see Queues.FlowProducer
 * @see Queues.JobProcessor
 * @example
 * // Enqueue a job, then inspect and operate on it later
 * Queues.Job job = Queues.of('invoice-sync').add(
 *     'sync-invoice',
 *     'InvoiceSyncProcessor',
 *     new Map<String, Object>{ 'invoiceId' => 'INV-001' }
 * );
 * Id jobId = job.getId();
 * Queues.State state = Queues.job(jobId).getState();
 */
public inherited sharing class Queues {
    @TestVisible
    private static QueuesService queueService = new QueuesService();

    // <editor-fold desc="Factories">
    /**
     * Creates a lightweight handle to a named queue. The queue must already exist as an active
     * `QueueDefinition__mdt` record before jobs can be added.
     *
     * @param queueName public queue name declared in QueueDefinition__mdt
     * @return queue handle bound to queueName
     * @throws QueueException when queueName is blank
     * @example
     * Queues.Queue queue = Queues.of('invoice-sync');
     */
    public static Queue of(final String queueName) {
        return new Queue(queueName);
    }

    /**
     * Creates a lightweight handle to an existing durable job by its Salesforce record id. The
     * handle is inert until a method is called; it does not verify that the job exists up front.
     *
     * @param jobId Salesforce Job__c.Id of an existing job
     * @return job handle bound to jobId
     * @throws QueueException when jobId is null
     * @example
     * Queues.Job job = Queues.job(existingJobId);
     */
    public static Job job(final Id jobId) {
        return new Job(jobId);
    }

    /**
     * Creates a `FlowProducer` used to insert atomic dependency graphs whose parents wait for
     * their children to resolve.
     *
     * @return a new FlowProducer
     * @example
     * Queues.JobNode root = Queues.flowProducer().add(
     *     new Queues.FlowJob('finish-import', 'invoice-sync', 'FinishImportProcessor')
     *         .child(new Queues.FlowJob('load-lines', 'invoice-sync', 'LoadLinesProcessor'))
     * );
     */
    public static FlowProducer flowProducer() {
        return new FlowProducer();
    }
    // </editor-fold>

    // <editor-fold desc="Maintenance">
    /**
     * Runs one bounded runtime-recovery sweep across queues in the current transaction: stalled
     * jobs, expired leases, stale queueable dispatches, dropped async handoffs, stale dependency
     * parents, and orphaned worker artifacts. Uses framework-owned system-mode persistence, so
     * restrict access to callers trusted to operate every configured queue.
     *
     * A `done` value of `false` means the sweep stopped early to preserve governor headroom. Do not
     * re-invoke it in the same transaction: governor limits are per-transaction and looping cannot
     * replenish them. Let the next scheduled maintenance run, or call it again from a fresh
     * transaction, to continue.
     *
     * @return result whose done is false when the sweep stopped early and must be continued in
     * a later transaction
     */
    public static MaintenanceResult runRuntimeMaintenance() {
        return queueService.runRuntimeMaintenance();
    }

    /**
     * Runs one bounded job-scheduler repair sweep in the current transaction: it reconciles durable
     * `JobScheduler__c` state with native Scheduled Apex artifacts. Repair-only; it never
     * materializes occurrences itself. The scheduled maintenance job instead runs this through a
     * queueable handoff so scheduler repair gets an independent governor budget; calling this method
     * directly does not.
     *
     * @return result whose done is false when the sweep stopped early and must be continued in
     * a later transaction
     */
    public static MaintenanceResult runJobSchedulerMaintenance() {
        return queueService.runJobSchedulerMaintenance();
    }

    /**
     * Runs one bounded terminal-cleanup sweep in the current transaction. Queues whose
     * `QueueDefinition__mdt.TerminalCleanupPolicy__c` is `DELETE` remove eligible terminal
     * `Job__c` rows according to their per-state retention fields. `JobRun__c` rows are removed
     * by master-detail cascade.
     *
     * @return result whose done is false when cleanup stopped early and must be continued in
     * a later transaction
     */
    public static MaintenanceResult runTerminalCleanupMaintenance() {
        return queueService.runTerminalCleanupMaintenance();
    }

    /**
     * Schedules the recurring maintenance job at the default hourly cadence (`0 0 * * * ?`).
     * Idempotent: returns the existing scheduled job id when maintenance is already scheduled.
     *
     * @return CronTrigger id of the scheduled (or already-scheduled) maintenance job
     * @example
     * Queues.scheduleMaintenance();
     */
    public static Id scheduleMaintenance() {
        return queueService.scheduleMaintenance();
    }

    /**
     * Schedules the recurring maintenance job with a custom cron expression. Idempotent: returns
     * the existing scheduled job id when maintenance is already scheduled, without re-applying the
     * expression. As an operational recommendation, prefer hourly-or-slower cadences; sub-hourly
     * expressions are not enforced by the framework.
     *
     * @param cronExpression Salesforce cron expression for the maintenance cadence
     * @return CronTrigger id of the scheduled (or already-scheduled) maintenance job
     * @throws QueueException when cronExpression is blank
     * @example
     * Queues.scheduleMaintenance('0 0 0,6,12,18 * * ?');
     */
    public static Id scheduleMaintenance(final String cronExpression) {
        return queueService.scheduleMaintenance(cronExpression);
    }

    /**
     * Aborts the scheduled maintenance job if present.
     *
     * @return number of maintenance schedules aborted (0 when none was scheduled)
     */
    public static Integer unscheduleMaintenance() {
        return queueService.unscheduleMaintenance();
    }

    /**
     * Reports whether the recurring maintenance job is currently scheduled.
     *
     * @return true when a maintenance schedule exists
     */
    public static Boolean isMaintenanceScheduled() {
        return queueService.isMaintenanceScheduled();
    }
    // </editor-fold>

    /**
     * Durable lifecycle state of a job, stored on `Job__c.State__c`. `COMPLETED`, `FAILED`, and
     * `CANCELED` are terminal; `WAITING`, `DELAYED`, and `PAUSED` are runnable or holdable;
     * `WAITING_CHILDREN` is a `FlowProducer` parent awaiting its children.
     */
    public enum State {
        /** Due or soon-due, eligible to be claimed. */
        WAITING,
        /** Not yet available; waits for its available-at time. */
        DELAYED,
        /** Claimed and leased to an executor. */
        ACTIVE,
        /** Processor returned a completed result. */
        COMPLETED,
        /** Attempts exhausted or unrecoverable failure. */
        FAILED,
        /** Held until resumed. */
        PAUSED,
        /** Flow parent awaiting child resolution. */
        WAITING_CHILDREN,
        /** Future work abandoned, preserved for observability. */
        CANCELED
    }

    /**
     * Retry backoff strategy. Delay values are expressed in minutes. Queueable retry delay is
     * capped at 10 minutes by the native queueable delay limit; `WORKER` retry delay is uncapped.
     */
    public enum Backoff {
        /** Retry immediately when attempts remain. */
        NONE,
        /** Same delay between every retry. */
        FIXED,
        /** Delay grows from the base on each retry. */
        EXPONENTIAL
    }

    /**
     * Transport lane that runs the processor. Selected per bulk request or flow graph. Lanes are
     * isolated and must not share durable runtime ownership.
     */
    public enum ExecutionMode {
        /** Default Batch Apex worker lane. */
        WORKER,
        /** Ordered queueable dispatcher submission. */
        QUEUEABLE_SERIAL,
        /** Aggressive concurrent queueable submission. */
        QUEUEABLE_CONCURRENT,
        /** Flow Scheduled Path lane. */
        INVOCABLE
    }

    /**
     * Parent behavior when a `FlowProducer` child reaches terminal `FAILED`. Evaluated only after
     * the child is terminal, never per attempt. Default (unset) keeps the parent in
     * `WAITING_CHILDREN`.
     */
    public enum DependencyFailurePolicy {
        /** Fail the parent when this child fails. */
        FAIL_PARENT,
        /** Release the parent even if siblings are unresolved. */
        CONTINUE_PARENT,
        /** Treat only this child as resolved. */
        IGNORE_DEPENDENCY,
        /** Resolve this child and remove the parent link. */
        REMOVE_DEPENDENCY
    }

    /**
     * Parent behavior when a `FlowProducer` child reaches terminal `CANCELED`. Evaluated only
     * after the child is terminal. Default (unset) keeps the parent in `WAITING_CHILDREN`.
     */
    public enum DependencyCanceledPolicy {
        /** Cancel the parent when this child is canceled. */
        CANCEL_PARENT,
        /** Fail the parent when this child is canceled. */
        FAIL_PARENT,
        /** Treat only this child as resolved. */
        IGNORE_DEPENDENCY
    }

    /**
     * State of a `QueueableDispatch__c` handoff envelope for queueable-lane jobs.
     */
    public enum QueueableDispatchState {
        /** Created, dispatcher not yet submitted. */
        WAITING,
        /** Dispatcher running, submitting executors. */
        ACTIVE,
        /** Every requested job dispatched. */
        COMPLETED,
        /** Dispatch failed catastrophically. */
        FAILED
    }

    /**
     * Recorded status of a single execution attempt on `JobRun__c.Status__c`.
     */
    public enum RunStatus {
        /** Attempt in progress. */
        ACTIVE,
        /** Attempt completed successfully. */
        COMPLETED,
        /** Attempt failed. */
        FAILED,
        /** Processor paused with a checkpoint. */
        PAUSED,
        /** Processor deferred with a checkpoint and delay. */
        DEFERRED
    }

    /**
     * Outcome a processor returns from `process(ctx)`, produced through the `JobContext` result
     * factories (`complete`, `fail`, `pause`, `defer`).
     */
    public enum Outcome {
        /** Finished successfully. */
        COMPLETED,
        /** Retryable failure. */
        FAILED,
        /** Pause with checkpoint until resumed. */
        PAUSED,
        /** Resume after a delay with checkpoint. */
        DEFERRED
    }

    /**
     * Outcome of a maintenance sweep. `total` is the count of items processed,
     * `done` reports whether the sweep finished or stopped early to preserve governor headroom,
     * and `countsByQueueName` breaks the total down per queue.
     *
     * @example
     * Queues.MaintenanceResult result = Queues.runRuntimeMaintenance();
     * Integer invoiceSync = result.getCount('invoice-sync');
     * Boolean finished = result.done;
     */
    public class MaintenanceResult {
        /** Total number of items processed across all queues. */
        public Integer total { get; private set; }
        /** `false` when the sweep stopped early for governor headroom and should be re-run. */
        public Boolean done { get; private set; }
        /** Per-queue breakdown of processed items, keyed by queue name. */
        public Map<String, Integer> countsByQueueName { get; private set; }

        /** Creates an empty result with `total` `0` and `done` `true`. */
        public MaintenanceResult() {
            total = 0;
            done = true;
            countsByQueueName = new Map<String, Integer>();
        }

        /**
         * Adds processed items for a queue, updating `total` and the per-queue count.
         *
         * @param queueName queue the items belong to; ignored when blank
         * @param count number of items to add; treated as 0 when null
         * @return this result for fluent chaining
         */
        public MaintenanceResult add(final String queueName, final Integer count) {
            Integer safeCount = count ?? 0;
            total += safeCount;
            if (String.isNotBlank(queueName)) {
                countsByQueueName.put(queueName, getCount(queueName) + safeCount);
            }
            return this;
        }

        /**
         * Marks the sweep as stopped early so callers know to run maintenance again.
         *
         * @return this result for fluent chaining
         */
        public MaintenanceResult markIncomplete() {
            done = false;
            return this;
        }

        /**
         * Returns the number of items processed for a single queue.
         *
         * @param queueName queue name to look up
         * @return processed count for queueName, or 0 when none
         */
        public Integer getCount(final String queueName) {
            return countsByQueueName.get(queueName) ?? 0;
        }
    }

    /**
     * Read-only status of a queue's current framework-owned Batch Apex worker, as seen in the
     * Salesforce Flex Queue. Returned by `Queue.getWorkerStatus()` and used to decide whether the
     * worker can be reordered.
     *
     * @see Queues.Queue
     */
    public class WorkerStatus {
        private final String queueName;
        private final Id jobId;
        private final String status;

        /**
         * @param queueName queue this worker serves
         * @param jobId AsyncApexJob id of the current worker, or null when there is none
         * @param status native AsyncApexJob.Status of the worker, or null when there is none
         */
        public WorkerStatus(final String queueName, final Id jobId, final String status) {
            this.queueName = queueName;
            this.jobId = jobId;
            this.status = status;
        }

        /**
         * @return queue this worker serves
         */
        public String getQueueName() {
            return queueName;
        }

        /**
         * @return AsyncApexJob id of the current framework worker, or null when none exists
         */
        public Id getJobId() {
            return jobId;
        }

        /**
         * @return raw native AsyncApexJob.Status, or null when no worker exists
         */
        public String getStatus() {
            return status;
        }

        /**
         * @return true when a framework-owned worker currently exists for the queue
         */
        public Boolean hasWorker() {
            return jobId != null;
        }

        /**
         * Reports whether the worker is waiting in the Flex Queue and therefore reorderable.
         *
         * @return true when the worker status is Holding
         */
        public Boolean isHolding() {
            return QueuesSupport.normalizeEnumValue(status) == 'HOLDING';
        }
    }

    /**
     * Contract a user class implements to run job business logic. The framework calls `process`
     * once per transaction for a single durable job attempt. The implementation must have a public
     * no-argument constructor and be allowlisted by an active `Processor__mdt` record. Side effects
     * must be externally idempotent, because a catastrophic transaction failure can replay an
     * attempt after an external write already succeeded; use `JobContext.idempotencyKey()` as the
     * stable logical-job key.
     *
     * @example
     * public class InvoiceSyncProcessor implements Queues.JobProcessor {
     *     public Queues.ProcessResult process(final Queues.JobContext ctx) {
     *         Map<String, Object> data = (Map<String, Object>) ctx.data();
     *         // ... perform work, callouts, etc.
     *         return ctx.complete(new Map<String, Object>{ 'status' => 'synced' });
     *     }
     * }
     */
    public interface JobProcessor {
        /**
         * Runs the job's business logic for a single attempt.
         *
         * @param ctx durable context for the attempt: data, checkpoint, progress, attempts, and
         * result factories
         * @return one of the ctx result factories: complete, fail, pause, or defer
         * @throws Queues.UnrecoverableJobException to fail the job immediately without further
         * retries
         */
        ProcessResult process(JobContext ctx);
    }

    /**
     * Handle to a named queue. Adds jobs, schedules recurring work, and performs queue-level
     * administration (pause, resume, wake, drain, cancel, stalled recovery, worker reordering,
     * stats). Obtain one with `Queues.of(queueName)`. The backing `QueueDefinition__mdt` supplies
     * default attempts, backoff, lease, and claim policy that individual jobs may override.
     *
     * @see Queues
     * @example
     * Queues.Queue queue = Queues.of('invoice-sync');
     * queue.add('sync-invoice', 'InvoiceSyncProcessor', new Map<String, Object>{ 'id' => 'INV-001' });
     * Integer waiting = queue.getWaitingCount();
     */
    public class Queue {
        private final String queueName;

        /**
         * @param queueName public queue name declared in QueueDefinition__mdt
         * @throws QueueException when queueName is blank
         */
        public Queue(final String queueName) {
            QueuesSupport.notBlank(queueName, 'queueName');
            this.queueName = queueName;
        }

        /**
         * @return this queue's name
         */
        public String getName() {
            return queueName;
        }

        /**
         * Adds a single job with default options on the default `WORKER` lane.
         *
         * @param name display name of the job
         * @param processor Processor__mdt.ProcessorClass__c allowlisted to run the job
         * @param data JSON-serializable payload exposed to the processor through ctx.data()
         * @return handle to the created (or idempotently reused) job
         * @throws ConfigurationException when the queue is missing or inactive, or the processor is
         * not an active `Processor__mdt` mapping
         * @example
         * Queues.Job job = Queues.of('invoice-sync').add(
         *     'sync-invoice',
         *     'InvoiceSyncProcessor',
         *     new Map<String, Object>{ 'invoiceId' => 'INV-001' }
         * );
         */
        public Job add(final String name, final String processor, final Object data) {
            return add(name, processor, data, new JobOptions());
        }

        /**
         * Adds a single job with per-job options on the default `WORKER` lane.
         *
         * @param name display name of the job
         * @param processor allowlisted processor class name
         * @param data JSON-serializable payload
         * @param opts per-job options such as jobId, attempts, priority, delay, backoff
         * @return handle to the created (or idempotently reused) job
         * @throws ConfigurationException when the queue or processor is not configured or inactive
         * @example
         * Queues.of('invoice-sync').add(
         *     'sync-invoice', 'InvoiceSyncProcessor', data,
         *     new Queues.JobOptions().jobId('invoice:INV-001').attempts(5)
         * );
         */
        public Job add(final String name, final String processor, final Object data, final JobOptions opts) {
            return add(name, processor, data, opts, new BulkJobOptions());
        }

        /**
         * Adds a single job with bulk options that select the execution lane.
         *
         * @param name display name of the job
         * @param processor allowlisted processor class name
         * @param data JSON-serializable payload
         * @param opts bulk options selecting the execution mode and optional dispatcher dedupe key
         * @return handle to the created (or idempotently reused) job
         * @throws ConfigurationException when the queue or processor is not configured or inactive
         */
        public Job add(final String name, final String processor, final Object data, final BulkJobOptions opts) {
            return add(name, processor, data, new JobOptions(), opts);
        }

        /**
         * Adds a single job with both per-job and bulk options.
         *
         * @param name display name of the job
         * @param processor allowlisted processor class name
         * @param data JSON-serializable payload
         * @param jobOpts per-job options
         * @param bulkOpts bulk options selecting the execution mode
         * @return handle to the created (or idempotently reused) job
         * @throws ConfigurationException when the queue or processor is not configured or inactive
         */
        public Job add(
            final String name,
            final String processor,
            final Object data,
            final JobOptions jobOpts,
            final BulkJobOptions bulkOpts
        ) {
            return addBulk(new List<JobData>{ new JobData(name, processor, data, jobOpts) }, bulkOpts)[0];
        }

        /**
         * Atomically adds several jobs on the default `WORKER` lane. If any job cannot be inserted,
         * the whole request rolls back. Salesforce governor limits still bound a single request.
         *
         * @param jobs jobs to enqueue
         * @return handles to the created (or idempotently reused) jobs, in request order
         * @throws ConfigurationException when the queue or any processor is not configured or
         * inactive
         */
        public List<Job> addBulk(final List<JobData> jobs) {
            return addBulk(jobs, new BulkJobOptions());
        }

        /**
         * Atomically adds several jobs with bulk options selecting the execution lane. The request
         * is execution-mode homogeneous: a reused job id resolving to a different mode is rejected.
         * If any job cannot be inserted, the whole request rolls back.
         *
         * @param jobs jobs to enqueue
         * @param opts bulk options selecting the execution mode and optional dispatcher dedupe key
         * @return handles to the created (or idempotently reused) jobs, in request order
         * @throws QueueException when jobs or opts is null, or a duplicate job id maps to a
         * different execution mode
         * @throws ConfigurationException when the queue or any processor is not configured or
         * inactive
         * @example
         * List<Queues.Job> jobs = Queues.of('invoice-sync').addBulk(
         *     new List<Queues.JobData>{
         *         new Queues.JobData('sync', 'InvoiceSyncProcessor', new Map<String, Object>{ 'id' => 'A' }),
         *         new Queues.JobData('sync', 'InvoiceSyncProcessor', new Map<String, Object>{ 'id' => 'B' })
         *     },
         *     new Queues.BulkJobOptions().queueable()
         * );
         */
        public List<Job> addBulk(final List<JobData> jobs, final BulkJobOptions opts) {
            QueuesSupport.notNull(jobs, 'jobs');
            QueuesSupport.notNull(opts, 'opts');
            List<Job> enqueuedJobs = new List<Job>();
            for (Job__c job : queueService.enqueue(queueName, jobs, opts)) {
                enqueuedJobs.add(new Job(job.Id));
            }
            return enqueuedJobs;
        }

        /**
         * Pauses the queue. New work can still be added but no worker is scheduled and any pending
         * worker wake is canceled until the queue is resumed.
         *
         * @return this queue for fluent chaining
         * @throws ConfigurationException when the queue is not configured
         */
        public Queue pause() {
            queueService.pause(queueName);
            return this;
        }

        /**
         * Resumes a paused queue and schedules a worker when due work exists.
         *
         * @return this queue for fluent chaining
         * @throws ConfigurationException when the queue is not configured or is inactive
         */
        public Queue resume() {
            queueService.resume(queueName);
            return this;
        }

        /**
         * Wakes the queue now, scheduling a worker for any currently due work.
         *
         * @return 1 when a worker was scheduled, otherwise 0
         * @throws ConfigurationException when the queue is not configured or is inactive
         */
        public Integer wake() {
            return queueService.wake(queueName);
        }

        /**
         * Recovers stalled jobs for this queue whose active lease has expired, retrying them or
         * failing them when attempts are exhausted. Bounded-progress; not a substitute for
         * org-level scheduled maintenance.
         *
         * @return number of stalled jobs recovered
         * @throws ConfigurationException when the queue is not configured
         */
        public Integer recoverStalled() {
            return queueService.recoverStalled(queueName);
        }

        /**
         * Marks drainable jobs (`WAITING`, `DELAYED`, `PAUSED`, `WAITING_CHILDREN`) as `CANCELED`,
         * preserving the records for observability. Bounded-progress; run again while it keeps
         * returning a full batch.
         *
         * @return number of jobs canceled in this call
         * @throws ConfigurationException when the queue is not configured
         */
        public Integer cancel() {
            return queueService.cancel(queueName);
        }

        /**
         * Deletes drainable jobs and cascades their `JobRun__c` attempt history. Destructive and
         * not recoverable by later maintenance; use `cancel()` to preserve records. Bounded-progress.
         *
         * @return number of jobs deleted in this call
         * @throws ConfigurationException when the queue is not configured
         */
        public Integer drain() {
            return queueService.drain(queueName);
        }

        /**
         * Returns the status of this queue's current framework-owned Batch Apex worker. Read-only
         * and does not require the worker-management permission.
         *
         * @return worker status; hasWorker() is false when no worker exists
         * @throws ConfigurationException when the queue is not configured
         */
        public WorkerStatus getWorkerStatus() {
            return queueService.getWorkerStatus(queueName);
        }

        /**
         * Moves this queue's current worker to the front of the Flex Queue. Only valid while the
         * worker is `Holding`; the framework never promotes workers automatically.
         *
         * @return true only when Salesforce reordered the worker; false when no worker exists,
         * it is not holding, it already left the Flex Queue, or another transaction wins the race
         * @throws QueueException when the caller lacks the QueuesManageWorkers custom permission
         * @throws ConfigurationException when the queue is not configured
         */
        public Boolean moveWorkerToFront() {
            return queueService.moveWorkerToFront(queueName);
        }

        /**
         * Moves this queue's current worker to the end of the Flex Queue. Only valid while the
         * worker is `Holding`.
         *
         * @return true only when Salesforce reordered the worker; false otherwise
         * @throws QueueException when the caller lacks the QueuesManageWorkers custom permission
         * @throws ConfigurationException when the queue is not configured
         */
        public Boolean moveWorkerToEnd() {
            return queueService.moveWorkerToEnd(queueName);
        }

        /**
         * Returns exact aggregate statistics for the queue: pause state, active worker count, and
         * job counts by state.
         *
         * @return queue statistics snapshot
         * @throws ConfigurationException when the queue is not configured
         * @example
         * Queues.QueueStats stats = Queues.of('invoice-sync').getStats();
         * Integer failed = stats.failed;
         */
        public QueueStats getStats() {
            return queueService.getStats(queueName);
        }

        /**
         * Returns job counts keyed by every state.
         *
         * @return map of each State to its job count
         * @throws ConfigurationException when the queue is not configured
         */
        public Map<State, Integer> getJobCounts() {
            return getStats().asJobCounts();
        }

        /**
         * Returns job counts for the requested states only.
         *
         * @param states states to include; null or empty returns counts for all states
         * @return map of each requested State to its job count
         * @throws ConfigurationException when the queue is not configured
         */
        public Map<State, Integer> getJobCounts(final Set<State> states) {
            return getStats().asJobCounts(states);
        }

        /**
         * @return number of WAITING jobs in the queue
         * @throws ConfigurationException when the queue is not configured
         */
        public Integer getWaitingCount() {
            return getStats().waiting;
        }

        /**
         * @return number of DELAYED jobs in the queue
         * @throws ConfigurationException when the queue is not configured
         */
        public Integer getDelayedCount() {
            return getStats().delayed;
        }

        /**
         * @return number of ACTIVE jobs in the queue
         * @throws ConfigurationException when the queue is not configured
         */
        public Integer getActiveCount() {
            return getStats().active;
        }

        /**
         * @return number of COMPLETED jobs in the queue
         * @throws ConfigurationException when the queue is not configured
         */
        public Integer getCompletedCount() {
            return getStats().completed;
        }

        /**
         * @return number of FAILED jobs in the queue
         * @throws ConfigurationException when the queue is not configured
         */
        public Integer getFailedCount() {
            return getStats().failed;
        }

        /**
         * @return number of PAUSED jobs in the queue
         * @throws ConfigurationException when the queue is not configured
         */
        public Integer getPausedCount() {
            return getStats().paused;
        }

        /**
         * @return number of WAITING_CHILDREN jobs in the queue
         * @throws ConfigurationException when the queue is not configured
         */
        public Integer getWaitingChildrenCount() {
            return getStats().waitingChildren;
        }

        /**
         * @return number of CANCELED jobs in the queue
         * @throws ConfigurationException when the queue is not configured
         */
        public Integer getCanceledCount() {
            return getStats().canceled;
        }

        /**
         * @return total number of jobs in the queue across all states
         * @throws ConfigurationException when the queue is not configured
         */
        public Integer count() {
            return getStats().total;
        }

        /**
         * Creates a handle to a job by record id. Does not verify queue membership or existence.
         *
         * @param jobId Salesforce Job__c.Id
         * @return job handle bound to jobId
         * @throws QueueException when jobId is null
         */
        public Job getJob(final Id jobId) {
            return new Job(jobId);
        }

        /**
         * Returns the current durable state of a job.
         *
         * @param jobId Salesforce Job__c.Id
         * @return current state of the job
         * @throws JobNotFoundException when no job exists for jobId
         */
        public State getJobState(final Id jobId) {
            return queueService.getJobState(jobId);
        }

        /**
         * Creates or replaces a `WORKER`-mode scheduler that materializes a job from a default
         * template named after the scheduler id.
         *
         * @param jobSchedulerId queue-scoped scheduler identity; reused to replace an existing one
         * @param repeatOpts cadence: pattern(...) for cron or every(...) for minute-based
         * @return handle to the first materialized occurrence job
         * @throws ConfigurationException when the queue is not configured, is inactive, or the
         * repeat options are invalid
         * @throws InvalidOperationException when an existing scheduler has too many pending jobs to
         * replace safely
         */
        public Job upsertJobScheduler(final String jobSchedulerId, final RepeatOptions repeatOpts) {
            return upsertJobScheduler(jobSchedulerId, repeatOpts, null, Queues.ExecutionMode.WORKER);
        }

        /**
         * Creates or replaces a `WORKER`-mode scheduler with an explicit job template.
         *
         * @param jobSchedulerId queue-scoped scheduler identity
         * @param repeatOpts cadence: pattern(...) or every(...)
         * @param jobTemplate template describing the job name, processor, data, and options
         * @return handle to the first materialized occurrence job
         * @throws ConfigurationException when the queue, processor, repeat options, or template
         * options are invalid
         * @throws InvalidOperationException when an existing scheduler has too many pending jobs to
         * replace safely
         */
        public Job upsertJobScheduler(
            final String jobSchedulerId,
            final RepeatOptions repeatOpts,
            final JobTemplate jobTemplate
        ) {
            return upsertJobScheduler(jobSchedulerId, repeatOpts, jobTemplate, Queues.ExecutionMode.WORKER);
        }

        /**
         * Creates or replaces a scheduler with an explicit execution mode. Re-upsert keeps the
         * scheduler identity, cancels pending jobs from the old schedule, aborts old native
         * schedules, resets durable state from the new options, and advances the monotonic
         * iteration without resetting the lifetime `lim(...)` counter.
         *
         * @param jobSchedulerId queue-scoped scheduler identity
         * @param repeatOpts cadence: pattern(...) or every(...)
         * @param jobTemplate template describing the job name, processor, data, and options
         * @param executionMode lane the materialized occurrences run on
         * @return handle to the first materialized occurrence job
         * @throws ConfigurationException when the queue, processor, repeat options, or template
         * options are invalid
         * @throws InvalidOperationException when an existing scheduler has too many pending jobs to
         * replace safely
         * @example
         * Queues.of('invoice-sync').upsertJobScheduler(
         *     'nightly-sync',
         *     new Queues.RepeatOptions().pattern('0 0 2 * * ?'),
         *     new Queues.JobTemplate('sync', 'InvoiceSyncProcessor'),
         *     Queues.ExecutionMode.QUEUEABLE_SERIAL
         * );
         */
        public Job upsertJobScheduler(
            final String jobSchedulerId,
            final RepeatOptions repeatOpts,
            final JobTemplate jobTemplate,
            final ExecutionMode executionMode
        ) {
            return new Job(
                queueService.upsertJobScheduler(queueName, jobSchedulerId, repeatOpts, jobTemplate, executionMode).Id
            );
        }

        /**
         * Removes a scheduler and its linked jobs, cascading their attempt history, and aborts the
         * native schedule. Destructive; use `cancelJobScheduler(...)` to preserve observability.
         *
         * @param jobSchedulerId queue-scoped scheduler identity
         * @return true when the scheduler was fully removed; false when none existed, or when
         * the scheduler was deactivated but bounded cleanup did not finish and another call (or
         * maintenance) is required
         * @throws InvalidOperationException when the scheduler has ACTIVE linked jobs that block
         * hard removal
         */
        public Boolean removeJobScheduler(final String jobSchedulerId) {
            return queueService.removeJobScheduler(queueName, jobSchedulerId);
        }

        /**
         * Cancels a scheduler: aborts native schedules, deactivates it, and marks pending linked
         * jobs `CANCELED` while preserving all records. A canceled scheduler may later be
         * re-upserted with the same identity.
         *
         * @param jobSchedulerId queue-scoped scheduler identity
         * @return true when cancellation fully completed; false when none existed, there was
         * nothing to cancel, or the scheduler was deactivated but bounded cleanup did not finish and
         * another call (or maintenance) is required
         */
        public Boolean cancelJobScheduler(final String jobSchedulerId) {
            return queueService.cancelJobScheduler(queueName, jobSchedulerId);
        }

        /**
         * Reads a single scheduler's durable state.
         *
         * @param jobSchedulerId queue-scoped scheduler identity
         * @return scheduler value object, or null when none exists
         */
        public JobScheduler getJobScheduler(final String jobSchedulerId) {
            return queueService.getJobScheduler(queueName, jobSchedulerId);
        }

        /**
         * Lists up to the first `25` schedulers for this queue, ascending by name.
         *
         * @return schedulers for this queue
         */
        public List<JobScheduler> getJobSchedulers() {
            return getJobSchedulers(0, 24, true);
        }

        /**
         * Lists schedulers for this queue within an inclusive index window.
         *
         * @param startIndex zero-based start index, clamped to 0
         * @param endIndex zero-based inclusive end index
         * @param ascending true to order by name ascending, false for descending
         * @return schedulers in the requested window
         */
        public List<JobScheduler> getJobSchedulers(
            final Integer startIndex,
            final Integer endIndex,
            final Boolean ascending
        ) {
            return queueService.getJobSchedulers(queueName, startIndex, endIndex, ascending);
        }

        /**
         * @return number of schedulers defined for this queue
         */
        public Integer getJobSchedulersCount() {
            return queueService.getJobSchedulersCount(queueName);
        }
    }

    /**
     * Inserts atomic dependency graphs whose parents wait in `WAITING_CHILDREN` until their
     * children resolve. Graphs are inserted parent-first in waves; any insertion failure rolls back
     * the complete graph. Structural cycles, reused `FlowJob` instances, and duplicate queue-scoped
     * job ids within one graph are rejected. Existing jobs referenced by a graph may only
     * participate in an idempotent replay; adding new children to an existing node is rejected.
     * Obtain one with `Queues.flowProducer()`.
     *
     * @see Queues.FlowJob
     * @see Queues.JobNode
     * @example
     * Queues.JobNode root = Queues.flowProducer().add(
     *     new Queues.FlowJob('finish-import', 'invoice-sync', 'FinishImportProcessor')
     *         .child(new Queues.FlowJob('load-lines', 'invoice-sync', 'LoadLinesProcessor'))
     *         .child(new Queues.FlowJob('sync-summary', 'invoice-sync', 'SyncSummaryProcessor'))
     * );
     */
    public class FlowProducer {
        /**
         * Atomically inserts one dependency graph on the default `WORKER` lane.
         *
         * @param flow root flow job describing the graph
         * @return root node of the created durable job tree
         * @throws QueueException when the graph has cycles, reused FlowJob instances, duplicate
         * queue-scoped job ids, an `opts.parent(...)`, or tries to add new children to an existing
         * job
         * @throws ConfigurationException when a queue or processor in the graph is not configured or
         * inactive
         */
        public JobNode add(final FlowJob flow) {
            return queueService.addFlow(flow);
        }

        /**
         * Atomically inserts one dependency graph on the lane selected by `opts`.
         *
         * @param flow root flow job describing the graph
         * @param opts flow options selecting one execution mode for the whole graph
         * @return root node of the created durable job tree
         * @throws QueueException when the graph is structurally invalid or extends an existing job
         * @throws ConfigurationException when a queue or processor in the graph is not configured or
         * inactive
         */
        public JobNode add(final FlowJob flow, final FlowOpts opts) {
            return queueService.addFlow(flow, opts);
        }

        /**
         * Atomically inserts several dependency graphs on the default `WORKER` lane. If any graph
         * fails, the entire bulk request rolls back.
         *
         * @param flows root flow jobs describing each graph
         * @return root nodes of the created durable job trees, in request order
         * @throws QueueException when any graph is structurally invalid or extends an existing job
         * @throws ConfigurationException when a queue or processor in any graph is not configured or
         * inactive
         */
        public List<JobNode> addBulk(final List<FlowJob> flows) {
            return queueService.addFlowBulk(flows);
        }

        /**
         * Atomically inserts several dependency graphs on the lane selected by `opts`. If any graph
         * fails, the entire bulk request rolls back.
         *
         * @param flows root flow jobs describing each graph
         * @param opts flow options selecting one execution mode for every graph
         * @return root nodes of the created durable job trees, in request order
         * @throws QueueException when any graph is structurally invalid or extends an existing job
         * @throws ConfigurationException when a queue or processor in any graph is not configured or
         * inactive
         */
        public List<JobNode> addBulk(final List<FlowJob> flows, final FlowOpts opts) {
            return queueService.addFlowBulk(flows, opts);
        }

        /**
         * Reads the complete durable dependency tree rooted at a job, as visible to the current user.
         *
         * @param rootJobId Salesforce Job__c.Id of the root job
         * @return root node of the durable job tree, each node carrying a Job handle and children
         * @throws JobNotFoundException when no job exists for rootJobId
         * @example
         * Queues.JobNode root = Queues.flowProducer().getFlow(rootJobId);
         */
        public JobNode getFlow(final Id rootJobId) {
            return queueService.getFlow(rootJobId);
        }
    }

    /**
     * Lightweight handle to a single durable job, identified by its Salesforce `Job__c.Id`.
     * Exposes admin operations (retry, pause, resume, promote, discard, cancel, remove, defer,
     * change priority, update data or progress) and state getters. Obtain one with
     * `Queues.job(jobId)`. Each mutating call validates the job's current state and throws when the
     * transition is not allowed.
     *
     * @see Queues
     * @example
     * Queues.Job job = Queues.job(jobId);
     * if (job.isFailed()) {
     *     job.retry();
     * }
     */
    public class Job {
        private final Id jobId;

        /**
         * @param jobId Salesforce Job__c.Id
         * @throws QueueException when jobId is null
         */
        public Job(final Id jobId) {
            QueuesSupport.notNull(jobId, 'jobId');
            this.jobId = jobId;
        }

        /**
         * @return Salesforce Job__c.Id this handle is bound to
         */
        public Id getId() {
            return jobId;
        }

        /**
         * @return current durable state of the job
         * @throws JobNotFoundException when the job no longer exists
         */
        public State getState() {
            return queueService.getJobState(jobId);
        }

        /**
         * Manually retries a failed, completed, or paused job: resets attempt state and moves it
         * back to runnable work.
         *
         * @return this handle for fluent chaining
         * @throws JobNotFoundException when the job no longer exists
         * @throws InvalidOperationException when the job is not failed, completed, or paused
         */
        public Job retry() {
            queueService.retryJob(jobId);
            return this;
        }

        /**
         * Pauses the job. Waiting, delayed, or paused jobs pause immediately; an active job records
         * a pause request and is paused before its next execution if it stays runnable.
         *
         * @return this handle for fluent chaining
         * @throws JobNotFoundException when the job no longer exists
         * @throws InvalidOperationException when the job is not waiting, delayed, active, or paused
         */
        public Job pause() {
            queueService.pauseJob(jobId);
            return this;
        }

        /**
         * Resumes a paused job, returning it to runnable (or delayed) state.
         *
         * @return this handle for fluent chaining
         * @throws JobNotFoundException when the job no longer exists
         * @throws InvalidOperationException when the job is not paused
         */
        public Job resume() {
            queueService.resumeJob(jobId);
            return this;
        }

        /**
         * Promotes a delayed job to run now: moves it from `DELAYED` to `WAITING` and wakes the
         * queue if needed.
         *
         * @return this handle for fluent chaining
         * @throws JobNotFoundException when the job no longer exists
         * @throws InvalidOperationException when the job is not delayed
         */
        public Job promote() {
            queueService.promoteJob(jobId);
            return this;
        }

        /**
         * Discards future retries for a non-active job by capping its remaining attempts.
         *
         * @return this handle for fluent chaining
         * @throws JobNotFoundException when the job no longer exists
         * @throws InvalidOperationException when the job is active
         */
        public Job discard() {
            queueService.discardJob(jobId);
            return this;
        }

        /**
         * Cancels cancelable future work, marking the job `CANCELED` and preserving the record.
         * Resolves any parent dependency that was waiting on this job.
         *
         * @return this handle for fluent chaining
         * @throws JobNotFoundException when the job no longer exists
         * @throws InvalidOperationException when the job is not waiting, delayed, paused, or
         * waiting-children
         */
        public Job cancel() {
            queueService.cancelJob(jobId);
            return this;
        }

        /**
         * Permanently deletes the job and cascades its `JobRun__c` attempt history. Cannot be
         * replaced by later maintenance; use `cancel()` to preserve records.
         *
         * @throws JobNotFoundException when the job no longer exists
         * @throws InvalidOperationException when the job is active
         */
        public void remove() {
            queueService.removeJob(jobId);
        }

        /**
         * Defers a runnable or paused job by a relative delay.
         *
         * @param delay minutes to delay; 0 makes the job immediately available
         * @return this handle for fluent chaining
         * @throws QueueException when delay is negative, or for a queueable job when it exceeds
         * the native 10-minute queueable delay limit
         * @throws JobNotFoundException when the job no longer exists
         * @throws InvalidOperationException when the job is not waiting, delayed, or paused
         */
        public Job defer(final Integer delay) {
            queueService.deferJob(jobId, delay);
            return this;
        }

        /**
         * Changes claim or dispatch priority for a waiting, delayed, or paused job. Lower values
         * are claimed first; priority does not guarantee execution order.
         *
         * @param priority new priority, zero or greater
         * @return this handle for fluent chaining
         * @throws QueueException when priority is negative
         * @throws JobNotFoundException when the job no longer exists
         * @throws InvalidOperationException when the job is not waiting, delayed, or paused
         */
        public Job changePriority(final Integer priority) {
            queueService.changePriority(jobId, priority);
            return this;
        }

        /**
         * Replaces a non-active job's payload before its next execution.
         *
         * @param data JSON-serializable payload to store
         * @return this handle for fluent chaining
         * @throws JobNotFoundException when the job no longer exists
         * @throws InvalidOperationException when the job is active
         */
        public Job updateData(final Object data) {
            queueService.updateJobData(jobId, data);
            return this;
        }

        /**
         * Replaces a non-active job's progress value.
         *
         * @param progress JSON-serializable progress to store
         * @return this handle for fluent chaining
         * @throws JobNotFoundException when the job no longer exists
         * @throws InvalidOperationException when the job is active
         */
        public Job updateProgress(final Object progress) {
            queueService.updateJobProgress(jobId, progress);
            return this;
        }

        /**
         * @return true when the job is currently WAITING
         * @throws JobNotFoundException when the job no longer exists
         */
        public Boolean isWaiting() {
            return getState() == State.WAITING;
        }

        /**
         * @return true when the job is currently DELAYED
         * @throws JobNotFoundException when the job no longer exists
         */
        public Boolean isDelayed() {
            return getState() == State.DELAYED;
        }

        /**
         * @return true when the job is currently ACTIVE
         * @throws JobNotFoundException when the job no longer exists
         */
        public Boolean isActive() {
            return getState() == State.ACTIVE;
        }

        /**
         * @return true when the job is currently PAUSED
         * @throws JobNotFoundException when the job no longer exists
         */
        public Boolean isPaused() {
            return getState() == State.PAUSED;
        }

        /**
         * @return true when the job is currently COMPLETED
         * @throws JobNotFoundException when the job no longer exists
         */
        public Boolean isCompleted() {
            return getState() == State.COMPLETED;
        }

        /**
         * @return true when the job is currently FAILED
         * @throws JobNotFoundException when the job no longer exists
         */
        public Boolean isFailed() {
            return getState() == State.FAILED;
        }

        /**
         * @return true when the job is currently WAITING_CHILDREN
         * @throws JobNotFoundException when the job no longer exists
         */
        public Boolean isWaitingChildren() {
            return getState() == State.WAITING_CHILDREN;
        }

        /**
         * @return true when the job is currently CANCELED
         * @throws JobNotFoundException when the job no longer exists
         */
        public Boolean isCanceled() {
            return getState() == State.CANCELED;
        }
    }

    /**
     * Node in a durable dependency tree returned by `FlowProducer.add(...)` and
     * `FlowProducer.getFlow(...)`. Each node carries a `Job` handle and its child nodes.
     *
     * @see Queues.FlowProducer
     * @example
     * void render(Queues.JobNode node) {
     *     System.debug(node.getJob().getId() + ': ' + node.getJob().getState());
     *     for (Queues.JobNode child : node.getChildren()) {
     *         render(child);
     *     }
     * }
     */
    public class JobNode {
        private final Job job;
        private final List<JobNode> children;

        /**
         * Creates a leaf node with no children.
         *
         * @param job job handle for this node
         * @throws QueueException when job is null
         */
        public JobNode(final Job job) {
            this(job, new List<JobNode>());
        }

        /**
         * Creates a node with children.
         *
         * @param job job handle for this node
         * @param children child nodes; null is treated as an empty list
         * @throws QueueException when job is null
         */
        public JobNode(final Job job, final List<JobNode> children) {
            QueuesSupport.notNull(job, 'job');
            this.job = job;
            this.children = children ?? new List<JobNode>();
        }

        /**
         * @return job handle for this node
         */
        public Job getJob() {
            return job;
        }

        /**
         * @return child nodes of this node
         */
        public List<JobNode> getChildren() {
            return children;
        }

        /**
         * @return true when this node has at least one child
         */
        public Boolean hasChildren() {
            return !children.isEmpty();
        }
    }

    /**
     * One job specification within an `addBulk(...)` request: a name, the processor class to run,
     * the payload, and optional per-job options.
     *
     * @see Queues.Queue
     * @example
     * new Queues.JobData('sync-invoice', 'InvoiceSyncProcessor', new Map<String, Object>{ 'id' => 'A' });
     */
    public class JobData {
        private final String name;
        private final String processor;
        private final Object data;
        private final JobOptions opts;

        /**
         * Creates a job specification with default options.
         *
         * @param name display name of the job
         * @param processor allowlisted processor class name
         * @param data JSON-serializable payload
         * @throws QueueException when name or processor is blank
         */
        public JobData(final String name, final String processor, final Object data) {
            this(name, processor, data, new JobOptions());
        }

        /**
         * Creates a job specification with explicit options.
         *
         * @param name display name of the job
         * @param processor allowlisted processor class name
         * @param data JSON-serializable payload
         * @param opts per-job options; null is treated as default options
         * @throws QueueException when name or processor is blank
         */
        public JobData(final String name, final String processor, final Object data, final JobOptions opts) {
            QueuesSupport.notBlank(name, 'name');
            QueuesSupport.notBlank(processor, 'processor');
            this.name = name;
            this.processor = processor;
            this.data = data;
            this.opts = opts ?? new JobOptions();
        }

        /**
         * @return display name of the job
         */
        public String getName() {
            return name;
        }

        /**
         * @return JSON-serializable payload
         */
        public Object getData() {
            return data;
        }

        /**
         * @return allowlisted processor class name
         */
        public String getProcessor() {
            return processor;
        }

        /**
         * @return per-job options
         */
        public JobOptions getOpts() {
            return opts;
        }
    }

    /**
     * Builder for one node of a `FlowProducer` dependency graph. Unlike `JobData`, a flow job
     * carries its own queue name so a single graph can span queues, and its dependencies are
     * defined with `child(...)`. `opts.parent(...)` is not accepted inside a submitted graph;
     * define edges with `child(...)`.
     *
     * @see Queues.FlowProducer
     * @example
     * new Queues.FlowJob('finish-import', 'invoice-sync', 'FinishImportProcessor')
     *     .data(new Map<String, Object>{ 'orderId' => 'O-1' })
     *     .child(new Queues.FlowJob('load-lines', 'invoice-sync', 'LoadLinesProcessor'));
     */
    public class FlowJob {
        private final String name;
        private final String queueName;
        private final String processor;
        private Object data;
        private JobOptions opts;
        private List<FlowJob> children = new List<FlowJob>();

        /**
         * @param name display name of the job
         * @param queueName queue this node belongs to
         * @param processor allowlisted processor class name
         * @throws QueueException when name, queueName, or processor is blank
         */
        public FlowJob(final String name, final String queueName, final String processor) {
            QueuesSupport.notBlank(name, 'name');
            QueuesSupport.notBlank(queueName, 'queueName');
            QueuesSupport.notBlank(processor, 'processor');
            this.name = name;
            this.queueName = queueName;
            this.processor = processor;
        }

        /**
         * Sets the JSON-serializable payload for this node.
         *
         * @param data payload exposed to the processor through ctx.data()
         * @return this flow job for fluent chaining
         */
        public FlowJob data(final Object data) {
            this.data = data;
            return this;
        }

        /**
         * Sets the per-job options for this node, including dependency policies.
         *
         * @param opts per-job options; null is treated as default options
         * @return this flow job for fluent chaining
         */
        public FlowJob opts(final JobOptions opts) {
            this.opts = opts ?? new JobOptions();
            return this;
        }

        /**
         * Replaces this node's children with the given list.
         *
         * @param children child flow jobs this node depends on
         * @return this flow job for fluent chaining
         * @throws QueueException when children or any element is null
         */
        public FlowJob children(final List<FlowJob> children) {
            QueuesSupport.notNull(children, 'children');
            this.children = new List<FlowJob>();
            for (FlowJob child : children) {
                QueuesSupport.notNull(child, 'child');
                this.children.add(child);
            }
            return this;
        }

        /**
         * Adds one child this node depends on.
         *
         * @param child child flow job
         * @return this flow job for fluent chaining
         * @throws QueueException when child is null
         */
        public FlowJob child(final FlowJob child) {
            QueuesSupport.notNull(child, 'child');
            children.add(child);
            return this;
        }

        /**
         * @return display name of the job
         */
        public String getName() {
            return name;
        }

        /**
         * @return queue this node belongs to
         */
        public String getQueueName() {
            return queueName;
        }

        /**
         * @return allowlisted processor class name
         */
        public String getProcessor() {
            return processor;
        }

        /**
         * @return JSON-serializable payload, or null when unset
         */
        public Object getData() {
            return data;
        }

        /**
         * @return per-job options, never null
         */
        public JobOptions getOpts() {
            return opts ?? new JobOptions();
        }

        /**
         * @return child flow jobs this node depends on
         */
        public List<FlowJob> getChildren() {
            return children;
        }
    }

    /**
     * Cadence options for a scheduler. Provide exactly one of `pattern(...)` (native cron) or
     * `every(...)` (minute-based self-rescheduling). `lim(...)` caps lifetime occurrences for both
     * strategies; `startAt`/`endAt` further bound an `every(...)` schedule only and are not
     * supported with `pattern(...)`.
     *
     * @see Queues.Queue
     * @example
     * new Queues.RepeatOptions().every(15).startAt(System.now().addMinutes(15)).lim(10);
     * new Queues.RepeatOptions().pattern('0 0 2 * * ?');
     */
    public class RepeatOptions {
        private Integer everyMinutes;
        private String pattern;
        private Integer limitCount;
        private Datetime startAt;
        private Datetime endAt;

        /**
         * Sets a minute-based cadence (alias of `everyMinutes`).
         *
         * @param minutes interval in minutes, greater than 0
         * @return this options object for fluent chaining
         * @throws QueueException when minutes is not greater than 0
         */
        public RepeatOptions every(final Integer minutes) {
            return everyMinutes(minutes);
        }

        /**
         * Sets a minute-based cadence.
         *
         * @param minutes interval in minutes, greater than 0
         * @return this options object for fluent chaining
         * @throws QueueException when minutes is not greater than 0
         */
        public RepeatOptions everyMinutes(final Integer minutes) {
            QueuesSupport.isTrue(minutes > 0, new QueueException('minutes must be greater than 0'));
            everyMinutes = minutes;
            return this;
        }

        /**
         * Sets a native cron cadence.
         *
         * @param cronExpression Salesforce cron expression
         * @return this options object for fluent chaining
         * @throws QueueException when cronExpression is blank
         */
        public RepeatOptions pattern(final String cronExpression) {
            QueuesSupport.notBlank(cronExpression, 'cronExpression');
            pattern = cronExpression;
            return this;
        }

        /**
         * Caps the lifetime number of materialized occurrences for the scheduler identity.
         * Re-upsert does not reset this counter.
         *
         * @param limitCount maximum lifetime occurrences; null means unlimited, otherwise greater
         * than `0`
         * @return this options object for fluent chaining
         * @throws QueueException when limitCount is non-null and not greater than 0
         */
        public RepeatOptions lim(final Integer limitCount) {
            QueuesSupport.isTrue(
                limitCount == null || limitCount > 0,
                new QueueException('limit must be greater than 0')
            );
            this.limitCount = limitCount;
            return this;
        }

        /**
         * Sets the earliest time an `every(...)` schedule may first fire.
         *
         * @param startAt absolute start time
         * @return this options object for fluent chaining
         * @throws QueueException when startAt is null
         */
        public RepeatOptions startAt(final Datetime startAt) {
            QueuesSupport.notNull(startAt, 'startAt');
            this.startAt = startAt;
            return this;
        }

        /**
         * Sets the latest time an `every(...)` schedule may fire.
         *
         * @param endAt absolute end time
         * @return this options object for fluent chaining
         * @throws QueueException when endAt is null
         */
        public RepeatOptions endAt(final Datetime endAt) {
            QueuesSupport.notNull(endAt, 'endAt');
            this.endAt = endAt;
            return this;
        }

        /**
         * @return configured minute interval, or null when a cron pattern is used
         */
        public Integer getEveryMinutes() {
            return everyMinutes;
        }

        /**
         * @return configured cron expression, or null when a minute interval is used
         */
        public String getPattern() {
            return pattern;
        }

        /**
         * @return lifetime occurrence cap, or null when unlimited
         */
        public Integer getLimit() {
            return limitCount;
        }

        /**
         * @return configured start time, or null when unset
         */
        public Datetime getStartAt() {
            return startAt;
        }

        /**
         * @return configured end time, or null when unset
         */
        public Datetime getEndAt() {
            return endAt;
        }
    }

    /**
     * Describes the job a scheduler materializes on each occurrence: the generated job name and
     * processor, the payload, and the options applied to every occurrence. Template options may set
     * attempts, priority, parent provenance, and backoff, but not `jobId(...)`, `delay(...)`, or
     * `availableAt(...)`, because the scheduler owns occurrence ids and timing.
     *
     * @see Queues.Queue
     * @example
     * new Queues.JobTemplate('sync-invoice', 'InvoiceSyncProcessor')
     *     .data(new Map<String, Object>{ 'source' => 'scheduler' })
     *     .opts(new Queues.JobOptions().attempts(3).backoff(Queues.Backoff.EXPONENTIAL, 5));
     */
    public class JobTemplate {
        private final String name;
        private final String processor;
        private Object data;
        private JobOptions opts;

        /**
         * Creates a template whose job name equals the processor class name.
         *
         * @param processor allowlisted processor class name, also used as the job name
         * @throws QueueException when processor is blank
         */
        public JobTemplate(final String processor) {
            this(processor, processor);
        }

        /**
         * Creates a template with an explicit job name.
         *
         * @param name generated job name
         * @param processor allowlisted processor class name
         * @throws QueueException when name or processor is blank
         */
        public JobTemplate(final String name, final String processor) {
            QueuesSupport.notBlank(name, 'name');
            QueuesSupport.notBlank(processor, 'processor');
            this.name = name;
            this.processor = processor;
        }

        /**
         * Sets the payload generated for every occurrence.
         *
         * @param data JSON-serializable payload
         * @return this template for fluent chaining
         */
        public JobTemplate data(final Object data) {
            this.data = data;
            return this;
        }

        /**
         * Sets the options applied to every materialized occurrence.
         *
         * @param opts per-job options; null is treated as default options. Must not set
         * `jobId`, `delay`, or `availableAt`
         * @return this template for fluent chaining
         */
        public JobTemplate opts(final JobOptions opts) {
            this.opts = opts ?? new JobOptions();
            return this;
        }

        /**
         * @return generated job name
         */
        public String getName() {
            return name;
        }

        /**
         * @return allowlisted processor class name
         */
        public String getProcessor() {
            return processor;
        }

        /**
         * @return JSON-serializable payload, or null when unset
         */
        public Object getData() {
            return data;
        }

        /**
         * @return per-occurrence options, never null
         */
        public JobOptions getOpts() {
            return opts ?? new JobOptions();
        }
    }

    /**
     * Read-only snapshot of a durable scheduler's state, returned by `Queue.getJobScheduler(...)`
     * and `Queue.getJobSchedulers(...)`. Reports cadence, lifetime iteration count and cap, window,
     * next run time, active state, and the job template.
     *
     * @see Queues.Queue
     * @example
     * Queues.JobScheduler s = Queues.of('invoice-sync').getJobScheduler('nightly-sync');
     * Integer runs = s.getIterationCount();
     * Datetime nextRunAt = s.getNextRunAt();
     */
    public class JobScheduler {
        private final String jobSchedulerId;
        private final String jobName;
        private final String processor;
        private final Integer iterationCount;
        private final Integer limitCount;
        private final Datetime startAt;
        private final Datetime endAt;
        private final Integer everyMinutes;
        private final String pattern;
        private final Datetime nextRunAt;
        private final Boolean active;
        private final JobTemplate template;

        /**
         * Framework constructor used when mapping durable scheduler state into a value object.
         *
         * @param jobSchedulerId queue-scoped scheduler identity
         * @param jobName generated job name
         * @param processor processor class name
         * @param iterationCount lifetime materialized occurrence count; null is treated as 0
         * @param limitCount lifetime occurrence cap, or null when unlimited
         * @param startAt configured start time, or null
         * @param endAt configured end time, or null
         * @param everyMinutes minute interval, or null when a cron pattern is used
         * @param pattern cron expression, or null when a minute interval is used
         * @param nextRunAt next scheduled run time, or null
         * @param active whether the scheduler is active; null is treated as false
         * @param template materialization template
         */
        public JobScheduler(
            final String jobSchedulerId,
            final String jobName,
            final String processor,
            final Integer iterationCount,
            final Integer limitCount,
            final Datetime startAt,
            final Datetime endAt,
            final Integer everyMinutes,
            final String pattern,
            final Datetime nextRunAt,
            final Boolean active,
            final JobTemplate template
        ) {
            this.jobSchedulerId = jobSchedulerId;
            this.jobName = jobName;
            this.processor = processor;
            this.iterationCount = iterationCount ?? 0;
            this.limitCount = limitCount;
            this.startAt = startAt;
            this.endAt = endAt;
            this.everyMinutes = everyMinutes;
            this.pattern = pattern;
            this.nextRunAt = nextRunAt;
            this.active = active ?? false;
            this.template = template;
        }

        /**
         * @return queue-scoped scheduler identity
         */
        public String getJobSchedulerId() {
            return jobSchedulerId;
        }

        /**
         * @return generated job name
         */
        public String getJobName() {
            return jobName;
        }

        /**
         * @return processor class name
         */
        public String getProcessor() {
            return processor;
        }

        /**
         * @return lifetime number of occurrences materialized so far
         */
        public Integer getIterationCount() {
            return iterationCount;
        }

        /**
         * @return lifetime occurrence cap, or null when unlimited
         */
        public Integer getLimit() {
            return limitCount;
        }

        /**
         * @return configured start time, or null when unset
         */
        public Datetime getStartAt() {
            return startAt;
        }

        /**
         * @return configured end time, or null when unset
         */
        public Datetime getEndAt() {
            return endAt;
        }

        /**
         * @return minute interval, or null when a cron pattern is used
         */
        public Integer getEveryMinutes() {
            return everyMinutes;
        }

        /**
         * @return cron expression, or null when a minute interval is used
         */
        public String getPattern() {
            return pattern;
        }

        /**
         * @return next scheduled run time, or null when none is planned
         */
        public Datetime getNextRunAt() {
            return nextRunAt;
        }

        /**
         * @return true when the scheduler is active
         */
        public Boolean isActive() {
            return active;
        }

        /**
         * @return materialization template
         */
        public JobTemplate getTemplate() {
            return template;
        }
    }

    /**
     * Per-job options applied at enqueue time: idempotency key, attempts, priority, delay or
     * absolute availability, retry backoff, parent provenance, and dependency policies. All setters
     * are chainable. Queue metadata supplies defaults for any option left unset.
     *
     * @see Queues.Queue
     * @example
     * new Queues.JobOptions()
     *     .jobId('invoice:INV-001')
     *     .attempts(5)
     *     .priority(2)
     *     .delay(3)
     *     .backoff(Queues.Backoff.FIXED, 5, 0.25);
     */
    public class JobOptions {
        private String jobId;
        private Integer attempts;
        private Integer priority;
        private Integer delay;
        private Datetime availableAt;
        private Backoff backoffType;
        private Integer backoffValue;
        private Decimal backoffJitter;
        private Id parentJobId;
        private DependencyFailurePolicy dependencyFailurePolicy;
        private DependencyCanceledPolicy dependencyCanceledPolicy;

        /**
         * Sets a caller-owned, queue-scoped idempotency key. Re-adding the same key to the same
         * queue reconciles to the existing job at the database boundary.
         *
         * @param jobId queue-scoped idempotency key, 174 characters or fewer
         * @return this options object for fluent chaining
         * @throws QueueException when jobId is blank or longer than 174 characters
         */
        public JobOptions jobId(final String jobId) {
            QueuesService.requireValidJobId(jobId);
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the maximum number of execution attempts.
         *
         * @param attempts attempt count, greater than 0
         * @return this options object for fluent chaining
         * @throws QueueException when attempts is not greater than 0
         */
        public JobOptions attempts(final Integer attempts) {
            QueuesSupport.isTrue(attempts > 0, new QueueException('attempts must be greater than 0'));
            this.attempts = attempts;
            return this;
        }

        /**
         * Sets the claim or dispatch priority. Lower values are claimed first; priority does not
         * guarantee execution order.
         *
         * @param priority priority, zero or greater
         * @return this options object for fluent chaining
         * @throws QueueException when priority is negative
         */
        public JobOptions priority(final Integer priority) {
            QueuesSupport.isTrue(priority >= 0, new QueueException('priority must be zero or greater'));
            this.priority = priority;
            return this;
        }

        /**
         * Sets a relative availability delay in minutes.
         *
         * @param delay minutes to wait before the job becomes available, zero or greater
         * @return this options object for fluent chaining
         * @throws QueueException when delay is negative
         */
        public JobOptions delay(final Integer delay) {
            QueuesSupport.isTrue(delay >= 0, new QueueException('delay must be zero or greater'));
            this.delay = delay;
            return this;
        }

        /**
         * Sets an absolute availability time. Takes precedence over `delay(...)`.
         *
         * @param availableAt absolute time the job becomes available
         * @return this options object for fluent chaining
         * @throws QueueException when availableAt is null
         */
        public JobOptions availableAt(final Datetime availableAt) {
            QueuesSupport.notNull(availableAt, 'availableAt');
            this.availableAt = availableAt;
            return this;
        }

        /**
         * Sets the retry backoff strategy and base delay with no jitter.
         *
         * @param type backoff strategy
         * @param delay base delay in minutes, zero or greater
         * @return this options object for fluent chaining
         * @throws QueueException when type is null or delay is negative
         */
        public JobOptions backoff(final Backoff type, final Integer delay) {
            return backoff(type, delay, 0.0);
        }

        /**
         * Sets the retry backoff strategy, base delay, and jitter ratio. Queueable retry delay is
         * capped at 10 minutes by the native queueable delay limit.
         *
         * @param type backoff strategy
         * @param delay base delay in minutes, zero or greater
         * @param jitter jitter ratio between 0 and 1; null is treated as 0
         * @return this options object for fluent chaining
         * @throws QueueException when type is null, delay is negative, jitter is outside
         * `0..1`, or jitter is set with a `NONE` backoff
         */
        public JobOptions backoff(final Backoff type, final Integer delay, final Decimal jitter) {
            QueuesSupport.notNull(type, 'type');
            QueuesSupport.isTrue(delay >= 0, new QueueException('delay must be zero or greater'));
            Decimal resolvedJitter = jitter ?? 0;
            QueuesSupport.isTrue(
                resolvedJitter >= 0 && resolvedJitter <= 1,
                new QueueException('jitter must be between 0 and 1')
            );
            QueuesSupport.isTrue(
                type != Backoff.NONE || resolvedJitter == 0,
                new QueueException('jitter requires a backoff strategy')
            );
            backoffType = type;
            backoffValue = type == Backoff.NONE ? 0 : delay;
            backoffJitter = type == Backoff.NONE ? 0 : resolvedJitter;
            return this;
        }

        /**
         * Sets a provenance-only parent link. This does not create waiting-children or failure
         * propagation semantics and is rejected inside a submitted `FlowProducer` graph; use
         * `FlowJob.child(...)` to define dependencies.
         *
         * @param parentJobId Salesforce Job__c.Id of the parent
         * @return this options object for fluent chaining
         * @throws QueueException when parentJobId is null
         */
        public JobOptions parent(final Id parentJobId) {
            QueuesSupport.notNull(parentJobId, 'parentJobId');
            this.parentJobId = parentJobId;
            return this;
        }

        /**
         * Flow dependency policy: fail the parent when this child reaches terminal `FAILED`.
         *
         * @return this options object for fluent chaining
         */
        public JobOptions failParentOnFailure() {
            dependencyFailurePolicy = Queues.DependencyFailurePolicy.FAIL_PARENT;
            return this;
        }

        /**
         * Flow dependency policy: release the parent when this child fails, even if other children
         * are still unresolved.
         *
         * @return this options object for fluent chaining
         */
        public JobOptions continueParentOnFailure() {
            dependencyFailurePolicy = Queues.DependencyFailurePolicy.CONTINUE_PARENT;
            return this;
        }

        /**
         * Flow dependency policy: treat only this failed child as resolved; the parent still waits
         * for remaining children.
         *
         * @return this options object for fluent chaining
         */
        public JobOptions ignoreDependencyOnFailure() {
            dependencyFailurePolicy = Queues.DependencyFailurePolicy.IGNORE_DEPENDENCY;
            return this;
        }

        /**
         * Flow dependency policy: treat this failed child as resolved and remove the parent link.
         *
         * @return this options object for fluent chaining
         */
        public JobOptions removeDependencyOnFailure() {
            dependencyFailurePolicy = Queues.DependencyFailurePolicy.REMOVE_DEPENDENCY;
            return this;
        }

        /**
         * Flow dependency policy: cancel the parent when this child reaches terminal `CANCELED`.
         *
         * @return this options object for fluent chaining
         */
        public JobOptions cancelParentOnCanceled() {
            dependencyCanceledPolicy = Queues.DependencyCanceledPolicy.CANCEL_PARENT;
            return this;
        }

        /**
         * Flow dependency policy: fail the parent when this child reaches terminal `CANCELED`.
         *
         * @return this options object for fluent chaining
         */
        public JobOptions failParentOnCanceled() {
            dependencyCanceledPolicy = Queues.DependencyCanceledPolicy.FAIL_PARENT;
            return this;
        }

        /**
         * Flow dependency policy: treat only this canceled child as resolved; the parent still
         * waits for remaining children.
         *
         * @return this options object for fluent chaining
         */
        public JobOptions ignoreDependencyOnCanceled() {
            dependencyCanceledPolicy = Queues.DependencyCanceledPolicy.IGNORE_DEPENDENCY;
            return this;
        }

        /**
         * @return configured idempotency key, or null when unset
         */
        public String getJobId() {
            return jobId;
        }

        /**
         * @return configured attempt count, or null when unset
         */
        public Integer getAttempts() {
            return attempts;
        }

        /**
         * @return configured priority, or null when unset
         */
        public Integer getPriority() {
            return priority;
        }

        /**
         * @return configured relative delay in minutes, or null when unset
         */
        public Integer getDelay() {
            return delay;
        }

        /**
         * @return configured absolute availability time, or null when unset
         */
        public Datetime getAvailableAt() {
            return availableAt;
        }

        /**
         * @return configured backoff strategy, or null when unset
         */
        public Backoff getBackoffType() {
            return backoffType;
        }

        /**
         * @return configured backoff base delay in minutes, or null when unset
         */
        public Integer getBackoffValue() {
            return backoffValue;
        }

        /**
         * @return configured backoff jitter ratio, or null when unset
         */
        public Decimal getBackoffJitter() {
            return backoffJitter;
        }

        /**
         * @return configured provenance parent id, or null when unset
         */
        public Id getParentJobId() {
            return parentJobId;
        }

        /**
         * @return configured dependency failure policy, or null for the default
         */
        public DependencyFailurePolicy getDependencyFailurePolicy() {
            return dependencyFailurePolicy;
        }

        /**
         * @return configured dependency canceled policy, or null for the default
         */
        public DependencyCanceledPolicy getDependencyCanceledPolicy() {
            return dependencyCanceledPolicy;
        }
    }

    /**
     * Bulk-request options: the execution lane for the whole request and an optional dispatcher
     * dedupe key. `WORKER` is the default. The dedupe key dedupes the initial queueable dispatcher
     * handoff only; it is not a per-job idempotency key (use `JobOptions.jobId(...)` for that).
     *
     * @see Queues.Queue
     * @example
     * new Queues.BulkJobOptions().queueable().dedupeKey('invoice-sync:batch-2026-06-04');
     */
    public class BulkJobOptions {
        private String dedupeKey;
        private ExecutionMode executionMode;

        /**
         * Sets the dispatcher dedupe key for the initial queueable handoff.
         *
         * @param dedupeKey dispatcher dedupe key
         * @return this options object for fluent chaining
         * @throws QueueException when dedupeKey is blank
         */
        public BulkJobOptions dedupeKey(final String dedupeKey) {
            QueuesSupport.notBlank(dedupeKey, 'dedupeKey');
            this.dedupeKey = dedupeKey;
            return this;
        }

        /**
         * Selects the `WORKER` lane (the default).
         *
         * @return this options object for fluent chaining
         */
        public BulkJobOptions worker() {
            executionMode = Queues.ExecutionMode.WORKER;
            return this;
        }

        /**
         * Selects the `QUEUEABLE_SERIAL` lane.
         *
         * @return this options object for fluent chaining
         */
        public BulkJobOptions queueable() {
            executionMode = Queues.ExecutionMode.QUEUEABLE_SERIAL;
            return this;
        }

        /**
         * Selects the `QUEUEABLE_CONCURRENT` lane.
         *
         * @return this options object for fluent chaining
         */
        public BulkJobOptions queueableConcurrent() {
            executionMode = Queues.ExecutionMode.QUEUEABLE_CONCURRENT;
            return this;
        }

        /**
         * Selects the `INVOCABLE` lane.
         *
         * @return this options object for fluent chaining
         */
        public BulkJobOptions invocable() {
            executionMode = Queues.ExecutionMode.INVOCABLE;
            return this;
        }

        /**
         * @return selected execution mode, defaulting to WORKER when unset
         */
        public ExecutionMode getExecutionMode() {
            return executionMode ?? Queues.ExecutionMode.WORKER;
        }

        /**
         * @return configured dispatcher dedupe key, or null when unset
         */
        public String getDedupeKey() {
            return dedupeKey;
        }
    }

    /**
     * Flow-request options: selects the single execution lane applied to every job in a
     * `FlowProducer` graph. `WORKER` is the default.
     *
     * @see Queues.FlowProducer
     * @example
     * Queues.flowProducer().add(flow, new Queues.FlowOpts().queueable());
     */
    public class FlowOpts {
        private ExecutionMode executionMode;

        /**
         * Selects the `WORKER` lane (the default).
         *
         * @return this options object for fluent chaining
         */
        public FlowOpts worker() {
            executionMode = Queues.ExecutionMode.WORKER;
            return this;
        }

        /**
         * Selects the `QUEUEABLE_SERIAL` lane.
         *
         * @return this options object for fluent chaining
         */
        public FlowOpts queueable() {
            executionMode = Queues.ExecutionMode.QUEUEABLE_SERIAL;
            return this;
        }

        /**
         * Selects the `QUEUEABLE_CONCURRENT` lane.
         *
         * @return this options object for fluent chaining
         */
        public FlowOpts queueableConcurrent() {
            executionMode = Queues.ExecutionMode.QUEUEABLE_CONCURRENT;
            return this;
        }

        /**
         * Selects the `INVOCABLE` lane.
         *
         * @return this options object for fluent chaining
         */
        public FlowOpts invocable() {
            executionMode = Queues.ExecutionMode.INVOCABLE;
            return this;
        }

        /**
         * @return selected execution mode, defaulting to WORKER when unset
         */
        public ExecutionMode getExecutionMode() {
            return executionMode ?? Queues.ExecutionMode.WORKER;
        }
    }

    /**
     * Durable, read-only context passed to `JobProcessor.process(...)` for a single attempt. Exposes
     * the job's identity, idempotency key, attempt counters, pause request, and deserialized data,
     * checkpoint, and progress, plus the factory methods that build the processor's result.
     *
     * @see Queues.JobProcessor
     * @see Queues.ProcessResult
     * @example
     * public Queues.ProcessResult process(final Queues.JobContext ctx) {
     *     Map<String, Object> data = (Map<String, Object>) ctx.data();
     *     request.setHeader('Idempotency-Key', ctx.idempotencyKey());
     *     return ctx.complete(new Map<String, Object>{ 'status' => 'synced' });
     * }
     */
    public class JobContext {
        private final Id jobId;
        private final String queueName;
        private final String name;
        private final String processorClass;
        private final String idempotencyKey;
        private final Integer attemptsMade;
        private final Integer maxAttempts;
        private final Boolean pauseRequested;
        private final String dataJson;
        private final String checkpointJson;
        private final String progressJson;

        /**
         * Framework constructor that snapshots a durable job into an attempt context.
         *
         * @param job durable job record being executed
         * @throws QueueException when job is null
         */
        public JobContext(final Job__c job) {
            QueuesSupport.notNull(job, 'job');
            jobId = job.Id;
            queueName = job.QueueName__c;
            name = job.Name__c;
            processorClass = job.ProcessorClass__c;
            idempotencyKey = String.isNotBlank(job.QueueScopedExternalId__c)
                ? job.QueueScopedExternalId__c
                : QueuesSupport.buildQueueScopedExternalId(job.QueueName__c, job.Id);
            attemptsMade = QueuesSupport.toInteger(job.AttemptsMade__c, 0);
            maxAttempts = QueuesSupport.toInteger(job.MaxAttempts__c, 0);
            pauseRequested = job.PauseRequested__c;
            dataJson = job.Data__c;
            checkpointJson = job.Checkpoint__c;
            progressJson = job.Progress__c;
        }

        /**
         * @return Salesforce Job__c.Id of the durable job
         */
        public Id jobId() {
            return jobId;
        }

        /**
         * @return queue this job belongs to
         */
        public String queueName() {
            return queueName;
        }

        /**
         * @return display name of the job
         */
        public String name() {
            return name;
        }

        /**
         * @return processor class name running the job
         */
        public String processorClass() {
            return processorClass;
        }

        /**
         * Returns a stable, queue-scoped logical key for the job. The format is the queue name, a
         * colon, and the caller `jobId(...)` when set (`queueName + ':' + jobId`), otherwise the
         * queue name, a colon, and the record id (`queueName + ':' + Job__c.Id`). Send it to
         * external systems that support idempotent requests so a replayed attempt does not
         * double-apply a side effect.
         *
         * @return stable queue-scoped idempotency key for the durable job
         */
        public String idempotencyKey() {
            return idempotencyKey;
        }

        /**
         * Returns the current attempt number, counting this attempt: the first attempt sees `1`.
         *
         * @return current attempt number, including this attempt
         */
        public Integer attemptsMade() {
            return attemptsMade;
        }

        /**
         * @return configured maximum number of attempts
         */
        public Integer maxAttempts() {
            return maxAttempts;
        }

        /**
         * @return true when a pause was requested while the job was active
         */
        public Boolean isPauseRequested() {
            return pauseRequested ?? false;
        }

        /**
         * @return deserialized job payload, or null when there is none
         */
        public Object data() {
            return QueuesSupport.deserialize(dataJson);
        }

        /**
         * @return deserialized checkpoint saved by a previous pause or defer, or null when none
         */
        public Object checkpoint() {
            return QueuesSupport.deserialize(checkpointJson);
        }

        /**
         * @return deserialized progress saved by a previous attempt, or null when none
         */
        public Object progress() {
            return QueuesSupport.deserialize(progressJson);
        }

        /**
         * Builds a completed result carrying a return value.
         *
         * @param returnValue JSON-serializable value to store as the job result
         * @return completed process result
         */
        public ProcessResult complete(final Object returnValue) {
            return new ProcessResult(Outcome.COMPLETED).withReturnValue(returnValue);
        }

        /**
         * Builds a retryable failed result. The job retries while attempts remain.
         *
         * @param failedReason human-readable failure reason recorded on the attempt and job
         * @return failed process result
         */
        public ProcessResult fail(final String failedReason) {
            return new ProcessResult(Outcome.FAILED).withFailedReason(failedReason);
        }

        /**
         * Builds a paused result carrying a checkpoint. The job pauses until resumed and the next
         * attempt can read the checkpoint.
         *
         * @param checkpoint JSON-serializable checkpoint to resume from
         * @return paused process result
         */
        public ProcessResult pause(final Object checkpoint) {
            return new ProcessResult(Outcome.PAUSED).withCheckpoint(checkpoint);
        }

        /**
         * Builds a deferred result carrying a checkpoint and a delay. The job becomes available
         * again after the delay and the next attempt can read the checkpoint.
         *
         * @param checkpoint JSON-serializable checkpoint to resume from
         * @param delay minutes to wait before the job is available again, zero or greater
         * @return deferred process result
         * @throws QueueException when delay is negative
         */
        public ProcessResult defer(final Object checkpoint, final Integer delay) {
            return new ProcessResult(Outcome.DEFERRED).withCheckpoint(checkpoint).withDelay(delay);
        }
    }

    /**
     * Outcome a `JobProcessor` returns from `process(...)`, built through the `JobContext` factories
     * (`complete`, `fail`, `pause`, `defer`) and optionally enriched with progress. The framework
     * persists the durable job and attempt result from this value after the processor returns.
     *
     * @see Queues.JobContext
     * @example
     * return ctx.defer(new Map<String, Object>{ 'step' => 'posted' }, 5)
     *     .withProgress(new Map<String, Object>{ 'step' => 'waiting-for-ledger' });
     */
    public class ProcessResult {
        private final Outcome outcome;
        private String checkpointJson;
        private String progressJson;
        private String returnValueJson;
        private String failedReason;
        private Integer delay;

        private ProcessResult(final Outcome outcome) {
            this.outcome = outcome;
        }

        /**
         * Attaches a checkpoint to resume from on the next attempt.
         *
         * @param checkpoint JSON-serializable checkpoint
         * @return this result for fluent chaining
         */
        public ProcessResult withCheckpoint(final Object checkpoint) {
            checkpointJson = QueuesSupport.serialize(checkpoint);
            return this;
        }

        /**
         * Attaches progress that is stored durably and readable by later attempts through
         * `ctx.progress()`.
         *
         * @param progress JSON-serializable progress
         * @return this result for fluent chaining
         */
        public ProcessResult withProgress(final Object progress) {
            progressJson = QueuesSupport.serialize(progress);
            return this;
        }

        /**
         * Attaches the value stored as the completed job's result.
         *
         * @param returnValue JSON-serializable return value
         * @return this result for fluent chaining
         */
        public ProcessResult withReturnValue(final Object returnValue) {
            returnValueJson = QueuesSupport.serialize(returnValue);
            return this;
        }

        /**
         * Attaches the failure reason recorded on the attempt and job.
         *
         * @param failedReason human-readable failure reason
         * @return this result for fluent chaining
         */
        public ProcessResult withFailedReason(final String failedReason) {
            this.failedReason = failedReason;
            return this;
        }

        /**
         * Attaches the delay before a deferred job becomes available again.
         *
         * @param delay minutes to wait, zero or greater
         * @return this result for fluent chaining
         * @throws QueueException when delay is negative
         */
        public ProcessResult withDelay(final Integer delay) {
            QueuesSupport.isTrue(delay >= 0, new QueueException('delay must be zero or greater'));
            this.delay = delay;
            return this;
        }

        /**
         * @return outcome this result represents
         */
        public Outcome getOutcome() {
            return outcome;
        }

        /**
         * @return serialized checkpoint, or null when none was attached
         */
        public String getCheckpointJson() {
            return checkpointJson;
        }

        /**
         * @return serialized progress, or null when none was attached
         */
        public String getProgressJson() {
            return progressJson;
        }

        /**
         * @return serialized return value, or null when none was attached
         */
        public String getReturnValueJson() {
            return returnValueJson;
        }

        /**
         * @return failure reason, or null when none was attached
         */
        public String getFailedReason() {
            return failedReason;
        }

        /**
         * @return delay in minutes for a deferred result, or null when none was attached
         */
        public Integer getDelay() {
            return delay;
        }
    }

    /**
     * Exact aggregate statistics for a queue: pause state, active worker count, total job count,
     * and per-state job counts. Returned by `Queue.getStats()`.
     *
     * @see Queues.Queue
     * @example
     * Queues.QueueStats stats = Queues.of('invoice-sync').getStats();
     * Integer failed = stats.getCount(Queues.State.FAILED);
     */
    public class QueueStats {
        /** Queue these statistics describe. */
        public String queueName { get; private set; }
        /** Whether the queue is currently paused. */
        public Boolean isPaused { get; private set; }
        /** Number of active framework workers for the queue. */
        public Integer activeWorkerCount { get; private set; }
        /** Total number of jobs across all states. */
        public Integer total { get; private set; }
        /** Number of `WAITING` jobs. */
        public Integer waiting { get; private set; }
        /** Number of `DELAYED` jobs. */
        public Integer delayed { get; private set; }
        /** Number of `ACTIVE` jobs. */
        public Integer active { get; private set; }
        /** Number of `COMPLETED` jobs. */
        public Integer completed { get; private set; }
        /** Number of `FAILED` jobs. */
        public Integer failed { get; private set; }
        /** Number of `PAUSED` jobs. */
        public Integer paused { get; private set; }
        /** Number of `WAITING_CHILDREN` jobs. */
        public Integer waitingChildren { get; private set; }
        /** Number of `CANCELED` jobs. */
        public Integer canceled { get; private set; }

        /**
         * Creates an empty statistics object for a queue.
         *
         * @param queueName queue these statistics describe
         */
        public QueueStats(final String queueName) {
            this.queueName = queueName;
            isPaused = false;
            activeWorkerCount = 0;
            total = 0;
            waiting = 0;
            delayed = 0;
            active = 0;
            completed = 0;
            failed = 0;
            paused = 0;
            waitingChildren = 0;
            canceled = 0;
        }

        /**
         * Sets the runtime fields (pause state and active worker count).
         *
         * @param isPaused whether the queue is paused; null is treated as false
         * @param activeWorkerCount number of active workers; null is treated as 0
         * @return this statistics object for fluent chaining
         */
        public QueueStats withRuntime(final Boolean isPaused, final Integer activeWorkerCount) {
            this.isPaused = isPaused ?? false;
            this.activeWorkerCount = activeWorkerCount ?? 0;
            return this;
        }

        /**
         * Adds a count for one state, updating the per-state field and `total`.
         *
         * @param state state the count belongs to
         * @param count number of jobs to add; null is treated as 0
         * @return this statistics object for fluent chaining
         */
        public QueueStats withStateCount(final State state, final Integer count) {
            Integer safeCount = count ?? 0;
            total += safeCount;
            switch on state {
                when WAITING {
                    waiting += safeCount;
                }
                when DELAYED {
                    delayed += safeCount;
                }
                when ACTIVE {
                    active += safeCount;
                }
                when COMPLETED {
                    completed += safeCount;
                }
                when FAILED {
                    failed += safeCount;
                }
                when PAUSED {
                    paused += safeCount;
                }
                when WAITING_CHILDREN {
                    waitingChildren += safeCount;
                }
                when CANCELED {
                    canceled += safeCount;
                }
                when else {
                }
            }
            return this;
        }

        /**
         * Returns the job count for a single state.
         *
         * @param state state to look up
         * @return job count for state, or 0 for an unrecognized state
         */
        public Integer getCount(final State state) {
            switch on state {
                when WAITING {
                    return waiting;
                }
                when DELAYED {
                    return delayed;
                }
                when ACTIVE {
                    return active;
                }
                when COMPLETED {
                    return completed;
                }
                when FAILED {
                    return failed;
                }
                when PAUSED {
                    return paused;
                }
                when WAITING_CHILDREN {
                    return waitingChildren;
                }
                when CANCELED {
                    return canceled;
                }
                when else {
                    return 0;
                }
            }
        }

        /**
         * Returns job counts keyed by every state.
         *
         * @return map of each State to its job count
         */
        public Map<State, Integer> asJobCounts() {
            return asJobCounts(null);
        }

        /**
         * Returns job counts for the requested states only.
         *
         * @param states states to include; null or empty returns counts for all states
         * @return map of each requested State to its job count, with 0 for absent states
         */
        public Map<State, Integer> asJobCounts(final Set<State> states) {
            Map<State, Integer> countsByState = new Map<State, Integer>{
                State.WAITING => waiting,
                State.DELAYED => delayed,
                State.ACTIVE => active,
                State.COMPLETED => completed,
                State.FAILED => failed,
                State.PAUSED => paused,
                State.WAITING_CHILDREN => waitingChildren,
                State.CANCELED => canceled
            };
            if (states == null || states.isEmpty()) {
                return countsByState;
            }

            Map<State, Integer> filteredCountsByState = new Map<State, Integer>();
            for (State state : states) {
                filteredCountsByState.put(state, countsByState.containsKey(state) ? countsByState.get(state) : 0);
            }
            return filteredCountsByState;
        }
    }

    /**
     * Base exception for all framework errors. Catch this to handle any `Queues` failure.
     */
    public virtual class QueueException extends Exception {
    }

    /**
     * Thrown when a queue or processor is missing or inactive in custom metadata.
     */
    public class ConfigurationException extends QueueException {
    }

    /**
     * Thrown when an operation targets a job id that does not exist.
     */
    public class JobNotFoundException extends QueueException {
    }

    /**
     * Thrown when an operation is not allowed for the job's current state, for example pausing a
     * completed job or removing an active one.
     */
    public class InvalidOperationException extends QueueException {
    }

    /**
     * Thrown by a processor to fail its job immediately without consuming further retries.
     *
     * @example
     * throw new Queues.UnrecoverableJobException('Invoice is permanently invalid.');
     */
    public class UnrecoverableJobException extends QueueException {
    }
}
