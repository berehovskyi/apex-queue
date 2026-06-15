@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesE2EJobProbeProcessor implements Queues.JobProcessor {
    private static final String TRANSACTION_MARKER = QueuesSupport.generateRandomToken();

    private static final String COMPLETE_MODE = 'complete';
    private static final String FAIL_MODE = 'fail';
    private static final String FAIL_UNTIL_ATTEMPT_MODE = 'fail-until-attempt';
    private static final String UNRECOVERABLE_MODE = 'unrecoverable';
    private static final String ASSERT_COMPLETED_JOB_MODE = 'assert-completed-job';
    private static final String ASSERT_ANY_COMPLETED_CHILD_MODE = 'assert-any-completed-child';
    private static final String PERFORMANCE_WORKER_QUEUE_NAME = 'e2e-performance-worker';
    private static final String PERFORMANCE_PARALLEL_WORKER_QUEUE_PREFIX = 'e2e-performance-worker-parallel-';
    private static final String PERFORMANCE_QUEUEABLE_QUEUE_NAME = 'e2e-performance-queueable';
    private static final String PERFORMANCE_QUEUEABLE_CONCURRENT_QUEUE_NAME = 'e2e-performance-queueable-concurrent';
    private static final String PERFORMANCE_INVOCABLE_QUEUE_NAME = 'e2e-performance-invocable';
    private static final Integer MAX_DELAY_MS = 5000;

    private static final String DEFAULT_FAILURE_MESSAGE = 'Retryable processor failure.';
    private static final String DEFAULT_UNRECOVERABLE_MESSAGE = 'Unrecoverable processor failure.';

    public Queues.ProcessResult process(final Queues.JobContext ctx) {
        Map<String, Object> data = (Map<String, Object>) ctx.data();
        String mode = stringValue(data, 'mode');
        Long startedAtMs = System.now().getTime();
        burnCpu(toInteger(data, 'delayMs', 0));

        if (mode == COMPLETE_MODE) {
            return complete(ctx, data, startedAtMs);
        }
        if (mode == FAIL_MODE) {
            throw new Queues.QueueException(resolveFailureMessage(ctx, data, DEFAULT_FAILURE_MESSAGE));
        }
        if (mode == FAIL_UNTIL_ATTEMPT_MODE) {
            return failUntilAttempt(ctx, data, startedAtMs);
        }
        if (mode == UNRECOVERABLE_MODE) {
            throw new Queues.UnrecoverableJobException(resolveMessage(data, DEFAULT_UNRECOVERABLE_MESSAGE));
        }
        if (mode == ASSERT_COMPLETED_JOB_MODE) {
            return assertCompletedJob(ctx, data, startedAtMs);
        }
        if (mode == ASSERT_ANY_COMPLETED_CHILD_MODE) {
            return assertAnyCompletedChild(ctx, data, startedAtMs);
        }

        throw new Queues.QueueException('Unsupported e2e job probe mode "' + mode + '".');
    }

    private static Queues.ProcessResult complete(
        final Queues.JobContext ctx,
        final Map<String, Object> data,
        final Long startedAtMs
    ) {
        Map<String, Object> returnValue = new Map<String, Object>{
            'mode' => stringValue(data, 'mode'),
            'payload' => data?.get('payload'),
            'jobId' => ctx.jobId(),
            'idempotencyKey' => ctx.idempotencyKey(),
            'queueName' => ctx.queueName(),
            'transactionMarker' => TRANSACTION_MARKER
        };
        addQueueableStackDepth(returnValue);
        return ctx.complete(returnValue).withProgress(completedProgress(ctx, data, startedAtMs));
    }

    private static Queues.ProcessResult failUntilAttempt(
        final Queues.JobContext ctx,
        final Map<String, Object> data,
        final Long startedAtMs
    ) {
        Integer succeedAtAttempt = toInteger(data, 'succeedAtAttempt', 2);
        QueuesSupport.isTrue(
            succeedAtAttempt > 0,
            new Queues.QueueException('succeedAtAttempt must be greater than 0.')
        );
        if (ctx.attemptsMade() >= succeedAtAttempt) {
            return complete(ctx, data, startedAtMs);
        }

        throw new Queues.QueueException(resolveFailureMessage(ctx, data, DEFAULT_FAILURE_MESSAGE));
    }

    private static Queues.ProcessResult assertCompletedJob(
        final Queues.JobContext ctx,
        final Map<String, Object> data,
        final Long startedAtMs
    ) {
        String requiredExternalId = stringValue(data, 'requiredExternalId');
        QueuesSupport.notBlank(requiredExternalId, 'requiredExternalId');

        Job__c requiredJob = findJob(ctx.queueName(), requiredExternalId);
        QueuesSupport.isTrue(
            requiredJob != null,
            new Queues.QueueException('Required job "' + requiredExternalId + '" was not found.')
        );
        if (QueuesSupport.toState(requiredJob.State__c) != Queues.State.COMPLETED) {
            throw new Queues.QueueException(
                resolveMessage(data, 'Required job "' + requiredExternalId + '" is not completed yet.')
            );
        }

        Map<String, Object> progress = completedProgress(ctx, data, startedAtMs);
        progress.put('requiredExternalId', requiredExternalId);
        return complete(ctx, data, startedAtMs).withProgress(progress);
    }

    private static Queues.ProcessResult assertAnyCompletedChild(
        final Queues.JobContext ctx,
        final Map<String, Object> data,
        final Long startedAtMs
    ) {
        Integer completedChildCount = 0;
        for (Queues.JobNode child : Queues.flowProducer().getFlow(ctx.jobId()).getChildren()) {
            if (child.getJob().isCompleted()) {
                completedChildCount++;
            }
        }
        if (completedChildCount == 0) {
            throw new Queues.UnrecoverableJobException(resolveMessage(data, 'Every child job failed.'));
        }

        Map<String, Object> progress = completedProgress(ctx, data, startedAtMs);
        progress.put('completedChildCount', completedChildCount);
        return complete(ctx, data, startedAtMs).withProgress(progress);
    }

    private static Map<String, Object> completedProgress(
        final Queues.JobContext ctx,
        final Map<String, Object> data,
        final Long startedAtMs
    ) {
        Map<String, Object> progress = new Map<String, Object>{
            'mode' => stringValue(data, 'mode'),
            'status' => 'completed',
            'jobId' => ctx.jobId(),
            'idempotencyKey' => ctx.idempotencyKey(),
            'transactionMarker' => TRANSACTION_MARKER
        };
        if (isPerformanceQueue(ctx.queueName())) {
            progress.put('startedAtMs', startedAtMs.toString());
        }
        addQueueableStackDepth(progress);
        return progress;
    }

    private static void addQueueableStackDepth(final Map<String, Object> target) {
        Quiddity currentQuiddity = Request.getCurrent().getQuiddity();
        if (currentQuiddity != Quiddity.QUEUEABLE && currentQuiddity != Quiddity.TRANSACTION_FINALIZER_QUEUEABLE) {
            return;
        }

        target.put('queueableStackDepth', System.AsyncInfo.getCurrentQueueableStackDepth());
        target.put('maximumQueueableStackDepth', System.AsyncInfo.getMaximumQueueableStackDepth());
    }

    private static Boolean isPerformanceQueue(final String queueName) {
        return queueName == PERFORMANCE_WORKER_QUEUE_NAME ||
            queueName?.startsWith(PERFORMANCE_PARALLEL_WORKER_QUEUE_PREFIX) == true ||
            queueName == PERFORMANCE_QUEUEABLE_QUEUE_NAME ||
            queueName == PERFORMANCE_QUEUEABLE_CONCURRENT_QUEUE_NAME ||
            queueName == PERFORMANCE_INVOCABLE_QUEUE_NAME;
    }

    private static Job__c findJob(final String queueName, final String externalId) {
        List<Job__c> jobs = [
            SELECT Id, State__c
            FROM Job__c
            WHERE QueueName__c = :queueName AND ExternalId__c = :externalId
            ORDER BY CreatedDate DESC
            LIMIT 1
        ];
        return jobs.isEmpty() ? null : jobs[0];
    }

    private static String resolveMessage(final Map<String, Object> data, final String defaultMessage) {
        String message = stringValue(data, 'message');
        return String.isNotBlank(message) ? message : defaultMessage;
    }

    private static String resolveFailureMessage(
        final Queues.JobContext ctx,
        final Map<String, Object> data,
        final String defaultMessage
    ) {
        String message = resolveMessage(data, defaultMessage);
        if (data?.get('includeIdempotencyKeyInFailureMessage') != true) {
            return message;
        }

        return message + ' [idempotencyKey=' + ctx.idempotencyKey() + ']';
    }

    private static String stringValue(final Map<String, Object> data, final String key) {
        if (data == null || String.isBlank(key) || !data.containsKey(key) || data.get(key) == null) {
            return null;
        }
        return data.get(key).toString();
    }

    private static Integer toInteger(final Map<String, Object> data, final String key, final Integer defaultValue) {
        String value = stringValue(data, key);
        return String.isBlank(value) ? defaultValue : Integer.valueOf(value);
    }

    private static void burnCpu(final Integer requestedDelayMs) {
        Integer delayMs = Math.max(0, Math.min(requestedDelayMs ?? 0, MAX_DELAY_MS));
        if (delayMs == 0) {
            return;
        }

        Long startedAt = System.now().getTime();
        while (System.now().getTime() - startedAt < delayMs) {
        }
    }
}
