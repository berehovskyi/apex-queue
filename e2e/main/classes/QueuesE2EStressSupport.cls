@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesE2EStressSupport {
    public static final String JOB_ID_PREFIX = 'stress-';
    public static final String SCHEDULER_NAME_PREFIX = 'stress-scheduler-';
    public static final Integer JOB_COUNT_PER_MODE = 500;
    public static final Integer PROCESSOR_DELAY_MS = 250;
    public static final Integer SCHEDULED_APEX_LIMIT = 100;
    public static final Integer SCHEDULED_APEX_RESERVE = 10;
    public static final Integer MAX_SCHEDULER_COUNT = 90;

    private static final String PROCESSOR_CLASS = 'QueuesE2EJobProbeProcessor';
    private static final String QUEUEABLE_SERIAL_QUEUE_NAME = 'e2e-stress-queueable';
    private static final String QUEUEABLE_CONCURRENT_QUEUE_NAME = 'e2e-stress-queueable-concurrent';
    private static final String INVOCABLE_QUEUE_NAME = 'e2e-stress-invocable';
    private static final List<String> WORKER_QUEUE_NAMES = new List<String>{
        'e2e-stress-worker-1',
        'e2e-stress-worker-2',
        'e2e-stress-worker-3',
        'e2e-stress-worker-4',
        'e2e-stress-worker-5'
    };

    public static Integer enqueueWorkerJobs() {
        Integer jobsPerQueue = JOB_COUNT_PER_MODE / WORKER_QUEUE_NAMES.size();
        Integer enqueuedCount = 0;
        for (Integer queueIndex = 0; queueIndex < WORKER_QUEUE_NAMES.size(); queueIndex++) {
            enqueuedCount += Queues.of(WORKER_QUEUE_NAMES[queueIndex])
                .addBulk(buildJobs('worker-' + queueIndex + '-', jobsPerQueue))
                .size();
        }
        return enqueuedCount;
    }

    public static Integer enqueueQueueableSerialJobs() {
        return Queues.of(QUEUEABLE_SERIAL_QUEUE_NAME)
            .addBulk(buildJobs('queueable-serial-', JOB_COUNT_PER_MODE), new Queues.BulkJobOptions().queueable())
            .size();
    }

    public static Integer enqueueQueueableConcurrentJobs() {
        return Queues.of(QUEUEABLE_CONCURRENT_QUEUE_NAME)
            .addBulk(
                buildJobs('queueable-concurrent-', JOB_COUNT_PER_MODE),
                new Queues.BulkJobOptions().queueableConcurrent()
            )
            .size();
    }

    public static Integer enqueueInvocableJobs() {
        return Queues.of(INVOCABLE_QUEUE_NAME)
            .addBulk(buildJobs('invocable-', JOB_COUNT_PER_MODE), new Queues.BulkJobOptions().invocable())
            .size();
    }

    public static Integer upsertJobSchedulers() {
        return upsertJobSchedulers(0, resolveSchedulerCount());
    }

    public static Integer upsertJobSchedulers(final Integer startIndex, final Integer requestedCount) {
        Integer schedulerCount = Math.min(requestedCount, resolveSchedulerCount());
        Datetime firstRunAt = System.now().addMinutes(1);
        for (Integer offset = 0; offset < schedulerCount; offset++) {
            Integer index = startIndex + offset;
            SchedulerLane lane = schedulerLane(index);
            String suffix = index.toString().leftPad(3, '0');
            Queues.of(lane.queueName)
                .upsertJobScheduler(
                    SCHEDULER_NAME_PREFIX + suffix,
                    new Queues.RepeatOptions().every(1).startAt(firstRunAt).lim(2),
                    new Queues.JobTemplate('stress-scheduler-' + suffix, PROCESSOR_CLASS)
                        .data(
                            new Map<String, Object>{
                                'mode' => 'complete',
                                'payload' => 'stress-scheduler-' + suffix,
                                'delayMs' => PROCESSOR_DELAY_MS
                            }
                        )
                        .opts(new Queues.JobOptions().attempts(1)),
                    lane.executionMode
                );
        }
        return schedulerCount;
    }

    public static Set<String> queueNames() {
        Set<String> queueNames = new Set<String>(WORKER_QUEUE_NAMES);
        queueNames.add(QUEUEABLE_SERIAL_QUEUE_NAME);
        queueNames.add(QUEUEABLE_CONCURRENT_QUEUE_NAME);
        queueNames.add(INVOCABLE_QUEUE_NAME);
        return queueNames;
    }

    private static List<Queues.JobData> buildJobs(final String idPrefix, final Integer jobCount) {
        List<Queues.JobData> jobs = new List<Queues.JobData>();
        Integer delayedFromIndex = jobCount / 2;
        for (Integer index = 0; index < jobCount; index++) {
            String suffix = index.toString().leftPad(3, '0');
            Queues.JobOptions opts = new Queues.JobOptions().jobId(JOB_ID_PREFIX + idPrefix + suffix).attempts(1);
            if (index >= delayedFromIndex) {
                opts.delay(1);
            }
            jobs.add(
                new Queues.JobData(
                    'stress-' + idPrefix + suffix,
                    PROCESSOR_CLASS,
                    new Map<String, Object>{
                        'mode' => 'complete',
                        'payload' => 'stress-' +
                        idPrefix +
                        suffix,
                        'delayMs' => PROCESSOR_DELAY_MS
                    },
                    opts
                )
            );
        }
        return jobs;
    }

    private static Integer resolveSchedulerCount() {
        Integer occupiedSlots = [
            SELECT COUNT()
            FROM CronTrigger
            WHERE State NOT IN ('DELETED', 'COMPLETE', 'ERROR')
        ];
        return Math.min(
            MAX_SCHEDULER_COUNT,
            Math.max(0, SCHEDULED_APEX_LIMIT - occupiedSlots - SCHEDULED_APEX_RESERVE)
        );
    }

    private static SchedulerLane schedulerLane(final Integer index) {
        Integer laneIndex = Math.mod(index, 8);
        if (laneIndex < WORKER_QUEUE_NAMES.size()) {
            return new SchedulerLane(WORKER_QUEUE_NAMES[laneIndex], Queues.ExecutionMode.WORKER);
        }
        if (laneIndex == 5) {
            return new SchedulerLane(QUEUEABLE_SERIAL_QUEUE_NAME, Queues.ExecutionMode.QUEUEABLE_SERIAL);
        }
        if (laneIndex == 6) {
            return new SchedulerLane(QUEUEABLE_CONCURRENT_QUEUE_NAME, Queues.ExecutionMode.QUEUEABLE_CONCURRENT);
        }
        return new SchedulerLane(INVOCABLE_QUEUE_NAME, Queues.ExecutionMode.INVOCABLE);
    }

    private class SchedulerLane {
        private final String queueName;
        private final Queues.ExecutionMode executionMode;

        private SchedulerLane(final String queueName, final Queues.ExecutionMode executionMode) {
            this.queueName = queueName;
            this.executionMode = executionMode;
        }
    }
}
