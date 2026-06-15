@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesE2ERawPerformanceProbe {
    public static final String DEFAULT_RUN_KEY = 'raw-performance';
    public static final String STATUS_WAITING = 'WAITING';
    public static final String STATUS_COMPLETED = 'COMPLETED';
    public static final String STATUS_FAILED = 'FAILED';
    public static final String VARIANT_BATCH_ITERATOR = 'BATCH_ITERATOR';
    public static final String VARIANT_BATCH_CURSOR = 'BATCH_CURSOR';
    public static final String VARIANT_BATCH_SERIAL = 'BATCH_SERIAL';
    public static final String VARIANT_QUEUEABLE_SERIAL = 'QUEUEABLE_SERIAL';
    public static final String VARIANT_QUEUEABLE_FUTURE = 'QUEUEABLE_FUTURE';
    public static final String VARIANT_INVOCABLE = 'INVOCABLE';
    public static final Integer DEFAULT_JOB_COUNT = 500;
    public static final Integer DEFAULT_DELAY_MS = 200;
    public static final Integer BATCH_SCOPE_SIZE = 1;
    public static final Integer FUTURE_LAUNCHER_SIZE = 50;

    public static Id enqueueBatchIterator(final String runKey, final Integer jobCount, final Integer delayMs) {
        Datetime requestedAt = System.now();
        String requestedAtMs = requestedAt.getTime().toString();
        createRuns(runKey, VARIANT_BATCH_ITERATOR, jobCount, requestedAt);
        return Database.executeBatch(
            new QueuesE2ERawBatchIterator(runKey, requestedAtMs, normalizeDelayMs(delayMs)),
            BATCH_SCOPE_SIZE
        );
    }

    public static Id enqueueBatchCursor(final String runKey, final Integer jobCount, final Integer delayMs) {
        Datetime requestedAt = System.now();
        String requestedAtMs = requestedAt.getTime().toString();
        createRuns(runKey, VARIANT_BATCH_CURSOR, jobCount, requestedAt);
        return Database.executeBatch(
            new QueuesE2ERawBatchCursor(runKey, requestedAtMs, normalizeDelayMs(delayMs)),
            BATCH_SCOPE_SIZE
        );
    }

    public static Id enqueueBatchSerial(final String runKey, final Integer jobCount, final Integer delayMs) {
        Datetime requestedAt = System.now();
        String requestedAtMs = requestedAt.getTime().toString();
        createRuns(runKey, VARIANT_BATCH_SERIAL, jobCount, requestedAt);
        return Database.executeBatch(
            new QueuesE2ERawBatchSerial(runKey, requestedAtMs, normalizeDelayMs(delayMs)),
            BATCH_SCOPE_SIZE
        );
    }

    public static Id enqueueQueueableSerial(final String runKey, final Integer jobCount, final Integer delayMs) {
        Datetime requestedAt = System.now();
        String requestedAtMs = requestedAt.getTime().toString();
        createRuns(runKey, VARIANT_QUEUEABLE_SERIAL, jobCount, requestedAt);
        return System.enqueueJob(new QueuesE2ERawQueueableSerial(runKey, 0, requestedAtMs, normalizeDelayMs(delayMs)));
    }

    public static List<Id> enqueueQueueableConcurrentViaFuture(
        final String runKey,
        final Integer jobCount,
        final Integer delayMs
    ) {
        Datetime requestedAt = System.now();
        String requestedAtMs = requestedAt.getTime().toString();
        Integer count = normalizeJobCount(jobCount);
        createRuns(runKey, VARIANT_QUEUEABLE_FUTURE, count, requestedAt);

        List<Id> launcherIds = new List<Id>();
        for (Integer itemIndex = 0; itemIndex < count; itemIndex += FUTURE_LAUNCHER_SIZE) {
            launcherIds.add(
                System.enqueueJob(
                    new QueuesE2ERawQueueableFutureLauncher(
                        runKey,
                        itemIndex,
                        Math.min(FUTURE_LAUNCHER_SIZE, count - itemIndex),
                        requestedAtMs,
                        normalizeDelayMs(delayMs)
                    )
                )
            );
        }
        return launcherIds;
    }

    public static void enqueueInvocable(final String runKey, final Integer jobCount) {
        createRuns(runKey, VARIANT_INVOCABLE, jobCount, System.now());
    }

    public static void cleanup(final String runKey) {
        delete [
            SELECT Id
            FROM RawPerformanceRun__c
            WHERE RunKey__c = :runKey
        ];
    }

    public static void printSummary(final String runKey) {
        List<Object> summaries = new List<Object>();
        List<String> variants = new List<String>();
        Map<String, Integer> runsByVariant = new Map<String, Integer>();
        Map<String, Set<String>> asyncJobIdsByVariant = new Map<String, Set<String>>();
        Map<String, Datetime> firstStartByVariant = new Map<String, Datetime>();
        Map<String, Datetime> lastFinishByVariant = new Map<String, Datetime>();
        Map<String, Long> firstStartMsByVariant = new Map<String, Long>();
        Map<String, Long> lastFinishMsByVariant = new Map<String, Long>();
        Map<String, Decimal> sumStartDelayByVariant = new Map<String, Decimal>();
        Map<String, Decimal> maxStartDelayByVariant = new Map<String, Decimal>();
        Map<String, Integer> startDelayCountByVariant = new Map<String, Integer>();
        Map<String, Decimal> sumDurationByVariant = new Map<String, Decimal>();
        Map<String, Integer> durationCountByVariant = new Map<String, Integer>();

        for (RawPerformanceRun__c run : [
            SELECT
                Variant__c,
                StartedAt__c,
                StartedAtMs__c,
                FinishedAt__c,
                FinishedAtMs__c,
                StartDelayMs__c,
                DurationMs__c,
                AsyncApexJobId__c
            FROM RawPerformanceRun__c
            WHERE RunKey__c = :runKey
            ORDER BY Variant__c, ItemIndex__c
        ]) {
            String variant = run.Variant__c;
            if (!runsByVariant.containsKey(variant)) {
                variants.add(variant);
                runsByVariant.put(variant, 0);
                asyncJobIdsByVariant.put(variant, new Set<String>());
                sumStartDelayByVariant.put(variant, 0);
                startDelayCountByVariant.put(variant, 0);
                sumDurationByVariant.put(variant, 0);
                durationCountByVariant.put(variant, 0);
            }

            runsByVariant.put(variant, runsByVariant.get(variant) + 1);
            if (String.isNotBlank(run.AsyncApexJobId__c)) {
                asyncJobIdsByVariant.get(variant).add(run.AsyncApexJobId__c);
            }
            if (run.StartedAt__c != null) {
                Datetime firstStart = firstStartByVariant.get(variant);
                if (firstStart == null || run.StartedAt__c < firstStart) {
                    firstStartByVariant.put(variant, run.StartedAt__c);
                }
            }
            if (run.FinishedAt__c != null) {
                Datetime lastFinish = lastFinishByVariant.get(variant);
                if (lastFinish == null || run.FinishedAt__c > lastFinish) {
                    lastFinishByVariant.put(variant, run.FinishedAt__c);
                }
            }
            Long startedAtMs = toLong(run.StartedAtMs__c);
            if (startedAtMs != null) {
                Long firstStartMs = firstStartMsByVariant.get(variant);
                if (firstStartMs == null || startedAtMs < firstStartMs) {
                    firstStartMsByVariant.put(variant, startedAtMs);
                }
            }
            Long finishedAtMs = toLong(run.FinishedAtMs__c);
            if (finishedAtMs != null) {
                Long lastFinishMs = lastFinishMsByVariant.get(variant);
                if (lastFinishMs == null || finishedAtMs > lastFinishMs) {
                    lastFinishMsByVariant.put(variant, finishedAtMs);
                }
            }
            if (run.StartDelayMs__c != null) {
                sumStartDelayByVariant.put(variant, sumStartDelayByVariant.get(variant) + run.StartDelayMs__c);
                startDelayCountByVariant.put(variant, startDelayCountByVariant.get(variant) + 1);
                Decimal maxStartDelay = maxStartDelayByVariant.get(variant);
                if (maxStartDelay == null || run.StartDelayMs__c > maxStartDelay) {
                    maxStartDelayByVariant.put(variant, run.StartDelayMs__c);
                }
            }
            if (run.DurationMs__c != null) {
                sumDurationByVariant.put(variant, sumDurationByVariant.get(variant) + run.DurationMs__c);
                durationCountByVariant.put(variant, durationCountByVariant.get(variant) + 1);
            }
        }

        for (String variant : variants) {
            Datetime firstStart = firstStartByVariant.get(variant);
            Datetime lastFinish = lastFinishByVariant.get(variant);
            Long firstStartMs = firstStartMsByVariant.get(variant);
            Long lastFinishMs = lastFinishMsByVariant.get(variant);
            Decimal wallSeconds = firstStartMs != null &&
                lastFinishMs != null
                ? Decimal.valueOf((lastFinishMs - firstStartMs).toString()) / 1000
                : firstStart == null ||
                      lastFinish == null
                      ? null
                      : Decimal.valueOf((lastFinish.getTime() - firstStart.getTime()).toString()) / 1000;
            Decimal sumDurationMs = sumDurationByVariant.get(variant);
            Decimal effectiveConcurrency = wallSeconds == null ||
                wallSeconds <= 0
                ? null
                : (sumDurationMs / 1000) / wallSeconds;
            Integer startDelayCount = startDelayCountByVariant.get(variant);
            Integer durationCount = durationCountByVariant.get(variant);

            summaries.add(
                new Map<String, Object>{
                    'variant' => variant,
                    'runs' => runsByVariant.get(variant),
                    'asyncJobs' => asyncJobIdsByVariant.get(variant).size(),
                    'firstStart' => firstStart,
                    'firstStartMs' => firstStartMs,
                    'lastFinish' => lastFinish,
                    'lastFinishMs' => lastFinishMs,
                    'wallSeconds' => wallSeconds?.setScale(1),
                    'avgStartDelayMs' => startDelayCount == 0
                        ? null
                        : (sumStartDelayByVariant.get(variant) / startDelayCount).setScale(1),
                    'maxStartDelayMs' => maxStartDelayByVariant.get(variant),
                    'avgDurationMs' => durationCount == 0 ? null : (sumDurationMs / durationCount).setScale(1),
                    'sumDurationMs' => sumDurationMs,
                    'effectiveConcurrency' => effectiveConcurrency?.setScale(1)
                }
            );
        }

        System.debug(
            LoggingLevel.ERROR,
            JSON.serializePretty(
                new Map<String, Object>{
                    'feature' => 'performance.raw.summary',
                    'runKey' => runKey,
                    'summary' => summaries
                }
            )
        );
    }

    public static void printSpanPattern(final String runKey, final String variant) {
        List<RawPerformanceRun__c> runs = [
            SELECT ItemIndex__c, StartDelayMs__c, DurationMs__c, AsyncApexJobId__c
            FROM RawPerformanceRun__c
            WHERE RunKey__c = :runKey AND Variant__c = :variant
            ORDER BY ItemIndex__c
        ];

        Map<Integer, SpanBucket> bucketsBySecond = new Map<Integer, SpanBucket>();
        Set<String> asyncJobIds = new Set<String>();
        Long firstStartDelayMs;
        Long lastStartDelayMs;
        Long previousStartDelayMs;
        Long minGapMs;
        Long maxGapMs;
        Decimal sumGapMs = 0;
        Integer gapCount = 0;
        List<Long> firstGapsMs = new List<Long>();

        for (RawPerformanceRun__c run : runs) {
            Long startDelayMs = toLong(run.StartDelayMs__c);
            Long durationMs = toLong(run.DurationMs__c);
            Integer itemIndex = toInteger(run.ItemIndex__c);
            if (String.isNotBlank(run.AsyncApexJobId__c)) {
                asyncJobIds.add(run.AsyncApexJobId__c);
            }
            if (startDelayMs == null) {
                continue;
            }

            if (firstStartDelayMs == null) {
                firstStartDelayMs = startDelayMs;
            }
            lastStartDelayMs = startDelayMs;

            if (previousStartDelayMs != null) {
                Long gapMs = startDelayMs - previousStartDelayMs;
                sumGapMs += gapMs;
                gapCount++;
                if (minGapMs == null || gapMs < minGapMs) {
                    minGapMs = gapMs;
                }
                if (maxGapMs == null || gapMs > maxGapMs) {
                    maxGapMs = gapMs;
                }
                if (firstGapsMs.size() < 30) {
                    firstGapsMs.add(gapMs);
                }
            }
            previousStartDelayMs = startDelayMs;

            Integer bucketSecond = Integer.valueOf((startDelayMs / 1000).toString());
            if (!bucketsBySecond.containsKey(bucketSecond)) {
                bucketsBySecond.put(bucketSecond, new SpanBucket(bucketSecond));
            }
            bucketsBySecond.get(bucketSecond).add(itemIndex, startDelayMs, durationMs);
        }

        List<Integer> bucketSeconds = new List<Integer>(bucketsBySecond.keySet());
        bucketSeconds.sort();

        List<Object> bucketSamples = new List<Object>();
        for (Integer index = 0; index < bucketSeconds.size(); index++) {
            if (index >= 25 && index < bucketSeconds.size() - 10) {
                continue;
            }
            bucketSamples.add(bucketsBySecond.get(bucketSeconds[index]).toMap());
        }

        System.debug(
            LoggingLevel.ERROR,
            JSON.serializePretty(
                new Map<String, Object>{
                    'feature' => 'performance.raw.spanPattern',
                    'runKey' => runKey,
                    'variant' => variant,
                    'runs' => runs.size(),
                    'asyncJobs' => asyncJobIds.size(),
                    'firstStartDelayMs' => firstStartDelayMs,
                    'lastStartDelayMs' => lastStartDelayMs,
                    'startSpanMs' => firstStartDelayMs == null ? null : lastStartDelayMs - firstStartDelayMs,
                    'bucketSeconds' => bucketSeconds.size(),
                    'avgGapMs' => gapCount == 0 ? null : (sumGapMs / gapCount).setScale(1),
                    'minGapMs' => minGapMs,
                    'maxGapMs' => maxGapMs,
                    'firstGapsMs' => firstGapsMs,
                    'bucketSamples' => bucketSamples
                }
            )
        );
    }

    public static List<RawPerformanceRun__c> queryRuns(
        final String runKey,
        final String variant,
        final Integer itemIndex,
        final Integer rowLimit
    ) {
        Integer safeLimit = Math.max(1, rowLimit ?? 1);
        return [
            SELECT Id, ItemIndex__c
            FROM RawPerformanceRun__c
            WHERE RunKey__c = :runKey AND Variant__c = :variant AND ItemIndex__c >= :itemIndex
            ORDER BY ItemIndex__c
            LIMIT :safeLimit
        ];
    }

    public static Boolean hasRun(final String runKey, final String variant, final Integer itemIndex) {
        return [
                SELECT COUNT()
                FROM RawPerformanceRun__c
                WHERE RunKey__c = :runKey AND Variant__c = :variant AND ItemIndex__c = :itemIndex
            ] > 0;
    }

    public static void capture(
        final Id runId,
        final String requestedAtMsValue,
        final Integer delayMs,
        final String asyncApexJobId
    ) {
        Long requestedAtMs = toLong(requestedAtMsValue);
        Datetime startedAt = System.now();
        Long startedAtMs = startedAt.getTime();
        burnCpu(normalizeDelayMs(delayMs));
        Datetime finishedAt = System.now();
        Long finishedAtMs = finishedAt.getTime();

        update new RawPerformanceRun__c(
            Id = runId,
            Status__c = STATUS_COMPLETED,
            StartedAt__c = startedAt,
            StartedAtMs__c = startedAtMs.toString(),
            FinishedAt__c = finishedAt,
            FinishedAtMs__c = finishedAtMs.toString(),
            StartDelayMs__c = requestedAtMs == null ? null : toDecimal(startedAtMs - requestedAtMs),
            DurationMs__c = toDecimal(finishedAtMs - startedAtMs),
            AsyncApexJobId__c = asyncApexJobId
        );
    }

    public static void captureFailure(final Id runId, final String asyncApexJobId, final Exception ex) {
        Datetime finishedAt = System.now();
        update new RawPerformanceRun__c(
            Id = runId,
            Status__c = STATUS_FAILED,
            FinishedAt__c = finishedAt,
            FinishedAtMs__c = finishedAt.getTime().toString(),
            AsyncApexJobId__c = asyncApexJobId,
            Message__c = ex?.getMessage()
        );
    }

    @SuppressWarnings('PMD.AvoidFutureAnnotation')
    @Future
    public static void enqueueConcurrentQueueableFromFuture(
        final String runId,
        final String requestedAtMs,
        final Integer delayMs
    ) {
        if (String.isBlank(runId)) {
            return;
        }

        Id rawRunId = runId;
        System.enqueueJob(new QueuesE2ERawQueueableConcurrent(rawRunId, requestedAtMs, normalizeDelayMs(delayMs)));
    }

    @SuppressWarnings('PMD.AvoidFutureAnnotation')
    @Future
    public static void enqueueSerialQueueableFromFuture(
        final String runKey,
        final Integer itemIndex,
        final String requestedAtMs,
        final Integer delayMs
    ) {
        if (String.isBlank(runKey)) {
            return;
        }

        System.enqueueJob(new QueuesE2ERawQueueableSerial(runKey, itemIndex, requestedAtMs, normalizeDelayMs(delayMs)));
    }

    private static void createRuns(
        final String runKey,
        final String variant,
        final Integer jobCount,
        final Datetime requestedAt
    ) {
        List<RawPerformanceRun__c> runs = new List<RawPerformanceRun__c>();
        Long requestedAtMs = requestedAt.getTime();
        for (Integer itemIndex = 0; itemIndex < normalizeJobCount(jobCount); itemIndex++) {
            runs.add(
                new RawPerformanceRun__c(
                    RunKey__c = runKey,
                    Variant__c = variant,
                    ItemIndex__c = itemIndex,
                    RequestedAt__c = requestedAt,
                    RequestedAtMs__c = requestedAtMs.toString(),
                    Status__c = STATUS_WAITING
                )
            );
        }
        insert runs;
    }

    private static void burnCpu(final Integer delayMs) {
        if (delayMs <= 0) {
            return;
        }

        Long startedAtMs = System.now().getTime();
        while (System.now().getTime() - startedAtMs < delayMs) {
        }
    }

    private static Integer normalizeJobCount(final Integer jobCount) {
        return Math.max(0, jobCount ?? DEFAULT_JOB_COUNT);
    }

    private static Integer normalizeDelayMs(final Integer delayMs) {
        return Math.max(0, Math.min(delayMs ?? DEFAULT_DELAY_MS, 1000));
    }

    private static Long toLong(final String value) {
        return String.isBlank(value) ? null : Long.valueOf(value);
    }

    private static Long toLong(final Decimal value) {
        String longValue = value?.toString();
        return String.isBlank(longValue) ? null : Long.valueOf(longValue);
    }

    private static Integer toInteger(final Decimal value) {
        String integerValue = value?.toString();
        return String.isBlank(integerValue) ? null : Integer.valueOf(integerValue);
    }

    private static Decimal toDecimal(final Long value) {
        String decimalValue = value?.toString();
        return String.isBlank(decimalValue) ? null : Decimal.valueOf(decimalValue);
    }

    private class SpanBucket {
        private Integer second;
        private Integer count = 0;
        private Integer firstIndex;
        private Integer lastIndex;
        private Long firstStartDelayMs;
        private Long lastStartDelayMs;
        private Decimal sumDurationMs = 0;

        private SpanBucket(final Integer second) {
            this.second = second;
        }

        private void add(final Integer itemIndex, final Long startDelayMs, final Long durationMs) {
            count++;
            if (firstIndex == null) {
                firstIndex = itemIndex;
                firstStartDelayMs = startDelayMs;
            }
            lastIndex = itemIndex;
            lastStartDelayMs = startDelayMs;
            if (durationMs != null) {
                sumDurationMs += durationMs;
            }
        }

        private Map<String, Object> toMap() {
            return new Map<String, Object>{
                'second' => second,
                'count' => count,
                'firstIndex' => firstIndex,
                'lastIndex' => lastIndex,
                'firstStartDelayMs' => firstStartDelayMs,
                'lastStartDelayMs' => lastStartDelayMs,
                'avgDurationMs' => count == 0 ? null : (sumDurationMs / count).setScale(1)
            };
        }
    }
}
