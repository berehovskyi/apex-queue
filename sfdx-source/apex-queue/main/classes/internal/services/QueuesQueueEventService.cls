/**
 * @description Maps framework lifecycle moments to optional QueueEvent platform events.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesQueueEventService {
    private static final String ALL_TOKEN = 'ALL';
    private static final String TYPE_SEPARATOR = ';';
    private static final String QUEUE_EVENTS_COMPONENT = 'queue events';
    private static final String UNSUPPORTED_EVENT_TYPE_MESSAGE = 'Queue "{0}" has unsupported PublishedEventTypes__c token "{1}".';
    private static final Integer REASON_MAX_LENGTH = 32768;
    private static final Set<QueueEventType> SUPPORTED_TYPES = new Set<QueueEventType>();
    private static final Map<String, QueueEventType> TYPES_BY_NAME = new Map<String, QueueEventType>();
    @TestVisible
    private static List<QueueEvent__e> scopedEvents = new List<QueueEvent__e>();
    @TestVisible
    private static List<Integer> scopeStartIndexes = new List<Integer>();

    static {
        for (QueueEventType typeValue : QueueEventType.values()) {
            SUPPORTED_TYPES.add(typeValue);
            TYPES_BY_NAME.put(typeValue.name(), typeValue);
        }
    }

    private QueuesDefinitionService definitionService = new QueuesDefinitionService();
    private QueuesErrorService errorService = new QueuesErrorService();
    private QueuesQueueEventRepository eventRepository = new QueuesQueueEventRepository(System.AccessLevel.SYSTEM_MODE);

    public QueuesQueueEventService() {
    }

    @TestVisible
    private QueuesQueueEventService(
        final QueuesDefinitionService definitionService,
        final QueuesErrorService errorService,
        final QueuesQueueEventRepository eventRepository
    ) {
        this.definitionService = definitionService;
        this.errorService = errorService;
        this.eventRepository = eventRepository;
    }

    // <editor-fold desc="Job Events">
    public void publishTerminalJobEvents(final List<Job__c> jobs, final List<JobRun__c> runs) {
        publishTerminalJobEvents(jobs, runs, null);
    }

    public void publishTerminalJobEvents(final List<Job__c> jobs, final List<JobRun__c> runs, final String component) {
        publishJobEvents(resolveTerminalJobEvents(jobs), runs, component);
    }

    public void publishCanceledJobEvents(final List<Job__c> jobs) {
        publishJobEvents(resolveJobEvents(jobs, QueueEventType.CANCELED), null, null);
    }

    public void publishActiveJobEvents(final List<Job__c> jobs) {
        publishJobEvents(resolveJobEvents(jobs, QueueEventType.ACTIVE), null, null);
    }

    public void publishRecoveredJobEvents(final List<Job__c> jobs, final List<JobRun__c> runs, final String component) {
        publishJobEvents(resolveJobEvents(jobs, QueueEventType.RECOVERED), runs, component);
    }

    public void publishStallRecoveryJobEvents(
        final List<Job__c> jobs,
        final List<JobRun__c> runs,
        final String component
    ) {
        List<JobEventCandidate> candidates = new List<JobEventCandidate>();
        for (Job__c job : jobs ?? new List<Job__c>()) {
            Queues.State state = QueuesSupport.toState(job.State__c);
            if (state == Queues.State.WAITING || state == Queues.State.DELAYED) {
                candidates.add(new JobEventCandidate(job, QueueEventType.RECOVERED));
            } else if (state == Queues.State.FAILED) {
                candidates.add(new JobEventCandidate(job, QueueEventType.STALLED));
                candidates.add(new JobEventCandidate(job, QueueEventType.FAILED));
            }
        }
        publishJobEvents(candidates, runs, component);
    }
    // </editor-fold>

    // <editor-fold desc="Savepoint Scopes">
    public void beginScope() {
        scopeStartIndexes.add(scopedEvents.size());
    }

    public void commitScope() {
        if (scopeStartIndexes.isEmpty()) {
            return;
        }

        scopeStartIndexes.remove(scopeStartIndexes.size() - 1);
        if (!scopeStartIndexes.isEmpty()) {
            return;
        }

        List<QueueEvent__e> events = scopedEvents;
        scopedEvents = new List<QueueEvent__e>();
        publishEventsImmediately(null, events);
    }

    public void abandonScope() {
        if (scopeStartIndexes.isEmpty()) {
            return;
        }

        Integer startIndex = scopeStartIndexes.remove(scopeStartIndexes.size() - 1);
        while (scopedEvents.size() > startIndex) {
            scopedEvents.remove(scopedEvents.size() - 1);
        }
    }
    // </editor-fold>

    // <editor-fold desc="Aggregate Events">
    public void publishSchedulerMaterialized(final JobScheduler__c scheduler, final Job__c job) {
        if (scheduler == null || job == null) {
            return;
        }

        QueueDefinition__mdt definition = findDefinition(job.QueueName__c);
        Set<QueueEventType> publishedTypes = resolvePublishedTypes(definition);
        if (!publishedTypes.contains(QueueEventType.SCHEDULER_MATERIALIZED)) {
            return;
        }

        QueueEvent__e eventRecord = mapJobEvent(
            QueueEventType.SCHEDULER_MATERIALIZED,
            job,
            null,
            null,
            QueuesSupport.now()
        );
        eventRecord.JobSchedulerId__c = scheduler.Id;
        publishEvents(job.QueueName__c, new List<QueueEvent__e>{ eventRecord });
    }

    public void publishRecovered(final String queueName, final String component, final Integer count) {
        publishAggregateEvent(QueueEventType.RECOVERED, queueName, component, count);
    }

    public void publishCleaned(final String queueName, final String component, final Integer count) {
        publishAggregateEvent(QueueEventType.CLEANED, queueName, component, count);
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private static List<JobEventCandidate> resolveTerminalJobEvents(final List<Job__c> jobs) {
        List<JobEventCandidate> candidates = new List<JobEventCandidate>();
        for (Job__c job : jobs ?? new List<Job__c>()) {
            Queues.State state = QueuesSupport.toState(job.State__c);
            if (state == Queues.State.COMPLETED) {
                candidates.add(new JobEventCandidate(job, QueueEventType.COMPLETED));
            } else if (state == Queues.State.FAILED) {
                candidates.add(new JobEventCandidate(job, QueueEventType.FAILED));
            } else if (state == Queues.State.CANCELED) {
                candidates.add(new JobEventCandidate(job, QueueEventType.CANCELED));
            }
        }
        return candidates;
    }

    private static List<JobEventCandidate> resolveJobEvents(final List<Job__c> jobs, final QueueEventType typeValue) {
        List<JobEventCandidate> candidates = new List<JobEventCandidate>();
        for (Job__c job : jobs ?? new List<Job__c>()) {
            if (job?.Id != null) {
                candidates.add(new JobEventCandidate(job, typeValue));
            }
        }
        return candidates;
    }

    private static QueueEvent__e mapJobEvent(
        final QueueEventType typeValue,
        final Job__c job,
        final JobRun__c run,
        final String component,
        final Datetime occurredAt
    ) {
        QueueEvent__e eventRecord = newEvent(typeValue, job.QueueName__c, occurredAt);
        eventRecord.JobId__c = job.Id;
        eventRecord.JobRunId__c = run?.Id;
        eventRecord.JobSchedulerId__c = job.JobScheduler__c;
        eventRecord.ExternalId__c = job.ExternalId__c;
        eventRecord.JobName__c = job.Name__c;
        eventRecord.State__c = job.State__c;
        eventRecord.ExecutionMode__c = job.ExecutionMode__c;
        eventRecord.ProcessorClass__c = job.ProcessorClass__c;
        eventRecord.AttemptNumber__c = job.AttemptsMade__c;
        eventRecord.Reason__c = QueuesSupport.truncate(job.FailedReason__c, REASON_MAX_LENGTH);
        eventRecord.Category__c = job.Category__c;
        eventRecord.GroupKey__c = job.GroupKey__c;
        eventRecord.CorrelationId__c = job.CorrelationId__c;
        eventRecord.Component__c = component;
        return eventRecord;
    }

    private static QueueEvent__e newEvent(
        final QueueEventType typeValue,
        final String queueName,
        final Datetime occurredAt
    ) {
        QueueEvent__e eventRecord = (QueueEvent__e) QueueEvent__e.SObjectType.newSObject(null, true);
        eventRecord.Type__c = typeValue.name();
        eventRecord.QueueName__c = queueName;
        eventRecord.OccurredAt__c = occurredAt ?? QueuesSupport.now();
        return eventRecord;
    }

    private static Set<String> resolveErrorQueueNames(final String queueName, final List<QueueEvent__e> events) {
        Set<String> queueNames = new Set<String>();
        if (String.isNotBlank(queueName)) {
            queueNames.add(queueName);
            return queueNames;
        }

        for (QueueEvent__e eventRecord : events ?? new List<QueueEvent__e>()) {
            if (String.isNotBlank(eventRecord.QueueName__c)) {
                queueNames.add(eventRecord.QueueName__c);
            }
        }
        return queueNames;
    }

    private static Map<Id, JobRun__c> mapRunsByJobId(final List<JobRun__c> runs) {
        Map<Id, JobRun__c> runsByJobId = new Map<Id, JobRun__c>();
        for (JobRun__c run : runs ?? new List<JobRun__c>()) {
            if (run.Job__c != null) {
                runsByJobId.put(run.Job__c, run);
            }
        }
        return runsByJobId;
    }

    private static Set<QueueEventType> supportedTypes() {
        Set<QueueEventType> types = new Set<QueueEventType>();
        types.addAll(SUPPORTED_TYPES);
        return types;
    }

    @TestVisible
    private Set<QueueEventType> resolvePublishedTypes(final QueueDefinition__mdt definition) {
        String queueName = definition?.QueueName__c;
        String rawTypes = definition?.PublishedEventTypes__c;
        if (String.isBlank(rawTypes)) {
            return new Set<QueueEventType>();
        }

        Set<QueueEventType> types = new Set<QueueEventType>();
        Boolean publishesAll = false;
        for (String token : rawTypes.split(TYPE_SEPARATOR)) {
            String typeName = QueuesSupport.normalizeEnumValue(token);
            if (String.isBlank(typeName)) {
                continue;
            }
            if (typeName == ALL_TOKEN) {
                publishesAll = true;
                continue;
            }
            QueueEventType typeValue = TYPES_BY_NAME.get(typeName);
            if (typeValue == null) {
                recordEventingError(
                    queueName,
                    new Queues.ConfigurationException(
                        String.format(UNSUPPORTED_EVENT_TYPE_MESSAGE, new List<String>{ queueName, token })
                    )
                );
                continue;
            }
            types.add(typeValue);
        }
        return publishesAll ? supportedTypes() : types;
    }

    private void publishJobEvents(
        final List<JobEventCandidate> candidates,
        final List<JobRun__c> runs,
        final String component
    ) {
        if (candidates == null || candidates.isEmpty()) {
            return;
        }

        Map<String, List<JobEventCandidate>> candidatesByQueueName = new Map<String, List<JobEventCandidate>>();
        Set<String> queueNames = new Set<String>();
        for (JobEventCandidate candidate : candidates) {
            String queueName = candidate?.job?.QueueName__c;
            if (String.isBlank(queueName)) {
                continue;
            }
            queueNames.add(queueName);
            List<JobEventCandidate> queueCandidates = candidatesByQueueName.get(queueName) ??
                new List<JobEventCandidate>();
            queueCandidates.add(candidate);
            candidatesByQueueName.put(queueName, queueCandidates);
        }
        if (queueNames.isEmpty()) {
            return;
        }

        Map<Id, JobRun__c> runsByJobId = mapRunsByJobId(runs);
        Map<String, QueueDefinition__mdt> definitionsByQueueName = findDefinitions(queueNames);
        Datetime occurredAt = QueuesSupport.now();
        List<QueueEvent__e> events = new List<QueueEvent__e>();
        for (String queueName : queueNames) {
            Set<QueueEventType> publishedTypes = resolvePublishedTypes(definitionsByQueueName.get(queueName));
            if (publishedTypes.isEmpty()) {
                continue;
            }
            for (JobEventCandidate candidate : candidatesByQueueName.get(queueName)) {
                if (!publishedTypes.contains(candidate.typeValue)) {
                    continue;
                }
                events.add(
                    mapJobEvent(
                        candidate.typeValue,
                        candidate.job,
                        runsByJobId.get(candidate.job.Id),
                        component,
                        occurredAt
                    )
                );
            }
        }
        publishEvents(null, events);
    }

    private void publishAggregateEvent(
        final QueueEventType typeValue,
        final String queueName,
        final String component,
        final Integer count
    ) {
        if (String.isBlank(queueName) || QueuesSupport.toInteger(count, 0) <= 0) {
            return;
        }

        QueueDefinition__mdt definition = findDefinition(queueName);
        Set<QueueEventType> publishedTypes = resolvePublishedTypes(definition);
        if (!publishedTypes.contains(typeValue)) {
            return;
        }

        QueueEvent__e eventRecord = newEvent(typeValue, queueName, QueuesSupport.now());
        eventRecord.Component__c = component;
        eventRecord.JobCount__c = count;
        publishEvents(queueName, new List<QueueEvent__e>{ eventRecord });
    }

    private Map<String, QueueDefinition__mdt> findDefinitions(final Set<String> queueNames) {
        try {
            return definitionService.findQueueDefinitionsByQueueNamesAsSystem(queueNames);
        } catch (Exception ex) {
            for (String queueName : queueNames ?? new Set<String>()) {
                recordEventingError(queueName, ex);
            }
            return new Map<String, QueueDefinition__mdt>();
        }
    }

    private QueueDefinition__mdt findDefinition(final String queueName) {
        try {
            return definitionService.findQueueDefinitionAsSystem(queueName);
        } catch (Exception ex) {
            recordEventingError(queueName, ex);
            return null;
        }
    }

    private void publishEvents(final String queueName, final List<QueueEvent__e> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        if (!scopeStartIndexes.isEmpty()) {
            scopedEvents.addAll(events);
            return;
        }

        publishEventsImmediately(queueName, events);
    }

    private void publishEventsImmediately(final String queueName, final List<QueueEvent__e> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        try {
            eventRepository.publishQueueEvents(events);
        } catch (Exception ex) {
            for (String errorQueueName : resolveErrorQueueNames(queueName, events)) {
                recordEventingError(errorQueueName, ex);
            }
        }
    }

    @TestVisible
    private void recordEventingError(final String queueName, final Exception ex) {
        if (String.isBlank(queueName)) {
            return;
        }

        try {
            errorService.recordQueueError(queueName, QUEUE_EVENTS_COMPONENT, ex);
        } catch (Exception loggingEx) {
            QueuesSupport.debugError(
                String.format(
                    'Failed to record queue event error for {0}: {1}\n{2}',
                    new List<String>{ queueName, loggingEx.getMessage(), loggingEx.getStackTraceString() }
                )
            );
        }
    }
    // </editor-fold>

    private enum QueueEventType {
        ACTIVE,
        COMPLETED,
        FAILED,
        CANCELED,
        STALLED,
        RECOVERED,
        SCHEDULER_MATERIALIZED,
        CLEANED
    }

    private class JobEventCandidate {
        private final Job__c job;
        private final QueueEventType typeValue;

        private JobEventCandidate(final Job__c job, final QueueEventType typeValue) {
            this.job = job;
            this.typeValue = typeValue;
        }
    }
}
