@IsTest(IsParallel=true)
private class QueuesQueueEventServiceTest {
    @IsTest
    static void should_publish_enabled_terminal_job_events_in_one_batch() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('COMPLETED;FAILED', eventProvider, null);
        List<Job__c> jobs = new List<Job__c>();
        List<JobRun__c> runs = new List<JobRun__c>();
        for (Integer index = 0; index < 200; index++) {
            Job__c job = job(Queues.State.COMPLETED, index + 1);
            jobs.add(job);
            runs.add(jobRun(job.Id, index + 1));
        }

        // Act
        service.publishTerminalJobEvents(jobs, runs);

        // Assert
        Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
        List<QueueEvent__e> events = (List<QueueEvent__e>) eventProvider.getLastArgs('publishQueueEvents')[0];
        Assert.areEqual(200, events.size());
        Assert.areEqual('COMPLETED', events[0].Type__c);
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, events[0].QueueName__c);
        Assert.areEqual(String.valueOf(jobs[0].Id), events[0].JobId__c);
        Assert.areEqual(String.valueOf(runs[0].Id), events[0].JobRunId__c);
        Assert.areEqual(jobs[0].ExternalId__c, events[0].ExternalId__c);
        Assert.areEqual(jobs[0].Name__c, events[0].JobName__c);
        Assert.areEqual(jobs[0].State__c, events[0].State__c);
        Assert.areEqual(jobs[0].ExecutionMode__c, events[0].ExecutionMode__c);
        Assert.areEqual(jobs[0].ProcessorClass__c, events[0].ProcessorClass__c);
        Assert.areEqual(jobs[0].AttemptsMade__c, events[0].AttemptNumber__c);
        Assert.areEqual(jobs[0].Category__c, events[0].Category__c);
        Assert.areEqual(jobs[0].GroupKey__c, events[0].GroupKey__c);
        Assert.areEqual(jobs[0].CorrelationId__c, events[0].CorrelationId__c);
        Assert.isTrue(String.isNotBlank(events[0].EventUuid));
        Assert.areNotEqual(null, events[0].OccurredAt__c);
    }

    @IsTest
    static void should_skip_disabled_or_filtered_event_types() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider disabledEventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService disabledService = newService((String) null, disabledEventProvider, null);
        QueuesTestSupport.RecordingStubProvider failedOnlyEventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService failedOnlyService = newService('FAILED', failedOnlyEventProvider, null);

        // Act
        disabledService.publishTerminalJobEvents(new List<Job__c>{ job(Queues.State.FAILED, 301) }, null);
        failedOnlyService.publishTerminalJobEvents(new List<Job__c>{ job(Queues.State.COMPLETED, 302) }, null);

        // Assert
        Assert.areEqual(0, disabledEventProvider.getCallCount('publishQueueEvents'));
        Assert.areEqual(0, failedOnlyEventProvider.getCallCount('publishQueueEvents'));
    }

    @IsTest
    static void should_publish_active_job_event_when_enabled() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('ACTIVE', eventProvider, null);
        Job__c activeJob = job(Queues.State.ACTIVE, 325);

        // Act
        service.publishActiveJobEvents(new List<Job__c>{ activeJob });

        // Assert
        Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
        List<QueueEvent__e> events = (List<QueueEvent__e>) eventProvider.getLastArgs('publishQueueEvents')[0];
        Assert.areEqual(1, events.size());
        Assert.areEqual('ACTIVE', events[0].Type__c);
        Assert.areEqual(String.valueOf(activeJob.Id), events[0].JobId__c);
        Assert.areEqual(Queues.State.ACTIVE.name(), events[0].State__c);
    }

    @IsTest
    static void should_ignore_blank_tokens_and_blank_job_queue_names() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService(' ; COMPLETED', eventProvider, null);
        Job__c namelessQueueJob = job(Queues.State.COMPLETED, 351);
        namelessQueueJob.QueueName__c = null;

        // Act
        service.publishTerminalJobEvents(new List<Job__c>{ job(Queues.State.COMPLETED, 350) }, null);
        service.publishTerminalJobEvents(new List<Job__c>{ namelessQueueJob }, null);

        // Assert
        Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
    }

    @IsTest
    static void should_record_invalid_event_type_without_throwing() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider errorEventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('FAILD', eventProvider, errorEventProvider);

        // Act
        service.publishTerminalJobEvents(new List<Job__c>{ job(Queues.State.FAILED, 401) }, null);

        // Assert
        Assert.areEqual(0, eventProvider.getCallCount('publishQueueEvents'));
        Assert.areEqual(1, errorEventProvider.getCallCount('publishQueueError'));
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) errorEventProvider.getLastArgs('publishQueueError')[0];
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, errorEvent.QueueName__c);
        Assert.areEqual('queue events', errorEvent.Component__c);
        Assert.isTrue(errorEvent.Message__c.contains('PublishedEventTypes__c'));
    }

    @IsTest
    static void should_skip_eventing_errors_without_queue_name() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider errorEventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('ALL', eventProvider, errorEventProvider);

        // Act
        service.recordEventingError(null, new Queues.QueueException('missing queue name'));

        // Assert
        Assert.areEqual(0, eventProvider.getCallCount('publishQueueEvents'));
        Assert.areEqual(0, errorEventProvider.getCallCount('publishQueueError'));
    }

    @IsTest
    static void should_swallow_eventing_error_logging_failures() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider errorEventProvider = new QueuesTestSupport.RecordingStubProvider()
            .withException('publishQueueError', new Queues.QueueException('error telemetry exploded'));
        QueuesQueueEventService service = newService('NOPE', eventProvider, errorEventProvider);

        // Act
        service.publishTerminalJobEvents(new List<Job__c>{ job(Queues.State.FAILED, 451) }, null);

        // Assert
        Assert.areEqual(0, eventProvider.getCallCount('publishQueueEvents'));
        Assert.areEqual(1, errorEventProvider.getCallCount('publishQueueError'));
    }

    @IsTest
    static void should_record_definition_lookup_failures_without_throwing() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider errorEventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider definitionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withException('queryQueueDefinitionsByQueueNames', new Queues.QueueException('bulk definition failed'))
            .withException('queryQueueDefinition', new Queues.QueueException('single definition failed'));
        QueuesQueueEventService service = newService(definitionProvider, eventProvider, errorEventProvider);

        // Act
        service.publishTerminalJobEvents(new List<Job__c>{ job(Queues.State.FAILED, 452) }, null);
        service.publishRecovered(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'dependency repair', 1);

        // Assert
        Assert.areEqual(0, eventProvider.getCallCount('publishQueueEvents'));
        Assert.areEqual(2, errorEventProvider.getCallCount('publishQueueError'));
    }

    @IsTest
    static void should_record_publish_failure_without_throwing() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider()
            .withException('publishQueueEvents', new Queues.QueueException('Event bus exploded.'));
        QueuesTestSupport.RecordingStubProvider errorEventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('ALL', eventProvider, errorEventProvider);

        // Act
        service.publishTerminalJobEvents(new List<Job__c>{ job(Queues.State.COMPLETED, 501) }, null);

        // Assert
        Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
        Assert.areEqual(1, errorEventProvider.getCallCount('publishQueueError'));
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) errorEventProvider.getLastArgs('publishQueueError')[0];
        Assert.areEqual('Event bus exploded.', errorEvent.Message__c);
    }

    @IsTest
    static void should_record_aggregate_publish_failure_for_queue_name() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider()
            .withException('publishQueueEvents', new Queues.QueueException('Aggregate event exploded.'));
        QueuesTestSupport.RecordingStubProvider errorEventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('CLEANED', eventProvider, errorEventProvider);

        // Act
        service.publishCleaned(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'terminal cleanup', 2);

        // Assert
        Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
        Assert.areEqual(1, errorEventProvider.getCallCount('publishQueueError'));
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) errorEventProvider.getLastArgs('publishQueueError')[0];
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, errorEvent.QueueName__c);
    }

    @IsTest
    static void should_skip_invalid_aggregate_events() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('RECOVERED', eventProvider, null);

        // Act
        service.publishRecovered(null, 'dependency repair', 1);
        service.publishRecovered(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'dependency repair', 0);
        service.publishCleaned(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'terminal cleanup', 1);

        // Assert
        Assert.areEqual(0, eventProvider.getCallCount('publishQueueEvents'));
    }

    @IsTest
    static void should_publish_per_job_and_aggregate_recovered_shapes() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('RECOVERED', eventProvider, null);
        Job__c recoveredJob = job(Queues.State.WAITING, 601);

        // Act
        service.publishRecoveredJobEvents(new List<Job__c>{ recoveredJob }, null, 'queueable handoff recovery');
        service.publishRecovered(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'dependency repair', 3);

        // Assert
        Assert.areEqual(2, eventProvider.getCallCount('publishQueueEvents'));
        List<QueueEvent__e> perJobEvents = (List<QueueEvent__e>) eventProvider.getCalls('publishQueueEvents')[0][0];
        List<QueueEvent__e> aggregateEvents = (List<QueueEvent__e>) eventProvider.getCalls('publishQueueEvents')[1][0];
        Assert.areEqual(String.valueOf(recoveredJob.Id), perJobEvents[0].JobId__c);
        Assert.areEqual(null, perJobEvents[0].JobCount__c);
        Assert.areEqual('queueable handoff recovery', perJobEvents[0].Component__c);
        Assert.areEqual(null, aggregateEvents[0].JobId__c);
        Assert.areEqual(3, aggregateEvents[0].JobCount__c);
        Assert.areEqual('dependency repair', aggregateEvents[0].Component__c);
    }

    @IsTest
    static void should_publish_stalled_when_recovery_exhausts_attempts() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('STALLED;FAILED;RECOVERED', eventProvider, null);
        Job__c stalledJob = job(Queues.State.FAILED, 701);

        // Act
        service.publishStallRecoveryJobEvents(new List<Job__c>{ stalledJob }, null, 'stalled job recovery');

        // Assert
        List<QueueEvent__e> events = (List<QueueEvent__e>) eventProvider.getLastArgs('publishQueueEvents')[0];
        Assert.areEqual(2, events.size());
        Assert.areEqual('STALLED', events[0].Type__c);
        Assert.areEqual('FAILED', events[1].Type__c);
        Assert.areEqual('stalled job recovery', events[0].Component__c);
        Assert.areEqual('stalled job recovery', events[1].Component__c);
    }

    @IsTest
    static void should_skip_null_scheduler_materialization_inputs() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('SCHEDULER_MATERIALIZED', eventProvider, null);

        // Act
        service.publishSchedulerMaterialized(null, job(Queues.State.WAITING, 751));
        service.publishSchedulerMaterialized(
            QueuesTestSupport.jobScheduler(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'event-test'),
            null
        );

        // Assert
        Assert.areEqual(0, eventProvider.getCallCount('publishQueueEvents'));
    }

    @IsTest
    static void should_buffer_events_until_scope_commits() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('COMPLETED', eventProvider, null);

        // Act
        service.beginScope();
        service.publishTerminalJobEvents(new List<Job__c>{ job(Queues.State.COMPLETED, 801) }, null);

        // Assert
        Assert.areEqual(0, eventProvider.getCallCount('publishQueueEvents'));

        // Act
        service.commitScope();

        // Assert
        Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
        List<QueueEvent__e> events = (List<QueueEvent__e>) eventProvider.getLastArgs('publishQueueEvents')[0];
        Assert.areEqual(1, events.size());
        Assert.areEqual('COMPLETED', events[0].Type__c);
    }

    @IsTest
    static void should_noop_scope_calls_without_open_scope() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('COMPLETED', eventProvider, null);

        // Act
        service.commitScope();
        service.abandonScope();

        // Assert
        Assert.areEqual(0, eventProvider.getCallCount('publishQueueEvents'));
    }

    @IsTest
    static void should_keep_nested_scope_buffered_until_outer_commit() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('COMPLETED', eventProvider, null);

        // Act
        service.beginScope();
        service.beginScope();
        service.publishTerminalJobEvents(new List<Job__c>{ job(Queues.State.COMPLETED, 851) }, null);
        service.commitScope();

        // Assert
        Assert.areEqual(0, eventProvider.getCallCount('publishQueueEvents'));

        // Act
        service.commitScope();

        // Assert
        Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
    }

    @IsTest
    static void should_abandon_only_events_buffered_in_rolled_back_scope() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventService service = newService('COMPLETED;FAILED', eventProvider, null);

        // Act
        service.beginScope();
        service.publishTerminalJobEvents(new List<Job__c>{ job(Queues.State.COMPLETED, 901) }, null);
        service.beginScope();
        service.publishTerminalJobEvents(new List<Job__c>{ job(Queues.State.FAILED, 902) }, null);
        service.abandonScope();
        service.commitScope();

        // Assert
        Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
        List<QueueEvent__e> events = (List<QueueEvent__e>) eventProvider.getLastArgs('publishQueueEvents')[0];
        Assert.areEqual(1, events.size());
        Assert.areEqual('COMPLETED', events[0].Type__c);
    }

    private static QueuesQueueEventService newService(
        final QueuesTestSupport.RecordingStubProvider definitionProvider,
        final QueuesTestSupport.RecordingStubProvider eventProvider,
        final QueuesTestSupport.RecordingStubProvider errorEventProvider
    ) {
        QueuesDefinitionService definitionService = new QueuesDefinitionService(
            (QueuesDefinitionRepository) Test.createStub(QueuesDefinitionRepository.class, definitionProvider)
        );
        return newService(definitionService, eventProvider, errorEventProvider);
    }

    private static QueuesQueueEventService newService(
        final String publishedEventTypes,
        final QueuesTestSupport.RecordingStubProvider eventProvider,
        final QueuesTestSupport.RecordingStubProvider errorEventProvider
    ) {
        QueueDefinition__mdt definition = QueuesTestSupport.queueDefinition();
        definition.PublishedEventTypes__c = publishedEventTypes;
        QueuesDefinitionService definitionService = new QueuesDefinitionService(
            (QueuesDefinitionRepository) Test.createStub(
                QueuesDefinitionRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ definition })
                    .withReturn('queryQueueDefinitionsByQueueNames', new List<QueueDefinition__mdt>{ definition })
            )
        );
        return newService(definitionService, eventProvider, errorEventProvider);
    }

    private static QueuesQueueEventService newService(
        final QueuesDefinitionService definitionService,
        final QueuesTestSupport.RecordingStubProvider eventProvider,
        final QueuesTestSupport.RecordingStubProvider errorEventProvider
    ) {
        QueuesErrorEventRepository errorEventRepository = errorEventProvider == null
            ? new QueuesErrorEventRepository()
            : (QueuesErrorEventRepository) Test.createStub(QueuesErrorEventRepository.class, errorEventProvider);
        QueuesErrorService errorService = new QueuesErrorService(new QueuesErrorRepository(), errorEventRepository);
        QueuesQueueEventRepository eventRepository = (QueuesQueueEventRepository) Test.createStub(
            QueuesQueueEventRepository.class,
            eventProvider
        );

        return new QueuesQueueEventService(definitionService, errorService, eventRepository);
    }

    private static Job__c job(final Queues.State state, final Integer sequence) {
        Job__c job = QueuesTestSupport.job(state.name());
        job.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, sequence);
        job.ExternalId__c = 'event-job-' + sequence;
        job.JobScheduler__c = QueuesTestSupport.fakeId(JobScheduler__c.SObjectType, sequence);
        job.AttemptsMade__c = 2;
        job.Category__c = 'billing';
        job.GroupKey__c = 'invoice-' + sequence;
        job.CorrelationId__c = 'corr-' + sequence;
        job.FailedReason__c = state == Queues.State.FAILED ? 'processor failed' : null;
        return job;
    }

    private static JobRun__c jobRun(final Id jobId, final Integer sequence) {
        JobRun__c run = QueuesTestSupport.jobRun(jobId, sequence, Queues.RunStatus.COMPLETED.name());
        run.Id = QueuesTestSupport.fakeId(JobRun__c.SObjectType, sequence);
        return run;
    }
}
