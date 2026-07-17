@IsTest(IsParallel=true)
private class QueuesQueueableDispatchServiceTest {
    // <editor-fold desc="Lifecycle Events">
    @IsTest
    static void should_publish_failed_events_when_dispatch_fails() {
        // Arrange
        Id dispatchId = QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 914);
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 914);
        QueueableDispatch__c dispatch = new QueueableDispatch__c(
            Id = dispatchId,
            State__c = Queues.QueueableDispatchState.ACTIVE.name()
        );
        Job__c waitingJob = new Job__c(
            Id = jobId,
            QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
            State__c = Queues.State.WAITING.name()
        );
        QueuesTestSupport.RecordingStubProvider dispatchProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryDispatchByIdForUpdate', dispatch)
            .withReturn('updateDispatch', dispatch);
        QueuesTestSupport.RecordingStubProvider jobProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryQueueableDispatchJobsForUpdate', new List<Job__c>{ waitingJob })
            .withReturn('updateJobs', new List<Job__c>{ waitingJob });
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueableDispatchService service = newService(
            dispatchProvider,
            jobProvider,
            QueuesTestSupport.newRecordingQueueEventService(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                'FAILED',
                eventProvider
            )
        );

        // Act
        QueueableDispatch__c result = service.failDispatchAsSystem(dispatchId, 'dispatcher failed');

        // Assert
        Assert.areEqual(Queues.QueueableDispatchState.FAILED.name(), result.State__c);
        Assert.areEqual(Queues.State.FAILED.name(), waitingJob.State__c);
        Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
        List<QueueEvent__e> events = (List<QueueEvent__e>) eventProvider.getLastArgs('publishQueueEvents')[0];
        Assert.areEqual(1, events.size());
        Assert.areEqual(Queues.State.FAILED.name(), events[0].Type__c);
        Assert.areEqual(String.valueOf(jobId), events[0].JobId__c);
    }

    @IsTest
    static void should_publish_active_and_failed_events_for_mixed_dispatch_results() {
        // Arrange
        Job__c activeJob = new Job__c(
            Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 915),
            QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
            State__c = Queues.State.ACTIVE.name()
        );
        Job__c failedJob = new Job__c(
            Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 916),
            QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
            State__c = Queues.State.FAILED.name()
        );
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueableDispatchService service = newService(
            new QueuesTestSupport.RecordingStubProvider(),
            new QueuesTestSupport.RecordingStubProvider(),
            QueuesTestSupport.newRecordingQueueEventService(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                'ACTIVE;FAILED',
                eventProvider
            )
        );

        // Act
        service.publishResolvedJobEvents(new List<Job__c>{ activeJob, failedJob });

        // Assert
        Assert.areEqual(2, eventProvider.getCallCount('publishQueueEvents'));
        Map<String, String> jobIdsByType = new Map<String, String>();
        for (List<Object> callArgs : eventProvider.getCalls('publishQueueEvents')) {
            List<QueueEvent__e> events = (List<QueueEvent__e>) callArgs[0];
            Assert.areEqual(1, events.size());
            jobIdsByType.put(events[0].Type__c, events[0].JobId__c);
        }
        Assert.areEqual(String.valueOf(activeJob.Id), jobIdsByType.get('ACTIVE'));
        Assert.areEqual(String.valueOf(failedJob.Id), jobIdsByType.get('FAILED'));
    }
    // </editor-fold>

    private static QueuesQueueableDispatchService newService(
        final QueuesTestSupport.RecordingStubProvider dispatchProvider,
        final QueuesTestSupport.RecordingStubProvider jobProvider,
        final QueuesQueueEventService queueEventService
    ) {
        return new QueuesQueueableDispatchService(
            (QueuesQueueableDispatchRepository) Test.createStub(
                QueuesQueueableDispatchRepository.class,
                dispatchProvider
            ),
            (QueuesJobRepository) Test.createStub(QueuesJobRepository.class, jobProvider),
            (QueuesAsyncApexJobRepository) Test.createStub(
                QueuesAsyncApexJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
            ),
            queueEventService
        );
    }
}
