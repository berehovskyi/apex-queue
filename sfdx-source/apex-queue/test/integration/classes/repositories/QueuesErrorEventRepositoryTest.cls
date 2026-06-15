@IsTest(IsParallel=false)
private class QueuesErrorEventRepositoryTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    private class ThrowingPublishRepository extends QueuesErrorEventRepository {
        private ThrowingPublishRepository() {
            super();
        }

        private ThrowingPublishRepository(final QueuesErrorRepository errorRepository) {
            super(errorRepository);
        }

        protected override void publishEvents(final List<SObject> events, final Object callback) {
            throw new Queues.QueueException('Event bus exploded.');
        }
    }

    private class FakeFailureResult implements EventBus.FailureResult {
        private String eventUuid;

        private FakeFailureResult(final String eventUuidValue) {
            eventUuid = eventUuidValue;
        }

        public List<String> getEventUuids() {
            return new List<String>{ eventUuid };
        }
    }

    @IsTest
    static void should_insert_queue_errors_for_failed_event_uuids() {
        // Arrange
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 701);
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 111);
        QueueErrorEvent__e eventRecord = QueuesErrorService.mapToErrorEvent(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'worker scheduling',
            jobId,
            asyncApexJobId,
            new Queues.QueueException('Dispatch exploded.')
        );
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorEventRepository.PublishFailureCallback callback = new QueuesErrorEventRepository.PublishFailureCallback(
            new List<QueueErrorEvent__e>{ eventRecord },
            (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, repositoryProvider)
        );

        // Act
        callback.handleFailure(new List<String>{ eventRecord.EventUuid });

        // Assert
        Assert.areEqual(1, repositoryProvider.getCallCount('insertQueueErrors'));
        List<QueueError__c> errors = (List<QueueError__c>) repositoryProvider.getLastArgs('insertQueueErrors')[0];
        Assert.areEqual(1, errors.size());
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, errors[0].QueueName__c);
        Assert.areEqual('worker scheduling', errors[0].Component__c);
        Assert.areEqual(jobId, errors[0].Job__c);
        Assert.areEqual(asyncApexJobId, errors[0].AsyncApexJobId__c);
        Assert.areEqual('Dispatch exploded.', errors[0].Message__c);
    }

    @IsTest
    static void should_map_invalid_job_ids_to_null_when_callback_materializes_errors() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 112);
        QueueErrorEvent__e eventRecord = QueuesErrorService.mapToErrorEvent(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'worker scheduling',
            null,
            asyncApexJobId,
            new Queues.QueueException('Dispatch exploded.')
        );
        eventRecord.JobId__c = 'not-an-id';
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorEventRepository.PublishFailureCallback callback = new QueuesErrorEventRepository.PublishFailureCallback(
            new List<QueueErrorEvent__e>{ eventRecord },
            (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, repositoryProvider)
        );

        // Act
        callback.handleFailure(new List<String>{ eventRecord.EventUuid });

        // Assert
        List<QueueError__c> errors = (List<QueueError__c>) repositoryProvider.getLastArgs('insertQueueErrors')[0];
        Assert.areEqual(null, errors[0].Job__c);
    }

    @IsTest
    static void should_ignore_unknown_event_uuids() {
        // Arrange
        QueueErrorEvent__e eventRecord = QueuesErrorService.mapToErrorEvent(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'runtime maintenance',
            null,
            null,
            new Queues.QueueException('Maintenance exploded.')
        );
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorEventRepository.PublishFailureCallback callback = new QueuesErrorEventRepository.PublishFailureCallback(
            new List<QueueErrorEvent__e>{ eventRecord },
            (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, repositoryProvider)
        );

        // Act
        callback.handleFailure(new List<String>{ '00000000-0000-0000-0000-000000000000' });

        // Assert
        Assert.areEqual(0, repositoryProvider.getCallCount('insertQueueErrors'));
    }

    @IsTest
    static void should_ignore_events_missing_required_fields() {
        // Arrange
        QueueErrorEvent__e eventRecord = new QueueErrorEvent__e(
            QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
            Message__c = 'Missing component.'
        );
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorEventRepository.PublishFailureCallback callback = new QueuesErrorEventRepository.PublishFailureCallback(
            new List<QueueErrorEvent__e>{ eventRecord },
            (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, repositoryProvider)
        );

        // Act
        callback.handleFailure(new List<String>{ null });

        // Assert
        Assert.areEqual(0, repositoryProvider.getCallCount('insertQueueErrors'));
    }

    @IsTest
    static void should_swallow_repository_failures_when_callback_insert_fails() {
        // Arrange
        QueueErrorEvent__e eventRecord = QueuesErrorService.mapToErrorEvent(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'runtime maintenance',
            null,
            null,
            new Queues.QueueException('Maintenance exploded.')
        );
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withException('insertQueueErrors', new Queues.QueueException('Insert exploded.'))
            .withException('insertQueueError', new Queues.QueueException('Single insert exploded.'));
        QueuesErrorEventRepository.PublishFailureCallback callback = new QueuesErrorEventRepository.PublishFailureCallback(
            new List<QueueErrorEvent__e>{ eventRecord },
            (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, repositoryProvider)
        );

        // Act
        callback.handleFailure(new List<String>{ eventRecord.EventUuid });

        // Assert
        Assert.areEqual(1, repositoryProvider.getCallCount('insertQueueErrors'));
        Assert.areEqual(1, repositoryProvider.getCallCount('insertQueueError'));
    }

    @IsTest
    static void should_retry_failed_callback_bulk_insert_one_error_at_a_time() {
        // Arrange
        QueueErrorEvent__e firstEventRecord = QueuesErrorService.mapToErrorEvent(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'runtime maintenance',
            null,
            null,
            new Queues.QueueException('First exploded.')
        );
        QueueErrorEvent__e secondEventRecord = QueuesErrorService.mapToErrorEvent(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'worker scheduling',
            null,
            QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 115),
            new Queues.QueueException('Second exploded.')
        );
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withException('insertQueueErrors', new Queues.QueueException('Insert exploded.'));
        QueuesErrorEventRepository.PublishFailureCallback callback = new QueuesErrorEventRepository.PublishFailureCallback(
            new List<QueueErrorEvent__e>{ firstEventRecord, secondEventRecord },
            (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, repositoryProvider)
        );

        // Act
        callback.handleFailure(new List<String>{ firstEventRecord.EventUuid, secondEventRecord.EventUuid });

        // Assert
        Assert.areEqual(1, repositoryProvider.getCallCount('insertQueueErrors'));
        Assert.areEqual(2, repositoryProvider.getCallCount('insertQueueError'));
    }

    @IsTest
    static void should_fallback_to_direct_insert_when_publish_throws() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 113);
        QueueErrorEvent__e eventRecord = QueuesErrorService.mapToErrorEvent(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'worker scheduling',
            null,
            asyncApexJobId,
            new Queues.QueueException('Dispatch exploded.')
        );
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        ThrowingPublishRepository repository = new ThrowingPublishRepository(
            (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, repositoryProvider)
        );

        // Act
        repository.publishQueueError(eventRecord);

        // Assert
        Assert.areEqual(1, repositoryProvider.getCallCount('insertQueueErrors'));
        List<QueueError__c> errors = (List<QueueError__c>) repositoryProvider.getLastArgs('insertQueueErrors')[0];
        Assert.areEqual(asyncApexJobId, errors[0].AsyncApexJobId__c);
        Assert.areEqual('Dispatch exploded.', errors[0].Message__c);
    }

    @IsTest
    static void should_publish_queue_error_events_through_event_bus() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 114);
        QueueErrorEvent__e eventRecord = QueuesErrorService.mapToErrorEvent(
            'queue-error-repository',
            'runtime maintenance',
            null,
            asyncApexJobId,
            new Queues.QueueException('Published through repository.')
        );
        QueuesErrorEventRepository repository = new QueuesErrorEventRepository(System.AccessLevel.SYSTEM_MODE);

        // Act
        Test.startTest();
        System.runAs(QueuesTestSupport.queueUser()) {
            repository.publishQueueError(eventRecord);
        }
        Test.getEventBus().deliver();
        Test.stopTest();

        // Assert
        QueueError__c storedError = QueuesTestSupport.queryQueueErrorsAsSystem('queue-error-repository')[0];
        Assert.areEqual('runtime maintenance', storedError.Component__c);
        Assert.areEqual(asyncApexJobId, storedError.AsyncApexJobId__c);
        Assert.areEqual('Published through repository.', storedError.Message__c);
    }

    @IsTest
    static void should_publish_system_mode_queue_error_events_as_queue_user() {
        // Arrange
        QueueErrorEvent__e eventRecord = QueuesErrorService.mapToErrorEvent(
            'queue-error-repository-user',
            'runtime maintenance',
            null,
            null,
            new Queues.QueueException('Published through repository as queue user.')
        );

        // Act
        Test.startTest();
        System.runAs(QueuesTestSupport.queueUser()) {
            Assert.isFalse(QueueErrorEvent__e.SObjectType.getDescribe(SObjectDescribeOptions.DEFERRED).isCreateable());
            new QueuesErrorEventRepository(System.AccessLevel.SYSTEM_MODE).publishQueueError(eventRecord);
        }
        Test.getEventBus().deliver();
        Test.stopTest();

        // Assert
        List<QueueError__c> errors = QueuesTestSupport.queryQueueErrorsAsSystem('queue-error-repository-user');
        Assert.areEqual(1, errors.size());
        Assert.areEqual('runtime maintenance', errors[0].Component__c);
        Assert.areEqual('Published through repository as queue user.', errors[0].Message__c);
    }

    @IsTest
    static void should_delegate_failure_result_to_handle_failure() {
        // Arrange
        QueueErrorEvent__e eventRecord = QueuesErrorService.mapToErrorEvent(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'runtime maintenance',
            null,
            null,
            new Queues.QueueException('Maintenance exploded.')
        );
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorEventRepository.PublishFailureCallback callback = new QueuesErrorEventRepository.PublishFailureCallback(
            new List<QueueErrorEvent__e>{ eventRecord },
            (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, repositoryProvider)
        );
        EventBus.FailureResult failureResult = new FakeFailureResult(eventRecord.EventUuid);

        // Act
        callback.onFailure(failureResult);

        // Assert
        Assert.areEqual(1, repositoryProvider.getCallCount('insertQueueErrors'));
    }

    @IsTest
    static void should_ignore_null_events_when_publishing() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        ThrowingPublishRepository repository = new ThrowingPublishRepository(
            (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, repositoryProvider)
        );

        // Act
        repository.publishQueueError(null);

        // Assert
        Assert.areEqual(0, repositoryProvider.getCallCount('insertQueueErrors'));
    }
}
