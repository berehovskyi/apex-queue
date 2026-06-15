@IsTest(IsParallel=true)
private class QueuesErrorServiceTest {
    // <editor-fold desc="Record Queue Error">
    @IsTest
    static void should_record_queue_error_when_exception_is_present() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
            QueuesErrorEventRepository.class,
            eventRepositoryProvider
        );

        QueuesErrorService service = new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub);

        // Act
        service.recordQueueError(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'runtime maintenance',
            new Queues.QueueException('Maintenance exploded.')
        );

        // Assert
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) eventRepositoryProvider.getLastArgs(
            'publishQueueError'
        )[0];
        Assert.areEqual(true, String.isNotBlank(errorEvent.EventUuid));
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, errorEvent.QueueName__c);
        Assert.areEqual('runtime maintenance', errorEvent.Component__c);
        Assert.areEqual('Maintenance exploded.', errorEvent.Message__c);
        Assert.areEqual(null, errorEvent.JobId__c);
        Assert.areEqual(null, errorEvent.AsyncApexJobId__c);
        Assert.areEqual(true, String.isNotBlank(errorEvent.StackTrace__c));
        Assert.areNotEqual(null, errorEvent.OccurredAt__c);
    }

    @IsTest
    static void should_record_queue_error_context_when_job_and_async_identifiers_are_present() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 301);
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 1);
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
            QueuesErrorEventRepository.class,
            eventRepositoryProvider
        );

        QueuesErrorService service = new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub);

        // Act
        service.recordQueueError(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'worker scheduling',
            jobId,
            asyncApexJobId,
            new Queues.QueueException('Dispatch exploded.')
        );

        // Assert
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) eventRepositoryProvider.getLastArgs(
            'publishQueueError'
        )[0];
        Assert.areEqual(true, String.isNotBlank(errorEvent.EventUuid));
        Assert.areEqual(jobId, errorEvent.JobId__c);
        Assert.areEqual(asyncApexJobId, errorEvent.AsyncApexJobId__c);
        Assert.areEqual('Dispatch exploded.', errorEvent.Message__c);
    }

    @IsTest
    static void should_publish_same_queue_error_only_once_per_transaction() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 302);
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 2);
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
            QueuesErrorEventRepository.class,
            eventRepositoryProvider
        );
        QueuesErrorService service = new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub);

        Exception failure = new Queues.QueueException('Dispatch exploded.');

        // Act
        service.recordQueueError(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'worker scheduling',
            jobId,
            asyncApexJobId,
            failure
        );
        service.recordQueueError(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'worker scheduling', failure);

        // Assert
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) eventRepositoryProvider.getLastArgs(
            'publishQueueError'
        )[0];
        Assert.areEqual(jobId, errorEvent.JobId__c);
        Assert.areEqual(asyncApexJobId, errorEvent.AsyncApexJobId__c);
    }

    @IsTest
    static void should_publish_same_logical_queue_error_only_once_even_when_components_differ() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 303);
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 3);
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
            QueuesErrorEventRepository.class,
            eventRepositoryProvider
        );
        QueuesErrorService service = new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub);

        Exception failure = new Queues.QueueException('Dispatch exploded.');

        // Act
        service.recordQueueError(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'worker scheduling',
            jobId,
            asyncApexJobId,
            failure
        );
        service.recordQueueError(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'runtime maintenance', failure);

        // Assert
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) eventRepositoryProvider.getLastArgs(
            'publishQueueError'
        )[0];
        Assert.areEqual('worker scheduling', errorEvent.Component__c);
        Assert.areEqual(jobId, errorEvent.JobId__c);
        Assert.areEqual(asyncApexJobId, errorEvent.AsyncApexJobId__c);
    }

    @IsTest
    static void should_propagate_publish_failures() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withException('publishQueueError', new Queues.QueueException('Repository failed.'));
        QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
            QueuesErrorEventRepository.class,
            eventRepositoryProvider
        );

        QueuesErrorService service = new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub);

        // Act
        try {
            service.recordQueueError(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                'runtime maintenance',
                new Queues.QueueException()
            );
            Assert.fail('Expected publishQueueError to throw.');
        } catch (Queues.QueueException ex) {
            Assert.areEqual('Repository failed.', ex.getMessage());
        }
    }

    @IsTest
    static void should_insert_queue_errors_and_skip_recording_when_exception_is_missing() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        QueueError__c error = new QueueError__c(
            QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
            Component__c = 'worker scheduling',
            Message__c = 'boom',
            OccurredAt__c = System.now()
        );
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns('insertQueueError', new List<Object>{ null, error })
            .withReturns(
                'insertQueueErrors',
                new List<Object>{ new List<QueueError__c>(), new List<QueueError__c>{ error } }
            );
        QueuesErrorRepository repositoryStub = (QueuesErrorRepository) Test.createStub(
            QueuesErrorRepository.class,
            repositoryProvider
        );

        QueuesErrorService service = new QueuesErrorService(repositoryStub, new QueuesErrorEventRepository());

        // Act
        QueueError__c nullInsertedError = service.insertQueueErrorAsSystem(null);
        QueueError__c insertedError = service.insertQueueErrorAsSystem(error);
        List<QueueError__c> emptyInsertedErrors = service.insertQueueErrors(new List<QueueError__c>());
        List<QueueError__c> insertedErrors = service.insertQueueErrors(new List<QueueError__c>{ error });
        service.recordQueueError(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'worker scheduling', (Exception) null);

        // Assert
        Assert.areEqual(null, nullInsertedError);
        Assert.areEqual(error, insertedError);
        Assert.areEqual(0, emptyInsertedErrors.size());
        Assert.areEqual(1, insertedErrors.size());
        Assert.areEqual(2, repositoryProvider.getCallCount('insertQueueError'));
        Assert.areEqual(2, repositoryProvider.getCallCount('insertQueueErrors'));
    }
    // </editor-fold>
}
