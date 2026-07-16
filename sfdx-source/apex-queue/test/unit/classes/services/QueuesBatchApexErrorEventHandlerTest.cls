@IsTest(IsParallel=true)
private class QueuesBatchApexErrorEventHandlerTest {
    // <editor-fold desc="Failure Scope Mapping">
    @IsTest
    static void should_map_worker_execute_events_when_scoped_job_ids_are_present() {
        // Arrange
        Id scopedJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 201);
        Id asyncApexJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 1);
        Datetime occurredAt = Datetime.newInstance(2026, 4, 17, 9, 30, 0);
        BatchApexErrorEvent workerExecuteEvent = QueuesTestSupport.batchApexErrorEvent(
            asyncApexJobId,
            'EXECUTE',
            'JobScope=[' + scopedJobId + ']',
            'System.LimitException',
            'Too many SOQL queries: 101',
            'stack trace',
            occurredAt
        );
        BatchApexErrorEvent finishEvent = QueuesTestSupport.batchApexErrorEvent(
            asyncApexJobId,
            'FINISH',
            'JobScope=[' + scopedJobId + ']',
            'System.LimitException',
            'Too many SOQL queries: 101',
            'stack trace'
        );

        // Act
        List<QueuesBatchApexErrorEventHandler.FailureScope> failureScopes = QueuesBatchApexErrorEventHandler.mapToFailureScopes(
            new List<BatchApexErrorEvent>{ workerExecuteEvent, finishEvent },
            new Map<Id, AsyncApexJob>{
                asyncApexJobId => QueuesTestSupport.asyncApexJob(asyncApexJobId, 'Failed', 'QueuesBatchableWorker')
            }
        );

        // Assert
        Assert.areEqual(1, failureScopes.size());
        Assert.areEqual(1, failureScopes[0].jobIds.size());
        Assert.areEqual(true, failureScopes[0].jobIds.contains(scopedJobId));
        Assert.areEqual(occurredAt, failureScopes[0].occurredAt);
        Assert.areEqual('System.LimitException: Too many SOQL queries: 101', failureScopes[0].failureMessage);
        Assert.areEqual('stack trace', failureScopes[0].stackTrace);
    }

    @IsTest
    static void should_map_multiple_job_ids_when_batch_scope_contains_more_than_one_record() {
        // Arrange
        Id firstScopedJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 211);
        Id secondScopedJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 212);
        Id asyncApexJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 11);
        BatchApexErrorEvent workerExecuteEvent = QueuesTestSupport.batchApexErrorEvent(
            asyncApexJobId,
            'EXECUTE',
            'JobScope=[' + firstScopedJobId + ', ' + secondScopedJobId + ']',
            'System.LimitException',
            'Too many SOQL queries: 101',
            'stack trace'
        );

        // Act
        List<QueuesBatchApexErrorEventHandler.FailureScope> failureScopes = QueuesBatchApexErrorEventHandler.mapToFailureScopes(
            new List<BatchApexErrorEvent>{ workerExecuteEvent },
            new Map<Id, AsyncApexJob>{
                asyncApexJobId => QueuesTestSupport.asyncApexJob(asyncApexJobId, 'Failed', 'QueuesBatchableWorker')
            }
        );

        // Assert
        Assert.areEqual(1, failureScopes.size());
        Assert.areEqual(2, failureScopes[0].jobIds.size());
        Assert.areEqual(true, failureScopes[0].jobIds.contains(firstScopedJobId));
        Assert.areEqual(true, failureScopes[0].jobIds.contains(secondScopedJobId));
    }

    @IsTest
    static void should_extract_only_job_ids_from_scope() {
        // Arrange
        Id scopedJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 216);
        Id accountId = QueuesTestSupport.fakeId(Account.SObjectType, 1);

        // Act
        Set<Id> scopedJobIds = QueuesBatchApexErrorEventHandler.extractScopedJobIds(
            'JobScope=[' + scopedJobId + ', ' + accountId + ', not-an-id]'
        );

        // Assert
        Assert.areEqual(new Set<Id>{ scopedJobId }, scopedJobIds);
    }

    @IsTest
    static void should_map_batch_worker_child_events_to_parent_worker_owner() {
        // Arrange
        Id scopedJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 213);
        Id childAsyncApexJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 214);
        String parentAsyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 215);
        BatchApexErrorEvent workerExecuteEvent = QueuesTestSupport.batchApexErrorEvent(
            childAsyncApexJobId,
            'EXECUTE',
            'JobScope=[' + scopedJobId + ']',
            'System.LimitException',
            'Too many SOQL queries: 201',
            'stack trace'
        );

        // Act
        List<QueuesBatchApexErrorEventHandler.FailureScope> failureScopes = QueuesBatchApexErrorEventHandler.mapToFailureScopes(
            new List<BatchApexErrorEvent>{ workerExecuteEvent },
            new Map<Id, AsyncApexJob>{
                childAsyncApexJobId => asyncApexJobWithParent(
                    childAsyncApexJobId,
                    parentAsyncApexJobId,
                    'Completed',
                    'QueuesBatchableWorker'
                )
            }
        );

        // Assert
        Assert.areEqual(1, failureScopes.size());
        Assert.areEqual(parentAsyncApexJobId, failureScopes[0].asyncApexJobId);
    }

    @IsTest
    static void should_update_active_jobs_and_insert_failed_runs_when_handling_failure_scopes() {
        // Arrange
        Datetime occurredAt = System.now();
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 1);
        Job__c activeJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        activeJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 202);
        activeJob.AttemptsMade__c = 1;
        activeJob.MaxAttempts__c = 3;
        activeJob.AsyncApexJobId__c = asyncApexJobId;

        Job__c completedJob = QueuesTestSupport.job(Queues.State.COMPLETED.name());
        completedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 203);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsForUpdateAsSystem', new List<Job__c>{ activeJob, completedJob });
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );
        QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesJobRunService jobRunServiceStub = (QueuesJobRunService) Test.createStub(
            QueuesJobRunService.class,
            jobRunProvider
        );

        QueuesBatchApexErrorEventHandler handler = new QueuesBatchApexErrorEventHandler(
            new QueuesAsyncApexJobRepository(),
            jobRunServiceStub,
            jobServiceStub,
            new QueuesQueueEventService()
        );

        // Act
        handler.handleFailureScopes(
            new List<QueuesBatchApexErrorEventHandler.FailureScope>{
                new QueuesBatchApexErrorEventHandler.FailureScope(
                    new Set<Id>{ activeJob.Id, completedJob.Id },
                    asyncApexJobId,
                    occurredAt,
                    'System.LimitException: Too many SOQL queries: 101',
                    'stack trace'
                )
            }
        );

        // Assert
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobsAsSystem'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyFailure'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('queryJobsForUpdateAsSystem'));
        Assert.areEqual(1, jobRunProvider.getCallCount('insertJobRunsAsSystem'));

        List<Job__c> updatedJobs = (List<Job__c>) jobServiceProvider.getLastArgs('updateJobsAsSystem')[0];
        Job__c updatedJob = updatedJobs[0];
        Assert.areEqual(activeJob.Id, updatedJob.Id);
        Assert.areEqual(2, QueuesSupport.toInteger(updatedJob.AttemptsMade__c, 0));

        List<JobRun__c> runs = (List<JobRun__c>) jobRunProvider.getLastArgs('insertJobRunsAsSystem')[0];
        JobRun__c run = runs[0];
        Assert.areEqual(activeJob.Id, run.Job__c);
        Assert.areEqual(2, QueuesSupport.toInteger(run.AttemptNumber__c, 0));
        Assert.areEqual(Queues.RunStatus.FAILED.name(), run.Status__c);
        Assert.areEqual(asyncApexJobId, run.AsyncApexJobId__c);
        Assert.areEqual(true, run.Message__c.contains('stack trace'));
    }

    @IsTest
    static void should_publish_recovered_event_when_batch_failure_is_retryable() {
        // Arrange
        Datetime occurredAt = System.now();
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 61);
        Job__c activeJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        activeJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 261);
        activeJob.AttemptsMade__c = 0;
        activeJob.MaxAttempts__c = 2;
        activeJob.AsyncApexJobId__c = asyncApexJobId;
        QueuesTestSupport.RecordingStubProvider jobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsByIdsForUpdate', new List<Job__c>{ activeJob });
        QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesBatchApexErrorEventHandler handler = new QueuesBatchApexErrorEventHandler(
            new QueuesAsyncApexJobRepository(),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
            newJobService(jobRepositoryProvider),
            QueuesTestSupport.newRecordingQueueEventService(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                'RECOVERED;STALLED;FAILED',
                eventProvider
            )
        );

        // Act
        handler.handleFailureScopes(
            new List<QueuesBatchApexErrorEventHandler.FailureScope>{
                new QueuesBatchApexErrorEventHandler.FailureScope(
                    new Set<Id>{ activeJob.Id },
                    asyncApexJobId,
                    occurredAt,
                    'batch scope failed',
                    'stack trace'
                )
            }
        );

        // Assert
        Assert.areEqual(Queues.State.WAITING.name(), activeJob.State__c);
        Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
        List<QueueEvent__e> events = (List<QueueEvent__e>) eventProvider.getLastArgs('publishQueueEvents')[0];
        Assert.areEqual(1, events.size());
        Assert.areEqual('RECOVERED', events[0].Type__c);
        Assert.areEqual(String.valueOf(activeJob.Id), events[0].JobId__c);
        Assert.areEqual('batch worker failure recovery', events[0].Component__c);
    }

    @IsTest
    static void should_publish_stalled_and_failed_events_when_batch_failure_exhausts_attempts() {
        // Arrange
        Datetime occurredAt = System.now();
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 62);
        Job__c activeJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        activeJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 262);
        activeJob.AttemptsMade__c = 0;
        activeJob.MaxAttempts__c = 1;
        activeJob.AsyncApexJobId__c = asyncApexJobId;
        QueuesTestSupport.RecordingStubProvider jobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsByIdsForUpdate', new List<Job__c>{ activeJob });
        QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesBatchApexErrorEventHandler handler = new QueuesBatchApexErrorEventHandler(
            new QueuesAsyncApexJobRepository(),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
            newJobService(jobRepositoryProvider),
            QueuesTestSupport.newRecordingQueueEventService(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                'RECOVERED;STALLED;FAILED',
                eventProvider
            )
        );

        // Act
        handler.handleFailureScopes(
            new List<QueuesBatchApexErrorEventHandler.FailureScope>{
                new QueuesBatchApexErrorEventHandler.FailureScope(
                    new Set<Id>{ activeJob.Id },
                    asyncApexJobId,
                    occurredAt,
                    'batch scope failed',
                    'stack trace'
                )
            }
        );

        // Assert
        Assert.areEqual(Queues.State.FAILED.name(), activeJob.State__c);
        Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
        List<QueueEvent__e> events = (List<QueueEvent__e>) eventProvider.getLastArgs('publishQueueEvents')[0];
        Assert.areEqual(2, events.size());
        Assert.areEqual('STALLED', events[0].Type__c);
        Assert.areEqual(String.valueOf(activeJob.Id), events[0].JobId__c);
        Assert.areEqual('FAILED', events[1].Type__c);
        Assert.areEqual(String.valueOf(activeJob.Id), events[1].JobId__c);
        Assert.areEqual('batch worker failure recovery', events[0].Component__c);
        Assert.areEqual('batch worker failure recovery', events[1].Component__c);
    }

    @IsTest
    static void should_ignore_failure_scope_when_job_claim_owner_points_to_newer_async_worker() {
        // Arrange
        String staleAsyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 31);
        String currentAsyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 32);
        Job__c activeJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        activeJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 215);
        activeJob.AsyncApexJobId__c = currentAsyncApexJobId;

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsForUpdateAsSystem', new List<Job__c>{ activeJob });
        QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesBatchApexErrorEventHandler handler = new QueuesBatchApexErrorEventHandler(
            new QueuesAsyncApexJobRepository(),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesQueueEventService()
        );

        // Act
        handler.handleFailureScopes(
            new List<QueuesBatchApexErrorEventHandler.FailureScope>{
                new QueuesBatchApexErrorEventHandler.FailureScope(
                    new Set<Id>{ activeJob.Id },
                    staleAsyncApexJobId,
                    System.now(),
                    'System.LimitException: Too many SOQL queries: 101',
                    'stack trace'
                )
            }
        );

        // Assert
        Assert.areEqual(1, jobServiceProvider.getCallCount('queryJobsForUpdateAsSystem'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('applyFailure'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobsAsSystem'));
        Assert.areEqual(0, ((List<Job__c>) jobServiceProvider.getLastArgs('updateJobsAsSystem')[0]).size());
        Assert.areEqual(1, jobRunProvider.getCallCount('insertJobRunsAsSystem'));
        Assert.areEqual(0, ((List<JobRun__c>) jobRunProvider.getLastArgs('insertJobRunsAsSystem')[0]).size());
    }

    @IsTest
    static void should_treat_blank_async_owner_as_current_failure_owner_and_require_matching_claim_owner() {
        // Arrange
        Job__c activeJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        activeJob.AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 34);
        Job__c activeJobWithoutClaimOwner = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        Job__c activeJobWithLongClaimOwner = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        String longClaimOwner = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 35);
        activeJobWithLongClaimOwner.AsyncApexJobId__c = longClaimOwner;

        // Act
        Boolean blankAsyncOwnerMatches = QueuesBatchApexErrorEventHandler.isCurrentFailureOwner(
            activeJob,
            new QueuesBatchApexErrorEventHandler.FailureScope(new Set<Id>(), null, System.now(), 'ignored', null)
        );
        Boolean matchingClaimOwner = QueuesBatchApexErrorEventHandler.isCurrentFailureOwner(
            activeJob,
            new QueuesBatchApexErrorEventHandler.FailureScope(
                new Set<Id>(),
                activeJob.AsyncApexJobId__c,
                System.now(),
                'ignored',
                null
            )
        );
        Boolean blankClaimOwnerDoesNotMatch = QueuesBatchApexErrorEventHandler.isCurrentFailureOwner(
            activeJobWithoutClaimOwner,
            new QueuesBatchApexErrorEventHandler.FailureScope(
                new Set<Id>(),
                QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 33),
                System.now(),
                'ignored',
                null
            )
        );
        Boolean shortClaimOwnerMatches = QueuesBatchApexErrorEventHandler.isCurrentFailureOwner(
            activeJobWithLongClaimOwner,
            new QueuesBatchApexErrorEventHandler.FailureScope(
                new Set<Id>(),
                longClaimOwner.left(15),
                System.now(),
                'ignored',
                null
            )
        );

        // Assert
        Assert.areEqual(true, blankAsyncOwnerMatches);
        Assert.areEqual(true, matchingClaimOwner);
        Assert.areEqual(true, shortClaimOwnerMatches);
        Assert.areEqual(false, blankClaimOwnerDoesNotMatch);
    }

    @IsTest
    static void should_ignore_irrelevant_batch_events_when_mapping_failure_scopes() {
        // Arrange
        Id scopedJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 204);
        Id asyncApexJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 2);
        BatchApexErrorEvent wrongWorkerEvent = QueuesTestSupport.batchApexErrorEvent(
            asyncApexJobId,
            'EXECUTE',
            'JobScope=[' + scopedJobId + ']',
            '',
            'Only message',
            null
        );
        BatchApexErrorEvent invalidAsyncIdEvent = QueuesTestSupport.batchApexErrorEvent(
            null,
            'EXECUTE',
            'JobScope=[' + scopedJobId + ']',
            'System.LimitException',
            '',
            null
        );
        BatchApexErrorEvent blankScopeEvent = QueuesTestSupport.batchApexErrorEvent(
            asyncApexJobId,
            'EXECUTE',
            '',
            'System.LimitException',
            '',
            null
        );

        // Act
        List<QueuesBatchApexErrorEventHandler.FailureScope> failureScopes = QueuesBatchApexErrorEventHandler.mapToFailureScopes(
            new List<BatchApexErrorEvent>{ wrongWorkerEvent, invalidAsyncIdEvent, blankScopeEvent },
            new Map<Id, AsyncApexJob>{
                asyncApexJobId => QueuesTestSupport.asyncApexJob(asyncApexJobId, 'Failed', 'OtherBatch')
            }
        );
        Set<Id> scopedJobIds = QueuesBatchApexErrorEventHandler.extractScopedJobIds(null);
        new QueuesBatchApexErrorEventHandler().handle(new List<BatchApexErrorEvent>());

        // Assert
        Assert.areEqual(0, failureScopes.size());
        Assert.areEqual(0, scopedJobIds.size());
    }

    @IsTest
    static void should_handle_events_with_stubbed_async_job_repository_and_skip_empty_scopes() {
        // Arrange
        Id asyncApexJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 3);
        BatchApexErrorEvent blankScopeEvent = QueuesTestSupport.batchApexErrorEvent(
            asyncApexJobId,
            'EXECUTE',
            '',
            null,
            'Only message',
            null
        );
        QueuesTestSupport.RecordingStubProvider asyncRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryAsyncJobsByIds',
                new List<AsyncApexJob>{
                    QueuesTestSupport.asyncApexJob(asyncApexJobId, 'Failed', 'QueuesBatchableWorker')
                }
            );
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesBatchApexErrorEventHandler handler = new QueuesBatchApexErrorEventHandler(
            (QueuesAsyncApexJobRepository) Test.createStub(QueuesAsyncApexJobRepository.class, asyncRepositoryProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesQueueEventService()
        );

        // Act
        handler.handle(new List<BatchApexErrorEvent>{ blankScopeEvent });

        // Assert
        Assert.areEqual(1, asyncRepositoryProvider.getCallCount('queryAsyncJobsByIds'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('queryJobsForUpdateAsSystem'));
        Assert.areEqual(0, jobRunProvider.getCallCount('insertJobRunsAsSystem'));
    }

    @IsTest
    static void should_return_without_querying_jobs_when_failure_scopes_do_not_reference_jobs() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 4);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesBatchApexErrorEventHandler handler = new QueuesBatchApexErrorEventHandler(
            new QueuesAsyncApexJobRepository(),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesQueueEventService()
        );

        // Act
        handler.handleFailureScopes(
            new List<QueuesBatchApexErrorEventHandler.FailureScope>{
                new QueuesBatchApexErrorEventHandler.FailureScope(null, asyncApexJobId, System.now(), 'ignored', null)
            }
        );

        // Assert
        Assert.areEqual(0, jobServiceProvider.getCallCount('queryJobs'));
        Assert.areEqual(0, jobRunProvider.getCallCount('insertJobRunsAsSystem'));
    }

    @IsTest
    static void should_default_failure_scope_job_ids_and_build_failure_messages_for_sparse_events() {
        // Arrange
        Id scopedJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 206);
        Id asyncApexJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 4);
        String firstFailureAsyncJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 2);
        String secondFailureAsyncJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 3);
        String thirdFailureAsyncJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 5);
        BatchApexErrorEvent messageOnlyEvent = QueuesTestSupport.batchApexErrorEvent(
            asyncApexJobId,
            'EXECUTE',
            'JobScope=[' + scopedJobId + ']',
            null,
            'Only message',
            null
        );
        BatchApexErrorEvent exceptionOnlyEvent = QueuesTestSupport.batchApexErrorEvent(
            asyncApexJobId,
            'EXECUTE',
            'JobScope=[' + scopedJobId + ']',
            'System.CustomException',
            '',
            null
        );
        BatchApexErrorEvent invalidAsyncIdEvent = (BatchApexErrorEvent) JSON.deserialize(
            JSON.serialize(
                new Map<String, Object>{
                    'AsyncApexJobId' => 'not-an-id',
                    'Phase' => 'EXECUTE',
                    'JobScope' => 'JobScope=[' +
                    scopedJobId +
                    ']',
                    'ExceptionType' => 'System.ShouldBeIgnoredException',
                    'Message' => '',
                    'StackTrace' => null
                }
            ),
            BatchApexErrorEvent.class
        );
        Job__c activeJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        activeJob.Id = scopedJobId;
        activeJob.AttemptsMade__c = 0;
        activeJob.MaxAttempts__c = 2;
        activeJob.AsyncApexJobId__c = secondFailureAsyncJobId;

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsForUpdateAsSystem', new List<Job__c>{ activeJob });
        QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesBatchApexErrorEventHandler handler = new QueuesBatchApexErrorEventHandler(
            new QueuesAsyncApexJobRepository(),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesQueueEventService()
        );

        // Act
        List<QueuesBatchApexErrorEventHandler.FailureScope> failureScopes = QueuesBatchApexErrorEventHandler.mapToFailureScopes(
            new List<BatchApexErrorEvent>{ messageOnlyEvent, exceptionOnlyEvent, invalidAsyncIdEvent },
            new Map<Id, AsyncApexJob>{
                asyncApexJobId => QueuesTestSupport.asyncApexJob(asyncApexJobId, 'Failed', 'QueuesBatchableWorker')
            }
        );
        handler.handleFailureScopes(
            new List<QueuesBatchApexErrorEventHandler.FailureScope>{
                new QueuesBatchApexErrorEventHandler.FailureScope(
                    null,
                    firstFailureAsyncJobId,
                    System.now(),
                    'ignored',
                    null
                ),
                new QueuesBatchApexErrorEventHandler.FailureScope(
                    new Set<Id>{ scopedJobId },
                    secondFailureAsyncJobId,
                    System.now(),
                    failureScopes[0].failureMessage,
                    null
                ),
                new QueuesBatchApexErrorEventHandler.FailureScope(
                    new Set<Id>{ scopedJobId },
                    thirdFailureAsyncJobId,
                    System.now(),
                    failureScopes[1].failureMessage,
                    null
                )
            }
        );

        // Assert
        Assert.areEqual(2, failureScopes.size());
        Assert.areEqual('Only message', failureScopes[0].failureMessage);
        Assert.areEqual('System.CustomException', failureScopes[1].failureMessage);
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobsAsSystem'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('queryJobsForUpdateAsSystem'));
        Assert.areEqual(1, jobRunProvider.getCallCount('insertJobRunsAsSystem'));
    }

    @IsTest
    static void should_build_run_message_from_stack_trace_when_failure_message_is_missing() {
        // Arrange
        Datetime occurredAt = System.now();
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 1);
        Job__c activeJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        activeJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 205);
        activeJob.AttemptsMade__c = 0;
        activeJob.MaxAttempts__c = 2;
        activeJob.AsyncApexJobId__c = asyncApexJobId;

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsForUpdateAsSystem', new List<Job__c>{ activeJob });
        QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesBatchApexErrorEventHandler handler = new QueuesBatchApexErrorEventHandler(
            new QueuesAsyncApexJobRepository(),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesQueueEventService()
        );

        // Act
        handler.handleFailureScopes(
            new List<QueuesBatchApexErrorEventHandler.FailureScope>{
                new QueuesBatchApexErrorEventHandler.FailureScope(
                    new Set<Id>{ activeJob.Id },
                    asyncApexJobId,
                    occurredAt,
                    null,
                    'stack trace only'
                )
            }
        );

        // Assert
        JobRun__c run = ((List<JobRun__c>) jobRunProvider.getLastArgs('insertJobRunsAsSystem')[0])[0];
        Assert.areEqual('stack trace only', run.Message__c);
    }

    @IsTest
    static void should_update_multiple_active_jobs_when_failure_scope_contains_multiple_job_ids() {
        // Arrange
        Datetime occurredAt = System.now();
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 6);
        Job__c firstActiveJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        firstActiveJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 213);
        firstActiveJob.AttemptsMade__c = 1;
        firstActiveJob.MaxAttempts__c = 3;
        firstActiveJob.AsyncApexJobId__c = asyncApexJobId;

        Job__c secondActiveJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        secondActiveJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 214);
        secondActiveJob.AttemptsMade__c = 0;
        secondActiveJob.MaxAttempts__c = 2;
        secondActiveJob.AsyncApexJobId__c = asyncApexJobId;

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsForUpdateAsSystem', new List<Job__c>{ firstActiveJob, secondActiveJob });
        QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesBatchApexErrorEventHandler handler = new QueuesBatchApexErrorEventHandler(
            new QueuesAsyncApexJobRepository(),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesQueueEventService()
        );

        // Act
        handler.handleFailureScopes(
            new List<QueuesBatchApexErrorEventHandler.FailureScope>{
                new QueuesBatchApexErrorEventHandler.FailureScope(
                    new Set<Id>{ firstActiveJob.Id, secondActiveJob.Id },
                    asyncApexJobId,
                    occurredAt,
                    'System.LimitException: Too many SOQL queries: 101',
                    'stack trace'
                )
            }
        );

        // Assert
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobsAsSystem'));
        Assert.areEqual(2, jobServiceProvider.getCallCount('applyFailure'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('queryJobsForUpdateAsSystem'));
        Assert.areEqual(1, jobRunProvider.getCallCount('insertJobRunsAsSystem'));

        List<Job__c> updatedJobs = (List<Job__c>) jobServiceProvider.getLastArgs('updateJobsAsSystem')[0];
        Assert.areEqual(2, updatedJobs.size());

        List<JobRun__c> runs = (List<JobRun__c>) jobRunProvider.getLastArgs('insertJobRunsAsSystem')[0];
        Assert.areEqual(2, runs.size());
        Assert.areEqual(Queues.RunStatus.FAILED.name(), runs[0].Status__c);
        Assert.areEqual(Queues.RunStatus.FAILED.name(), runs[1].Status__c);
    }

    private static QueuesJobService newJobService(final QueuesTestSupport.RecordingStubProvider jobRepositoryProvider) {
        return new QueuesJobService(
            (QueuesJobRepository) Test.createStub(QueuesJobRepository.class, jobRepositoryProvider),
            new QueuesDefinitionService()
        );
    }

    private static AsyncApexJob asyncApexJobWithParent(
        final Id jobId,
        final String parentJobId,
        final String status,
        final String apexClassName
    ) {
        return (AsyncApexJob) JSON.deserialize(
            JSON.serialize(
                new Map<String, Object>{
                    'Id' => jobId,
                    'ParentJobId' => parentJobId,
                    'Status' => status,
                    'ApexClass' => new Map<String, Object>{ 'Name' => apexClassName }
                }
            ),
            AsyncApexJob.class
        );
    }
    // </editor-fold>
}
