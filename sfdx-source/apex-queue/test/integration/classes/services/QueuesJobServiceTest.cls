@IsTest(IsParallel=false)
private class QueuesJobServiceTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_return_now_when_due_jobs_exist() {
        // Arrange
        Datetime currentNow = System.now();
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryDueJobs', new List<Job__c>{ QueuesTestSupport.job(Queues.State.WAITING.name()) });
        QueuesJobRepository repositoryStub = (QueuesJobRepository) Test.createStub(
            QueuesJobRepository.class,
            repositoryProvider
        );

        QueuesJobService service = new QueuesJobService(repositoryStub, new QueuesDefinitionService());

        // Act
        Datetime nextWakeAt = service.findNextWakeAtAsSystem(QueuesTestSupport.DEFAULT_QUEUE_NAME, currentNow);

        // Assert
        Assert.areEqual(currentNow, nextWakeAt);
        Assert.areEqual(1, repositoryProvider.getCallCount('queryDueJobs'));
        Assert.areEqual(0, repositoryProvider.getCallCount('queryNextPumpAtAsSystem'));
    }

    @IsTest
    static void should_fallback_to_next_pump_and_ignore_empty_job_collections_when_job_views_are_requested() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'job-service-fallback-' + System.now().getTime();
            Datetime currentNow = System.now();
            Job__c delayedJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.DELAYED.name(),
                'job-delay'
            );
            delayedJob.AvailableAt__c = currentNow.addMinutes(5);
            insert delayedJob;

            AggregateResult nextPumpRow = [
                SELECT MIN(AvailableAt__c) nextPumpAt
                FROM Job__c
                WHERE
                    QueueName__c = :queueName
                    AND State__c IN :new Set<String>{ Queues.State.WAITING.name(), Queues.State.DELAYED.name() }
                    AND AvailableAt__c > :currentNow
            ][0];

            QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryDueJobs', new List<Job__c>())
                .withReturn('queryNextPumpAt', new List<AggregateResult>{ nextPumpRow })
                .withReturn('queryJobsByIds', new List<Job__c>())
                .withReturn('queryJobsByIdsForUpdate', new List<Job__c>());
            QueuesJobRepository repositoryStub = (QueuesJobRepository) Test.createStub(
                QueuesJobRepository.class,
                repositoryProvider
            );

            QueuesJobService service = new QueuesJobService(repositoryStub, new QueuesDefinitionService());

            // Act
            Datetime nextWakeAt = service.findNextWakeAtAsSystem(queueName, currentNow);
            List<Job__c> claimedJobs = service.claimBatchScopeJobsAsSystem(
                QueuesTestSupport.queueDefinition(queueName, true),
                new List<Job__c>{ new Job__c() },
                currentNow,
                QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 301)
            );
            List<Job__c> jobs = service.queryJobsAsSystem(null);
            Integer retryDelay = QueuesJobService.resolveRetryDelayMinutes(
                new Job__c(BackoffType__c = 'mystery', BackoffValue__c = 5),
                2
            );

            // Assert
            Assert.areEqual(currentNow.addMinutes(5), nextWakeAt);
            Assert.areEqual(0, claimedJobs.size());
            Assert.areEqual(0, jobs.size());
            Assert.areEqual(0, retryDelay);
            Assert.areEqual(1, repositoryProvider.getCallCount('queryDueJobs'));
            Assert.areEqual(1, repositoryProvider.getCallCount('queryNextPumpAt'));
        }
    }

    @IsTest
    static void should_dedupe_external_ids_when_resolving_jobs() {
        // Arrange
        Job__c firstExistingJob = QueuesTestSupport.job(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            QueuesTestSupport.DEFAULT_JOB_NAME,
            Queues.State.WAITING.name(),
            'job-a'
        );
        firstExistingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 31);
        Job__c duplicateExistingJob = QueuesTestSupport.job(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            QueuesTestSupport.DEFAULT_JOB_NAME,
            Queues.State.WAITING.name(),
            'job-a'
        );
        duplicateExistingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 32);
        Job__c secondExistingJob = QueuesTestSupport.job(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            QueuesTestSupport.DEFAULT_JOB_NAME,
            Queues.State.WAITING.name(),
            'job-b'
        );
        secondExistingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 33);

        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryJobsByExternalIds',
                new List<Job__c>{ firstExistingJob, duplicateExistingJob, secondExistingJob }
            );
        QueuesJobRepository repositoryStub = (QueuesJobRepository) Test.createStub(
            QueuesJobRepository.class,
            repositoryProvider
        );

        QueuesJobService service = new QueuesJobService(repositoryStub, new QueuesDefinitionService());

        // Act
        Map<String, Job__c> jobsByExternalId = service.getJobsByExternalId(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            new List<Queues.JobData>{
                new Queues.JobData(
                    QueuesTestSupport.DEFAULT_JOB_NAME,
                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                    null,
                    new Queues.JobOptions().jobId('JOB-A')
                ),
                new Queues.JobData(
                    QueuesTestSupport.DEFAULT_JOB_NAME,
                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                    null,
                    new Queues.JobOptions().jobId('job-b')
                ),
                new Queues.JobData(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, null)
            }
        );
        Job__c createdJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        createdJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 34);
        Job__c resolvedCreatedJob = service.getResolvedJob(
            new Map<String, Job__c>{ 'job-a' => firstExistingJob },
            new Map<String, Job__c>{ 'job-a' => createdJob },
            'JOB-A'
        );
        Job__c resolvedExistingJob = service.getResolvedJob(
            new Map<String, Job__c>{ 'job-b' => secondExistingJob },
            new Map<String, Job__c>(),
            'JOB-B'
        );
        Job__c unresolvedJob = service.getResolvedJob(new Map<String, Job__c>(), new Map<String, Job__c>(), null);

        // Assert
        Assert.areEqual(2, jobsByExternalId.size());
        Assert.areEqual(firstExistingJob.Id, jobsByExternalId.get('job-a').Id);
        Assert.areEqual(secondExistingJob.Id, jobsByExternalId.get('job-b').Id);
        Assert.areEqual(1, repositoryProvider.getCallCount('queryJobsByExternalIds'));
        Assert.areEqual(createdJob.Id, resolvedCreatedJob.Id);
        Assert.areEqual(secondExistingJob.Id, resolvedExistingJob.Id);
        Assert.areEqual(null, unresolvedJob);
    }

    @IsTest
    static void should_claim_scope_jobs_when_scope_jobs_are_runnable() {
        // Arrange
        Datetime currentNow = System.now();
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        queueDefinition.DefaultLeaseMinutes__c = 7;

        Job__c scopeJob = new Job__c(Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 41));
        Job__c skippedScopeJob = new Job__c(Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 42));

        Job__c currentJob = QueuesTestSupport.job(
            queueDefinition.QueueName__c,
            QueuesTestSupport.DEFAULT_JOB_NAME,
            Queues.State.WAITING.name(),
            'job-41'
        );
        currentJob.Id = scopeJob.Id;
        currentJob.AvailableAt__c = currentNow.addMinutes(-1);
        Job__c skippedCurrentJob = QueuesTestSupport.job(
            queueDefinition.QueueName__c,
            QueuesTestSupport.DEFAULT_JOB_NAME,
            Queues.State.COMPLETED.name(),
            'job-42'
        );
        skippedCurrentJob.Id = skippedScopeJob.Id;
        skippedCurrentJob.AvailableAt__c = currentNow.addMinutes(-1);

        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsByIdsForUpdate', new List<Job__c>{ currentJob, skippedCurrentJob })
            .withReturn('queryEmptyJobLocator', QueuesTestSupport.emptyJobLocator())
            .withReturn('queryClaimedJobLocator', QueuesTestSupport.emptyJobLocator());
        QueuesJobRepository repositoryStub = (QueuesJobRepository) Test.createStub(
            QueuesJobRepository.class,
            repositoryProvider
        );
        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('getLeaseMinutes', 7)
        );

        QueuesJobService service = new QueuesJobService(repositoryStub, definitionStub);

        // Act
        List<Job__c> claimedJobs = service.claimBatchScopeJobsAsSystem(
            queueDefinition,
            new List<Job__c>{ scopeJob, skippedScopeJob },
            currentNow,
            QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 302)
        );
        Database.QueryLocator claimedLocator = service.queryClaimedJobLocatorAsSystem(claimedJobs);
        Database.QueryLocator emptyLocator = service.queryClaimedJobLocatorAsSystem(new List<Job__c>());
        Boolean hasLeaseTokenSuffix = claimedJobs[0].LeaseToken__c.endsWith(':' + scopeJob.Id);

        // Assert
        Assert.areEqual(1, claimedJobs.size());
        Assert.areEqual(scopeJob.Id, claimedJobs[0].Id);
        Assert.areEqual(Queues.State.ACTIVE, QueuesSupport.toState(claimedJobs[0].State__c));
        Assert.areEqual(true, hasLeaseTokenSuffix);
        Assert.areEqual(currentNow.addMinutes(7), claimedJobs[0].LeaseExpiresAt__c);
        Assert.areEqual(QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 302), claimedJobs[0].AsyncApexJobId__c);
        Assert.areNotEqual(null, claimedLocator);
        Assert.areNotEqual(null, emptyLocator);
        Assert.areEqual(1, repositoryProvider.getCallCount('updateJobs'));
        Assert.areEqual(2, repositoryProvider.getCallCount('queryClaimedJobLocator'));
        Assert.areEqual(0, ((Set<Id>) repositoryProvider.getCalls('queryClaimedJobLocator')[1][0]).size());
        Assert.areEqual(0, repositoryProvider.getCallCount('emptyJobLocator'));
    }

    @IsTest
    static void should_claim_invocable_job_when_due_and_leave_other_modes_alone() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            Datetime currentNow = System.now();
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            queueDefinition.DefaultLeaseMinutes__c = 9;

            Job__c invocableJob = QueuesTestSupport.job(
                queueDefinition.QueueName__c,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invocable-due'
            );
            invocableJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            invocableJob.AvailableAt__c = currentNow.addMinutes(-1);

            Job__c workerJob = QueuesTestSupport.job(
                queueDefinition.QueueName__c,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'worker-due'
            );
            workerJob.ExecutionMode__c = Queues.ExecutionMode.WORKER.name();
            workerJob.AvailableAt__c = currentNow.addMinutes(-1);

            Job__c futureInvocableJob = QueuesTestSupport.job(
                queueDefinition.QueueName__c,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.DELAYED.name(),
                'invocable-future'
            );
            futureInvocableJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            futureInvocableJob.AvailableAt__c = currentNow.addMinutes(5);

            insert new List<Job__c>{ invocableJob, workerJob, futureInvocableJob };
            QueuesTestSupport.RecordingStubProvider definitionProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'findQueueDefinitionsByQueueNamesAsSystem',
                    new Map<String, QueueDefinition__mdt>{ queueDefinition.QueueName__c => queueDefinition }
                )
                .withReturn('getLeaseMinutes', 9);
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                definitionProvider
            );
            QueuesJobService service = new QueuesJobService(
                new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE),
                definitionStub
            );
            Integer queriesBefore = Limits.getQueries();
            Integer dmlStatementsBefore = Limits.getDmlStatements();

            // Act
            List<Job__c> claimedJobs = service.claimInvocableJobsAsSystem(
                new Set<Id>{ invocableJob.Id, workerJob.Id, futureInvocableJob.Id },
                currentNow
            );

            // Assert
            Assert.areEqual(1, claimedJobs.size());
            Assert.areEqual(invocableJob.Id, claimedJobs[0].Id);
            Assert.areEqual(Queues.State.ACTIVE, QueuesSupport.toState(claimedJobs[0].State__c));
            Assert.areEqual(true, claimedJobs[0].LeaseToken__c.endsWith(':' + invocableJob.Id));
            Assert.areEqual(currentNow.addMinutes(9), claimedJobs[0].LeaseExpiresAt__c);
            Assert.areEqual(currentNow.addMinutes(-1), claimedJobs[0].AvailableAt__c);
            Assert.areEqual(null, claimedJobs[0].AsyncApexJobId__c);
            Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(workerJob.State__c));
            Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(futureInvocableJob.State__c));
            Assert.areEqual(queriesBefore + 1, Limits.getQueries());
            Assert.areEqual(dmlStatementsBefore + 1, Limits.getDmlStatements());
            Assert.areEqual(1, definitionProvider.getCallCount('findQueueDefinitionsByQueueNamesAsSystem'));
            Assert.areEqual(0, definitionProvider.getCallCount('requireQueueDefinition'));
        }
    }

    @IsTest
    static void should_fallback_to_required_definition_when_bulk_invocable_definition_lookup_misses_queue() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            Datetime currentNow = System.now();
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            queueDefinition.DefaultLeaseMinutes__c = 11;
            Job__c invocableJob = QueuesTestSupport.job(
                queueDefinition.QueueName__c,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invocable-definition-fallback'
            );
            invocableJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            invocableJob.AvailableAt__c = currentNow;
            insert invocableJob;

            QueuesTestSupport.RecordingStubProvider definitionProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('findQueueDefinitionsByQueueNamesAsSystem', new Map<String, QueueDefinition__mdt>())
                .withReturn('requireQueueDefinition', queueDefinition)
                .withReturn('getLeaseMinutes', 11);
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                definitionProvider
            );
            QueuesJobService service = new QueuesJobService(
                new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE),
                definitionStub
            );

            // Act
            List<Job__c> claimedJobs = service.claimInvocableJobsAsSystem(new Set<Id>{ invocableJob.Id }, currentNow);
            List<Job__c> emptyClaimedJobs = service.claimInvocableJobsAsSystem(new Set<Id>(), currentNow);

            // Assert
            Assert.areEqual(1, claimedJobs.size());
            Assert.areEqual(invocableJob.Id, claimedJobs[0].Id);
            Assert.areEqual(currentNow.addMinutes(11), claimedJobs[0].LeaseExpiresAt__c);
            Assert.areEqual(0, emptyClaimedJobs.size());
            Assert.areEqual(1, definitionProvider.getCallCount('findQueueDefinitionsByQueueNamesAsSystem'));
            Assert.areEqual(1, definitionProvider.getCallCount('requireQueueDefinition'));
        }
    }

    @IsTest
    static void should_claim_full_invocable_flow_batch_with_constant_query_cost() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'invocable-full-claim';
            Datetime currentNow = System.now();
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            List<Job__c> jobs = new List<Job__c>();
            for (Integer index = 0; index < 100; index++) {
                Job__c job = QueuesTestSupport.job(
                    queueName,
                    QueuesTestSupport.DEFAULT_JOB_NAME,
                    Queues.State.WAITING.name(),
                    'invocable-full-claim-' + index
                );
                job.AvailableAt__c = currentNow.addMinutes(-1);
                jobs.add(job);
            }
            insert jobs;

            List<Job__c> modeUpdates = new List<Job__c>();
            for (Job__c job : jobs) {
                job.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
                modeUpdates.add(new Job__c(Id = job.Id, ExecutionMode__c = job.ExecutionMode__c));
            }
            update modeUpdates;

            QueuesTestSupport.RecordingStubProvider definitionProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'findQueueDefinitionsByQueueNamesAsSystem',
                    new Map<String, QueueDefinition__mdt>{ queueName => queueDefinition }
                )
                .withReturn('getLeaseMinutes', 5);
            QueuesJobService service = new QueuesJobService(
                new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesDefinitionService) Test.createStub(QueuesDefinitionService.class, definitionProvider)
            );
            Integer queriesBefore = Limits.getQueries();
            Integer dmlStatementsBefore = Limits.getDmlStatements();

            // Act
            List<Job__c> claimedJobs = service.claimInvocableJobsAsSystem(QueuesSupport.toIdSet(jobs), currentNow);

            // Assert
            Assert.areEqual(100, claimedJobs.size());
            Assert.areEqual(queriesBefore + 1, Limits.getQueries());
            Assert.areEqual(dmlStatementsBefore + 1, Limits.getDmlStatements());
            Assert.areEqual(1, definitionProvider.getCallCount('findQueueDefinitionsByQueueNamesAsSystem'));
            Assert.areEqual(0, definitionProvider.getCallCount('requireQueueDefinition'));
            for (Job__c claimedJob : claimedJobs) {
                Assert.areEqual(Queues.State.ACTIVE, QueuesSupport.toState(claimedJob.State__c));
                Assert.isTrue(String.isNotBlank(claimedJob.LeaseToken__c));
            }
        }
    }

    @IsTest
    static void should_apply_processor_results_when_outcome_is_supported() {
        // Arrange
        Datetime currentNow = System.now();
        QueuesJobService service = new QueuesJobService();

        Job__c completedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        completedJob.LeaseToken__c = 'lease';
        completedJob.LeaseExpiresAt__c = currentNow.addMinutes(5);
        completedJob.AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 303);

        // Act
        service.applyResult(
            completedJob,
            new Queues.JobContext(completedJob)
                .complete(new Map<String, Object>{ 'status' => 'done' })
                .withProgress(new Map<String, Object>{ 'step' => 'complete' }),
            currentNow
        );

        Job__c pausedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        service.applyResult(
            pausedJob,
            new Queues.JobContext(pausedJob).pause(new Map<String, Object>{ 'cursor' => 2 }),
            currentNow
        );

        Job__c delayedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        service.applyResult(
            delayedJob,
            new Queues.JobContext(delayedJob).defer(new Map<String, Object>{ 'cursor' => 3 }, 5),
            currentNow
        );

        Job__c waitingJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        service.applyResult(
            waitingJob,
            new Queues.JobContext(waitingJob).defer(new Map<String, Object>{ 'cursor' => 4 }, 0),
            currentNow
        );

        // Assert
        Assert.areEqual(Queues.State.COMPLETED, QueuesSupport.toState(completedJob.State__c));
        Assert.areEqual('{"status":"done"}', completedJob.ReturnValue__c);
        Assert.areEqual('{"step":"complete"}', completedJob.Progress__c);
        Assert.areEqual(null, completedJob.LeaseToken__c);
        Assert.areEqual(null, completedJob.AsyncApexJobId__c);
        Assert.areEqual(Queues.State.PAUSED, QueuesSupport.toState(pausedJob.State__c));
        Assert.areEqual('{"cursor":2}', pausedJob.Checkpoint__c);
        Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(delayedJob.State__c));
        Assert.areEqual(currentNow.addMinutes(5), delayedJob.AvailableAt__c);
        Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(waitingJob.State__c));
    }

    @IsTest
    static void should_apply_failures_with_backoff_and_terminal_rules_when_job_fails() {
        // Arrange
        Datetime currentNow = System.now();
        QueuesJobService service = new QueuesJobService();

        Job__c fixedBackoffJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        fixedBackoffJob.AttemptsMade__c = 1;
        fixedBackoffJob.MaxAttempts__c = 3;
        fixedBackoffJob.BackoffType__c = Queues.Backoff.FIXED.name();
        fixedBackoffJob.BackoffValue__c = 3;
        fixedBackoffJob.AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 304);

        Job__c exponentialBackoffJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        exponentialBackoffJob.AttemptsMade__c = 3;
        exponentialBackoffJob.MaxAttempts__c = 5;
        exponentialBackoffJob.BackoffType__c = Queues.Backoff.EXPONENTIAL.name();
        exponentialBackoffJob.BackoffValue__c = 2;

        Job__c exhaustedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        exhaustedJob.AttemptsMade__c = 3;
        exhaustedJob.MaxAttempts__c = 3;

        Job__c unrecoverableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());

        // Act
        service.applyFailure(fixedBackoffJob, 'fixed retry', currentNow);
        service.applyFailure(exponentialBackoffJob, 'exp retry', currentNow);
        service.applyFailure(exhaustedJob, 'exhausted', currentNow);
        service.applyFailure(unrecoverableJob, '   ', currentNow, true);
        Integer retryDelayMinutes = QueuesJobService.resolveRetryDelayMinutes(
            QueuesTestSupport.job(Queues.State.ACTIVE.name()),
            1
        );

        // Assert
        Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(fixedBackoffJob.State__c));
        Assert.areEqual(currentNow.addMinutes(3), fixedBackoffJob.AvailableAt__c);
        Assert.areEqual(null, fixedBackoffJob.AsyncApexJobId__c);
        Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(exponentialBackoffJob.State__c));
        Assert.areEqual(currentNow.addMinutes(8), exponentialBackoffJob.AvailableAt__c);
        Assert.areEqual(Queues.State.FAILED, QueuesSupport.toState(exhaustedJob.State__c));
        Assert.areEqual(Queues.State.FAILED, QueuesSupport.toState(unrecoverableJob.State__c));
        Assert.areEqual(QueuesSupport.DEFAULT_EXCEPTION_REASON, unrecoverableJob.FailedReason__c);
        Assert.areEqual(0, retryDelayMinutes);
    }

    @IsTest
    static void should_apply_jittered_backoff_to_retry_available_at() {
        // Arrange
        Datetime currentNow = System.now();
        QueuesJobService service = new QueuesJobService();

        Job__c jitteredJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        jitteredJob.AttemptsMade__c = 1;
        jitteredJob.MaxAttempts__c = 3;
        jitteredJob.BackoffType__c = Queues.Backoff.FIXED.name();
        jitteredJob.BackoffValue__c = 10;
        jitteredJob.BackoffJitter__c = 0.5;

        Job__c immediateJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        immediateJob.AttemptsMade__c = 1;
        immediateJob.MaxAttempts__c = 3;
        immediateJob.BackoffType__c = Queues.Backoff.FIXED.name();
        immediateJob.BackoffValue__c = 10;
        immediateJob.BackoffJitter__c = 1;

        try {
            // Act
            QueuesSupport.forcedRandom = 0.25;
            Integer jitteredDelaySeconds = QueuesJobService.resolveRetryDelaySeconds(jitteredJob, 1);
            service.applyFailure(jitteredJob, 'jittered retry', currentNow);

            QueuesSupport.forcedRandom = 0.0;
            Integer immediateDelaySeconds = QueuesJobService.resolveRetryDelaySeconds(immediateJob, 1);
            service.applyFailure(immediateJob, 'immediate retry', currentNow);

            // Assert
            Assert.areEqual(375, jitteredDelaySeconds);
            Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(jitteredJob.State__c));
            Assert.areEqual(currentNow.addSeconds(375), jitteredJob.AvailableAt__c);
            Assert.areEqual(0, immediateDelaySeconds);
            Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(immediateJob.State__c));
            Assert.areEqual(currentNow, immediateJob.AvailableAt__c);
        } finally {
            QueuesSupport.forcedRandom = null;
        }
    }

    @IsTest
    static void should_map_jobs_when_options_and_defaults_are_combined() {
        // Arrange
        Datetime currentNow = System.now();
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        queueDefinition.DefaultAttempts__c = 5;
        queueDefinition.DefaultBackoffType__c = Queues.Backoff.FIXED.name();
        queueDefinition.DefaultBackoffValue__c = 4;
        queueDefinition.DefaultBackoffJitter__c = 0.4;
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
        Id parentJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 51);

        Queues.JobData immediateJobData = new Queues.JobData(
            QueuesTestSupport.DEFAULT_JOB_NAME,
            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            new Map<String, Object>{ 'invoiceId' => 'INV-901' }
        );
        Queues.JobData delayedJobData = new Queues.JobData(
            QueuesTestSupport.DEFAULT_JOB_NAME,
            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            new Map<String, Object>{ 'invoiceId' => 'INV-902' },
            new Queues.JobOptions()
                .jobId('job-902')
                .category('checkout')
                .groupKey('ORDER-902')
                .correlationId('checkout-902')
                .availableAt(currentNow.addMinutes(6))
                .attempts(7)
                .priority(2)
                .backoff(Queues.Backoff.NONE, 9)
                .parent(parentJobId)
                .failParentOnFailure()
                .ignoreDependencyOnCanceled()
        );

        QueuesJobService service = new QueuesJobService();

        // Act
        Job__c immediateJob = service.mapToJob(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            immediateJobData,
            queueDefinition,
            processorDefinition,
            currentNow,
            Queues.ExecutionMode.WORKER
        );
        Job__c delayedJob = service.mapToJob(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            delayedJobData,
            queueDefinition,
            processorDefinition,
            currentNow,
            Queues.ExecutionMode.QUEUEABLE_SERIAL
        );

        Datetime earliestWakeAt = service.findEarliestWakeAt(
            new List<Job__c>{
                null,
                new Job__c(State__c = Queues.State.COMPLETED.name(), AvailableAt__c = currentNow.addMinutes(-5)),
                new Job__c(State__c = Queues.State.WAITING.name(), AvailableAt__c = currentNow.addMinutes(3)),
                new Job__c(State__c = Queues.State.DELAYED.name())
            },
            currentNow
        );

        // Assert
        Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(immediateJob.State__c));
        Assert.areEqual(5, QueuesSupport.toInteger(immediateJob.MaxAttempts__c, 0));
        Assert.areEqual(Queues.Backoff.FIXED.name(), immediateJob.BackoffType__c);
        Assert.areEqual(4, QueuesSupport.toInteger(immediateJob.BackoffValue__c, 0));
        Assert.areEqual(0.4, immediateJob.BackoffJitter__c);
        Assert.areEqual(Queues.ExecutionMode.WORKER.name(), immediateJob.ExecutionMode__c);
        Assert.areEqual('DEFAULT', immediateJob.DependencyFailurePolicy__c);
        Assert.areEqual('DEFAULT', immediateJob.DependencyCanceledPolicy__c);

        Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(delayedJob.State__c));
        Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), delayedJob.ExecutionMode__c);
        Assert.areEqual('job-902', delayedJob.ExternalId__c);
        Assert.areEqual('checkout', delayedJob.Category__c);
        Assert.areEqual('ORDER-902', delayedJob.GroupKey__c);
        Assert.areEqual('checkout-902', delayedJob.CorrelationId__c);
        Assert.areEqual(
            QueuesSupport.buildQueueScopedExternalId(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'job-902'),
            delayedJob.QueueScopedExternalId__c
        );
        Assert.areEqual(7, QueuesSupport.toInteger(delayedJob.MaxAttempts__c, 0));
        Assert.areEqual(2, QueuesSupport.toInteger(delayedJob.Priority__c, 0));
        Assert.areEqual(Queues.Backoff.NONE.name(), delayedJob.BackoffType__c);
        Assert.areEqual(0, QueuesSupport.toInteger(delayedJob.BackoffValue__c, 0));
        Assert.areEqual(0.0, QueuesSupport.toDecimal(delayedJob.BackoffJitter__c, 0.0));
        Assert.areEqual(parentJobId, delayedJob.Parent__c);
        Assert.areEqual('FAIL_PARENT', delayedJob.DependencyFailurePolicy__c);
        Assert.areEqual('IGNORE_DEPENDENCY', delayedJob.DependencyCanceledPolicy__c);
        Assert.areEqual(currentNow, earliestWakeAt);
    }

    @IsTest
    static void should_map_delayed_job_from_relative_delay_when_available_at_is_not_provided() {
        // Arrange
        Datetime currentNow = System.now();
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
        Queues.JobData delayedJobData = new Queues.JobData(
            QueuesTestSupport.DEFAULT_JOB_NAME,
            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            new Map<String, Object>{ 'invoiceId' => 'INV-903' },
            new Queues.JobOptions().delay(7)
        );

        QueuesJobService service = new QueuesJobService();

        // Act
        Job__c delayedJob = service.mapToJob(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            delayedJobData,
            queueDefinition,
            processorDefinition,
            currentNow,
            Queues.ExecutionMode.WORKER
        );

        // Assert
        Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(delayedJob.State__c));
        Assert.areEqual(currentNow.addMinutes(7), delayedJob.AvailableAt__c);
    }

    @IsTest
    static void should_insert_jobs_allowing_partial_success_when_jobs_are_provided() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            Job__c queuedJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            List<Database.SaveResult> expectedSaveResults = buildInsertSaveResults(new List<Job__c>{ queuedJob });
            QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns(
                    'insertJobsAllowingPartialSuccess',
                    new List<Object>{ expectedSaveResults, new List<Database.SaveResult>() }
                );
            QueuesJobRepository repositoryStub = (QueuesJobRepository) Test.createStub(
                QueuesJobRepository.class,
                repositoryProvider
            );

            QueuesJobService service = new QueuesJobService(repositoryStub, new QueuesDefinitionService());

            // Act
            List<Database.SaveResult> saveResults = service.insertJobsAllowingPartialSuccess(
                new List<Job__c>{ queuedJob }
            );
            List<Database.SaveResult> emptySaveResults = service.insertJobsAllowingPartialSuccess(new List<Job__c>());

            // Assert
            Assert.areEqual(1, saveResults.size());
            Assert.areEqual(true, saveResults[0].isSuccess());
            Assert.areEqual(0, emptySaveResults.size());
            Assert.areEqual(2, repositoryProvider.getCallCount('insertJobsAllowingPartialSuccess'));
        }
    }

    @IsTest
    static void should_insert_update_and_delete_jobs_when_mutation_inputs_are_nonempty() {
        // Arrange
        Job__c queuedJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        queuedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 64);
        Job__c updatedJob = (Job__c) QueuesSupport.clone(queuedJob);
        updatedJob.Progress__c = '{"step":1}';

        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('insertJobs', new List<Job__c>{ queuedJob })
            .withReturn('updateJobs', new List<Job__c>{ updatedJob });
        QueuesJobRepository repositoryStub = (QueuesJobRepository) Test.createStub(
            QueuesJobRepository.class,
            repositoryProvider
        );

        QueuesJobService service = new QueuesJobService(repositoryStub, new QueuesDefinitionService());

        // Act
        List<Job__c> insertedJobs = service.insertJobs(new List<Job__c>{ queuedJob });
        Job__c returnedUpdatedJob = service.updateJob(updatedJob);
        service.deleteJobs(new List<Job__c>{ updatedJob });

        // Assert
        Assert.areEqual(1, insertedJobs.size());
        Assert.areEqual(queuedJob.Id, insertedJobs[0].Id);
        Assert.areEqual(updatedJob.Id, returnedUpdatedJob.Id);
        Assert.areEqual(1, repositoryProvider.getCallCount('insertJobs'));
        Assert.areEqual(1, repositoryProvider.getCallCount('updateJobs'));
        Assert.areEqual(1, repositoryProvider.getCallCount('deleteJobs'));
    }

    @IsTest
    static void should_require_existing_job_and_ignore_empty_mutations_when_inputs_are_empty() {
        // Arrange
        Id missingJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 61);
        Job__c existingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        existingJob.Id = missingJobId;
        Job__c lockedJob = (Job__c) QueuesSupport.clone(existingJob);

        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns('queryJobsByIds', new List<Object>{ new List<Job__c>(), new List<Job__c>{ existingJob } })
            .withReturn('queryJobsByIdsForUpdate', new List<Job__c>{ lockedJob })
            .withReturn('insertJobs', new List<Job__c>())
            .withReturn('updateJobs', new List<Job__c>())
            .withReturn('queryDueJobs', new List<Job__c>());
        QueuesJobRepository repositoryStub = (QueuesJobRepository) Test.createStub(
            QueuesJobRepository.class,
            repositoryProvider
        );

        QueuesJobService service = new QueuesJobService(repositoryStub, new QueuesDefinitionService());

        String missingJobMessage;

        // Act
        try {
            service.requireJob(missingJobId);
            Assert.fail('Expected missing job to throw.');
        } catch (Queues.JobNotFoundException ex) {
            missingJobMessage = ex.getMessage();
        }

        Job__c requiredJob = service.requireJob(missingJobId);
        Job__c requiredLockedJob = service.requireJobForUpdate(missingJobId);
        List<Job__c> insertedJobs = service.insertJobs(new List<Job__c>());
        List<Job__c> updatedJobs = service.updateJobs(new List<Job__c>());
        service.deleteJobs(null);
        List<Job__c> dueJobs = service.queryDueJobsAsSystem(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            new Set<Queues.State>(),
            System.now(),
            0
        );

        // Assert
        Assert.areEqual(true, missingJobMessage.contains(missingJobId));
        Assert.areEqual(missingJobId, requiredJob.Id);
        Assert.areEqual(missingJobId, requiredLockedJob.Id);
        Assert.areEqual(0, insertedJobs.size());
        Assert.areEqual(0, updatedJobs.size());
        Assert.areEqual(0, dueJobs.size());
        Assert.areEqual(1, repositoryProvider.getCallCount('insertJobs'));
        Assert.areEqual(1, repositoryProvider.getCallCount('updateJobs'));
        Assert.areEqual(1, repositoryProvider.getCallCount('deleteJobs'));
        Assert.areEqual(1, repositoryProvider.getCallCount('queryDueJobs'));
    }

    @IsTest
    static void should_query_jobs_when_job_ids_are_provided() {
        // Arrange
        Job__c existingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        existingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 62);

        QueuesJobRepository repositoryStub = (QueuesJobRepository) Test.createStub(
            QueuesJobRepository.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryJobsByIds', new List<Job__c>{ existingJob })
                .withReturn('queryJobsByIdsForUpdate', new List<Job__c>{ existingJob })
        );

        QueuesJobService service = new QueuesJobService(repositoryStub, new QueuesDefinitionService());

        // Act
        List<Job__c> jobs = service.queryJobsAsSystem(new Set<Id>{ existingJob.Id });
        List<Job__c> lockedJobs = service.queryJobsForUpdate(new Set<Id>{ existingJob.Id });

        // Assert
        Assert.areEqual(1, jobs.size());
        Assert.areEqual(existingJob.Id, jobs[0].Id);
        Assert.areEqual(1, lockedJobs.size());
        Assert.areEqual(existingJob.Id, lockedJobs[0].Id);
    }

    @IsTest
    static void should_query_job_views_and_next_dispatch_when_repository_results_vary() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'job-service-views-' + System.now().getTime();
            Datetime currentNow = System.now();
            Job__c nextDispatchJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            nextDispatchJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 63);
            Job__c expiredJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
            Job__c drainableJob = QueuesTestSupport.job(Queues.State.PAUSED.name());
            Job__c delayedJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.DELAYED.name(),
                'job-delay'
            );
            delayedJob.AvailableAt__c = currentNow.addMinutes(5);
            insert delayedJob;

            AggregateResult nextPumpRow = [
                SELECT MIN(AvailableAt__c) nextPumpAt
                FROM Job__c
                WHERE
                    QueueName__c = :queueName
                    AND State__c IN :new Set<String>{ Queues.State.WAITING.name(), Queues.State.DELAYED.name() }
                    AND AvailableAt__c > :currentNow
            ][0];
            Map<String, Integer> stateCountsByValue = new Map<String, Integer>{
                Queues.State.WAITING.name() => 1,
                Queues.State.FAILED.name() => 1
            };

            QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns(
                    'queryDueJobs',
                    new List<Object>{ new List<Job__c>(), new List<Job__c>{ nextDispatchJob } }
                )
                .withReturns(
                    'queryExpiredActiveJobsForUpdate',
                    new List<Object>{
                        new List<Job__c>{ expiredJob },
                        new List<Job__c>{ expiredJob },
                        new List<Job__c>{ expiredJob },
                        new List<Job__c>()
                    }
                )
                .withReturn('queryExpiredActiveQueueableJobsForUpdate', new List<Job__c>{ expiredJob })
                .withReturn('queryStaleInitiatedQueueableJobsForUpdate', new List<Job__c>{ expiredJob })
                .withReturn('queryExpiredActiveInvocableJobsForUpdate', new List<Job__c>{ expiredJob })
                .withReturn('hasJobsBySchedulerId', true)
                .withReturn('queryJobsBySchedulerIdForUpdate', new List<Job__c>{ drainableJob })
                .withReturns(
                    'queryDrainableJobsForUpdate',
                    new List<Object>{
                        new List<Job__c>{ drainableJob },
                        new List<Job__c>{ drainableJob },
                        new List<Job__c>{ drainableJob },
                        new List<Job__c>()
                    }
                )
                .withReturns(
                    'queryNextPumpAt',
                    new List<Object>{ new List<AggregateResult>(), new List<AggregateResult>{ nextPumpRow } }
                )
                .withReturn('queryEmptyJobLocator', QueuesTestSupport.emptyJobLocator())
                .withReturn('queryQueueStateCounts', stateCountsByValue)
                .withReturn('queryJobsByExternalIds', new List<Job__c>())
                .withReturn('queryJobsByIdsForUpdate', new List<Job__c>());
            QueuesJobRepository repositoryStub = (QueuesJobRepository) Test.createStub(
                QueuesJobRepository.class,
                repositoryProvider
            );

            QueuesJobService service = new QueuesJobService(repositoryStub, new QueuesDefinitionService());

            // Act
            Job__c missingDispatchJob = service.findNextDispatchJobAsSystem(queueName, currentNow);
            Job__c foundDispatchJob = service.findNextDispatchJobAsSystem(queueName, currentNow);
            List<Job__c> expiredJobs = service.queryExpiredActiveJobsForUpdate(queueName, currentNow, 10);
            List<Job__c> lockedExpiredJobs = service.queryExpiredActiveJobsForUpdate(queueName, currentNow, 10);
            List<Job__c> limitedLockedExpiredJobs = service.queryExpiredActiveJobsForUpdate(queueName, currentNow, 1);
            List<Job__c> emptyLockedExpiredJobs = service.queryExpiredActiveJobsForUpdate(queueName, currentNow, 0);
            List<Job__c> lockedExpiredQueueableJobs = service.queryExpiredActiveQueueableJobsForUpdate(
                queueName,
                currentNow,
                1
            );
            List<Job__c> staleInitiatedQueueableJobs = service.queryStaleInitiatedQueueableJobsForUpdate(
                queueName,
                currentNow.addMinutes(-5),
                currentNow,
                1
            );
            List<Job__c> staleInitiatedQueueableJobsAsSystem = service.queryStaleInitiatedQueueableJobsForUpdateAsSystem(
                queueName,
                currentNow.addMinutes(-5),
                currentNow,
                1
            );
            List<Job__c> lockedExpiredInvocableJobs = service.queryExpiredActiveInvocableJobsForUpdateAsSystem(
                queueName,
                currentNow,
                1
            );
            List<Job__c> drainableJobs = service.queryDrainableJobsForUpdate(queueName, 10);
            Boolean hasSchedulerPendingJobs = service.hasJobsBySchedulerId(
                QueuesTestSupport.fakeId(JobScheduler__c.SObjectType, 1),
                new Set<Queues.State>{ Queues.State.WAITING }
            );
            List<Job__c> schedulerPendingJobs = service.queryJobsBySchedulerIdForUpdate(
                QueuesTestSupport.fakeId(JobScheduler__c.SObjectType, 1),
                new Set<Queues.State>{ Queues.State.WAITING },
                10
            );
            List<Job__c> schedulerJobs = service.queryJobsBySchedulerIdForUpdate(
                QueuesTestSupport.fakeId(JobScheduler__c.SObjectType, 1),
                10
            );
            List<Job__c> lockedDrainableJobs = service.queryDrainableJobsForUpdate(queueName, 10);
            List<Job__c> limitedLockedDrainableJobs = service.queryDrainableJobsForUpdate(queueName, 1);
            List<Job__c> emptyLockedDrainableJobs = service.queryDrainableJobsForUpdate(queueName, 0);
            Datetime missingNextPumpAt = service.queryNextPumpAtAsSystem(queueName, currentNow);
            Datetime nextPumpAt = service.queryNextPumpAtAsSystem(queueName, currentNow);
            Map<String, Integer> queriedStateCountsByValue = service.queryQueueStateCounts(queueName);
            Database.QueryLocator emptyLocator = service.emptyJobLocator();
            Map<String, Job__c> jobsByExternalId = service.getJobsByExternalId(queueName, new Set<String>());
            List<Job__c> unlockedJobs = service.queryJobsForUpdate(null);

            // Assert
            Assert.areEqual(null, missingDispatchJob);
            Assert.areEqual(nextDispatchJob.Id, foundDispatchJob.Id);
            Assert.areEqual(1, expiredJobs.size());
            Assert.areEqual(1, lockedExpiredJobs.size());
            Assert.areEqual(1, limitedLockedExpiredJobs.size());
            Assert.areEqual(0, emptyLockedExpiredJobs.size());
            Assert.areEqual(1, lockedExpiredQueueableJobs.size());
            Assert.areEqual(1, staleInitiatedQueueableJobs.size());
            Assert.areEqual(1, staleInitiatedQueueableJobsAsSystem.size());
            Assert.areEqual(1, lockedExpiredInvocableJobs.size());
            Assert.areEqual(1, drainableJobs.size());
            Assert.areEqual(true, hasSchedulerPendingJobs);
            Assert.areEqual(1, schedulerPendingJobs.size());
            Assert.areEqual(1, schedulerJobs.size());
            Assert.areEqual(1, lockedDrainableJobs.size());
            Assert.areEqual(1, limitedLockedDrainableJobs.size());
            Assert.areEqual(0, emptyLockedDrainableJobs.size());
            Assert.areEqual(null, missingNextPumpAt);
            Assert.areNotEqual(null, nextPumpAt);
            Assert.areEqual(2, queriedStateCountsByValue.size());
            Assert.areEqual(1, queriedStateCountsByValue.get(Queues.State.WAITING.name()));
            Assert.areNotEqual(null, emptyLocator);
            Assert.areEqual(0, jobsByExternalId.size());
            Assert.areEqual(0, unlockedJobs.size());
            Assert.areEqual(1, repositoryProvider.getCallCount('queryJobsByExternalIds'));
            Assert.areEqual(1, repositoryProvider.getCallCount('hasJobsBySchedulerId'));
            Assert.areEqual(2, repositoryProvider.getCallCount('queryJobsBySchedulerIdForUpdate'));
        }
    }

    @IsTest
    static void should_delegate_active_job_presence_checks_to_repository() {
        // Arrange
        String queueName = 'job-service-has-active';
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('hasJobsInState', true);
        QueuesJobRepository repositoryStub = (QueuesJobRepository) Test.createStub(
            QueuesJobRepository.class,
            repositoryProvider
        );

        QueuesJobService service = new QueuesJobService(repositoryStub, new QueuesDefinitionService());

        // Act
        Boolean hasActiveJobs = service.hasActiveJobs(queueName);

        // Assert
        Assert.areEqual(true, hasActiveJobs);
        Assert.areEqual(1, repositoryProvider.getCallCount('hasJobsInState'));
        Assert.areEqual(queueName, (String) repositoryProvider.getLastArgs('hasJobsInState')[0]);
        Assert.areEqual(Queues.State.ACTIVE.name(), (String) repositoryProvider.getLastArgs('hasJobsInState')[1]);
    }

    @IsTest
    static void should_apply_failed_results_and_throw_when_result_is_missing() {
        // Arrange
        Datetime currentNow = System.now();
        Job__c failedResultJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        failedResultJob.AttemptsMade__c = 0;
        failedResultJob.MaxAttempts__c = 2;
        Job__c exponentialJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        exponentialJob.BackoffType__c = Queues.Backoff.EXPONENTIAL.name();
        exponentialJob.BackoffValue__c = 2;

        QueuesJobService service = new QueuesJobService();

        String missingResultMessage;

        // Act
        service.applyResult(
            failedResultJob,
            new Queues.JobContext(failedResultJob).fail('processor failed'),
            currentNow
        );
        Integer retryDelay = QueuesJobService.resolveRetryDelayMinutes(exponentialJob, 0);
        try {
            service.applyResult(QueuesTestSupport.job(Queues.State.ACTIVE.name()), null, currentNow);
            Assert.fail('Expected null result to throw.');
        } catch (Queues.QueueException ex) {
            missingResultMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(failedResultJob.State__c));
        Assert.areEqual('processor failed', failedResultJob.FailedReason__c);
        Assert.areEqual(2, retryDelay);
        Assert.areEqual('Processor returned a null ProcessResult.', missingResultMessage);
    }

    @IsTest
    static void should_leave_worker_retry_delay_minutes_uncapped() {
        // Arrange
        Datetime currentNow = System.now();
        QueuesJobService service = new QueuesJobService();

        Job__c fixedBackoffJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        fixedBackoffJob.AttemptsMade__c = 1;
        fixedBackoffJob.MaxAttempts__c = 10;
        fixedBackoffJob.BackoffType__c = Queues.Backoff.FIXED.name();
        fixedBackoffJob.BackoffValue__c = 50000;

        Job__c exponentialBackoffJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        exponentialBackoffJob.AttemptsMade__c = 4;
        exponentialBackoffJob.MaxAttempts__c = 50;
        exponentialBackoffJob.BackoffType__c = Queues.Backoff.EXPONENTIAL.name();
        exponentialBackoffJob.BackoffValue__c = 2;

        // Act
        Integer fixedRetryDelay = QueuesJobService.resolveRetryDelayMinutes(fixedBackoffJob, 1);
        Integer exponentialRetryDelay = QueuesJobService.resolveRetryDelayMinutes(exponentialBackoffJob, 4);
        service.applyFailure(fixedBackoffJob, 'fixed retry', currentNow);
        service.applyFailure(exponentialBackoffJob, 'exp retry', currentNow);

        // Assert
        Assert.areEqual(50000, fixedRetryDelay);
        Assert.areEqual(16, exponentialRetryDelay);
        Assert.areEqual(currentNow.addMinutes(50000), fixedBackoffJob.AvailableAt__c);
        Assert.areEqual(currentNow.addMinutes(16), exponentialBackoffJob.AvailableAt__c);
    }

    @IsTest
    static void should_cap_queueable_retry_delay_minutes_to_native_queueable_limit() {
        // Arrange
        Datetime currentNow = System.now();
        QueuesJobService service = new QueuesJobService();

        Job__c fixedBackoffJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        fixedBackoffJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        fixedBackoffJob.AttemptsMade__c = 1;
        fixedBackoffJob.MaxAttempts__c = 10;
        fixedBackoffJob.BackoffType__c = Queues.Backoff.FIXED.name();
        fixedBackoffJob.BackoffValue__c = 50000;

        Job__c exponentialBackoffJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        exponentialBackoffJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        exponentialBackoffJob.AttemptsMade__c = 40;
        exponentialBackoffJob.MaxAttempts__c = 50;
        exponentialBackoffJob.BackoffType__c = Queues.Backoff.EXPONENTIAL.name();
        exponentialBackoffJob.BackoffValue__c = 2;

        // Act
        Integer fixedRetryDelay = QueuesJobService.resolveRetryDelayMinutes(fixedBackoffJob, 1);
        Integer exponentialRetryDelay = QueuesJobService.resolveRetryDelayMinutes(exponentialBackoffJob, 40);
        service.applyFailure(fixedBackoffJob, 'fixed retry', currentNow);
        service.applyFailure(exponentialBackoffJob, 'exp retry', currentNow);

        // Assert
        Assert.areEqual(QueuesQueueableTransportService.NATIVE_MAX_QUEUEABLE_DELAY_MINUTES, fixedRetryDelay);
        Assert.areEqual(QueuesQueueableTransportService.NATIVE_MAX_QUEUEABLE_DELAY_MINUTES, exponentialRetryDelay);
        Assert.areEqual(
            currentNow.addMinutes(QueuesQueueableTransportService.NATIVE_MAX_QUEUEABLE_DELAY_MINUTES),
            fixedBackoffJob.AvailableAt__c
        );
        Assert.areEqual(
            currentNow.addMinutes(QueuesQueueableTransportService.NATIVE_MAX_QUEUEABLE_DELAY_MINUTES),
            exponentialBackoffJob.AvailableAt__c
        );
    }

    private static List<Database.SaveResult> buildInsertSaveResults(final List<Job__c> jobs) {
        List<Job__c> insertCandidates = new List<Job__c>();
        for (Job__c job : jobs) {
            insertCandidates.add((Job__c) QueuesSupport.clone(job));
        }
        return Database.insert(insertCandidates, false);
    }
}
