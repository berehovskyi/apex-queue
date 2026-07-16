@IsTest(IsParallel=false)
private class QueuesJobSchedulerServiceTest {
    private static final String CANCELED_BY_SCHEDULER_REPLACEMENT_REASON = 'Canceled by job scheduler replacement.';
    private static final String CANCELED_BY_SCHEDULER_CANCEL_REASON = 'Canceled by job scheduler cancel.';

    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_upsert_every_scheduler_and_materialize_initial_job() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-every-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 801);
            Datetime firstRunAt = System.now().addMinutes(5);
            Job__c parentJob = QueuesTestSupport.job(
                queueName,
                'parent-job',
                Queues.State.WAITING.name(),
                'parent-job'
            );
            insert parentJob;
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', scheduledJobId);
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            Job__c job = service.upsertJobScheduler(
                queueName,
                'invoice-granular',
                new Queues.RepeatOptions().every(5).startAt(firstRunAt).lim(2),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
                    .data(new Map<String, Object>{ 'invoiceId' => 'INV-EVERY' })
                    .opts(
                        new Queues.JobOptions()
                            .priority(2)
                            .attempts(4)
                            .category('billing')
                            .groupKey('ORDER-EVERY')
                            .correlationId('scheduler-correlation')
                            .backoff(Queues.Backoff.FIXED, 7)
                            .parent(parentJob.Id)
                    )
            );

            // Assert
            JobScheduler__c scheduler = queryOnlyScheduler(queueName, 'invoice-granular');
            Assert.areNotEqual(null, job.Id);
            Assert.areEqual(scheduler.Id, job.JobScheduler__c);
            Assert.areEqual(parentJob.Id, job.Parent__c);
            Assert.areEqual('billing', job.Category__c);
            Assert.areEqual('ORDER-EVERY', job.GroupKey__c);
            Assert.areEqual('scheduler-correlation', job.CorrelationId__c);
            Assert.areEqual(Queues.Backoff.FIXED.name(), job.BackoffType__c);
            Assert.areEqual(7, QueuesSupport.toInteger(job.BackoffValue__c, 0));
            Assert.areEqual(Queues.State.DELAYED.name(), job.State__c);
            Assert.areEqual(firstRunAt, job.AvailableAt__c);
            Assert.areEqual('scheduler:' + scheduler.Id + ':1', job.ExternalId__c);
            Assert.areEqual(scheduledJobId, scheduler.ScheduledJobId__c);
            Assert.areEqual(1, QueuesSupport.toInteger(scheduler.IterationCount__c, 0));
            Assert.areEqual(5, QueuesSupport.toInteger(scheduler.EveryMinutes__c, 0));
            Assert.areEqual(1, scheduleProvider.getCallCount('scheduleEvery'));
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_default_every_scheduler_without_startAt_to_safe_future_run() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-every-default-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 843);
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', scheduledJobId);
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );
            Datetime beforeUpsert = System.now();

            // Act
            Job__c job = service.upsertJobScheduler(
                queueName,
                'default-start',
                new Queues.RepeatOptions().every(5),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
            );
            Datetime afterUpsert = System.now();

            // Assert
            JobScheduler__c scheduler = queryOnlyScheduler(queueName, 'default-start');
            Datetime scheduledRunAt = (Datetime) scheduleProvider.getLastArgs('scheduleEvery')[1];
            Assert.areEqual(scheduledJobId, scheduler.ScheduledJobId__c);
            Assert.areEqual(1, QueuesSupport.toInteger(scheduler.IterationCount__c, 0));
            Assert.areEqual(job.AvailableAt__c, scheduler.NextRunAt__c);
            Assert.areEqual(job.AvailableAt__c, scheduledRunAt);
            Assert.isTrue(job.AvailableAt__c >= beforeUpsert.addMinutes(1));
            Assert.isTrue(job.AvailableAt__c <= afterUpsert.addMinutes(2));
            Assert.areEqual(1, scheduleProvider.getCallCount('scheduleEvery'));
        }
    }

    @IsTest
    static void should_default_scheduler_template_to_scheduler_id() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-template-default-' + System.now().getTime();
            String jobSchedulerId = QueuesTestSupport.DEFAULT_PROCESSOR_CLASS;
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 844);
            Datetime firstRunAt = System.now().addMinutes(5);
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', scheduledJobId);
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesWorkerService) Test.createStub(
                    QueuesWorkerService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                )
            );

            // Act
            Job__c job = service.upsertJobScheduler(
                queueName,
                jobSchedulerId,
                new Queues.RepeatOptions().every(5).startAt(firstRunAt),
                null
            );

            // Assert
            JobScheduler__c scheduler = queryOnlyScheduler(queueName, jobSchedulerId);
            Assert.areEqual(jobSchedulerId, job.Name__c);
            Assert.areEqual(jobSchedulerId, job.ProcessorClass__c);
            Assert.areEqual(jobSchedulerId, scheduler.JobName__c);
            Assert.areEqual(jobSchedulerId, scheduler.ProcessorClass__c);
        }
    }

    @IsTest
    static void should_reconcile_duplicate_scheduler_insert_when_upsert_races() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-upsert-race-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 848);
            Datetime firstRunAt = System.now().addMinutes(5);
            RacingJobSchedulerRepository schedulerRepository = new RacingJobSchedulerRepository();
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', scheduledJobId);
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider),
                schedulerRepository,
                new QueuesJobService(),
                new QueuesErrorService(),
                true
            );

            // Act
            Job__c job = service.upsertJobScheduler(
                queueName,
                'race-me',
                new Queues.RepeatOptions().every(5).startAt(firstRunAt),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
            );

            // Assert
            JobScheduler__c scheduler = queryOnlyScheduler(queueName, 'race-me');
            Assert.areEqual(
                1,
                [SELECT COUNT() FROM JobScheduler__c WHERE QueueName__c = :queueName AND Name = 'race-me']
            );
            Assert.areEqual(scheduler.Id, job.JobScheduler__c);
            Assert.areEqual('scheduler:' + scheduler.Id + ':1', job.ExternalId__c);
            Assert.areEqual(scheduledJobId, scheduler.ScheduledJobId__c);
            Assert.areEqual(1, QueuesSupport.toInteger(scheduler.IterationCount__c, 0));
            Assert.areEqual(1, schedulerRepository.insertAttempts);
            Assert.areEqual(2, schedulerRepository.queryCount);
            Assert.isTrue(schedulerRepository.insertedCompetingScheduler);
            Assert.areEqual(1, scheduleProvider.getCallCount('scheduleEvery'));
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_surface_non_duplicate_scheduler_insert_failure() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-insert-failure-' + System.now().getTime();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesWorkerService(),
                new FailingJobSchedulerRepository(),
                new QueuesJobService(),
                new QueuesErrorService(),
                true
            );
            Boolean failed = false;

            // Act
            try {
                service.upsertJobScheduler(
                    queueName,
                    'insert-failure',
                    new Queues.RepeatOptions().every(5),
                    new Queues.JobTemplate(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                    )
                );
                Assert.fail('Expected scheduler insert failure to surface.');
            } catch (DmlException ex) {
                failed = true;
            }

            // Assert
            Assert.isTrue(failed);
            Assert.areEqual(0, [SELECT COUNT() FROM JobScheduler__c WHERE QueueName__c = :queueName]);
        }
    }

    @IsTest
    static void should_surface_unreconciled_duplicate_scheduler_insert_race() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-unreconciled-race-' + System.now().getTime();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesWorkerService(),
                new UnreconciledDuplicateJobSchedulerRepository(),
                new QueuesJobService(),
                new QueuesErrorService(),
                true
            );
            Boolean failed = false;

            // Act
            try {
                service.upsertJobScheduler(
                    queueName,
                    'hidden-race',
                    new Queues.RepeatOptions().every(5),
                    new Queues.JobTemplate(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                    )
                );
                Assert.fail('Expected unreconciled duplicate scheduler insert to surface.');
            } catch (DmlException ex) {
                failed = true;
            }

            // Assert
            Assert.isTrue(failed);
            Assert.areEqual(0, [SELECT COUNT() FROM JobScheduler__c WHERE QueueName__c = :queueName]);
        }
    }

    @IsTest
    static void should_cancel_pending_jobs_and_replace_previous_schedule_when_upserting_existing_scheduler() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-upsert-replace-' + System.now().getTime();
            Id firstScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 841);
            Id secondScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 842);
            Id staleScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 844);
            Datetime firstRunAt = System.now().addMinutes(5);
            Datetime secondRunAt = System.now().addMinutes(10);
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns('scheduleEvery', new List<Object>{ firstScheduledJobId, secondScheduledJobId });
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobsByNamePrefix',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            firstScheduledJobId,
                            'WAITING',
                            '0 0 0 * * ?',
                            'Apex Queue Every Scheduler Test',
                            firstRunAt
                        ),
                        QueuesTestSupport.cronTrigger(
                            staleScheduledJobId,
                            'WAITING',
                            '0 0 0 * * ?',
                            'Apex Queue Every Scheduler Test',
                            firstRunAt
                        )
                    }
                );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider),
                QueuesTestSupport.newRecordingQueueEventService(queueName, 'CANCELED', eventProvider)
            );

            Job__c firstJob = service.upsertJobScheduler(
                queueName,
                'replace-me',
                new Queues.RepeatOptions().every(5).startAt(firstRunAt),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
                    .data(new Map<String, Object>{ 'version' => 'old' })
            );
            insert QueuesTestSupport.jobRun(firstJob.Id, 1, Queues.RunStatus.FAILED.name());
            JobScheduler__c firstScheduler = queryOnlyScheduler(queueName, 'replace-me');

            // Act
            Job__c secondJob = service.upsertJobScheduler(
                queueName,
                'replace-me',
                new Queues.RepeatOptions().every(10).startAt(secondRunAt).lim(5),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
                    .data(new Map<String, Object>{ 'version' => 'new' })
                    .opts(new Queues.JobOptions().priority(8))
            );

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT Id, ScheduledJobId__c, IterationCount__c, EveryMinutes__c, Limit__c, Data__c
                FROM JobScheduler__c
                WHERE QueueName__c = :queueName AND Name = 'replace-me'
                LIMIT 1
            ];
            Assert.areEqual(firstScheduler.Id, updatedScheduler.Id);
            Assert.areEqual(secondScheduledJobId, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(2, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
            Assert.areEqual(10, QueuesSupport.toInteger(updatedScheduler.EveryMinutes__c, 0));
            Assert.areEqual(5, QueuesSupport.toInteger(updatedScheduler.Limit__c, 0));
            Assert.areEqual(firstScheduler.Id, secondJob.JobScheduler__c);
            Assert.areEqual(secondRunAt, secondJob.AvailableAt__c);
            Assert.areEqual(Queues.State.DELAYED.name(), secondJob.State__c);
            Assert.areEqual(8, QueuesSupport.toInteger(secondJob.Priority__c, 0));
            Assert.areEqual('scheduler:' + firstScheduler.Id + ':2', secondJob.ExternalId__c);
            Job__c canceledJob = [
                SELECT State__c, FailedReason__c, JobScheduler__c, LeaseToken__c, LeaseExpiresAt__c, AsyncApexJobId__c
                FROM Job__c
                WHERE Id = :firstJob.Id
            ];
            Assert.areEqual(Queues.State.CANCELED.name(), canceledJob.State__c);
            Assert.areEqual(CANCELED_BY_SCHEDULER_REPLACEMENT_REASON, canceledJob.FailedReason__c);
            Assert.areEqual(firstScheduler.Id, canceledJob.JobScheduler__c);
            Assert.areEqual(null, canceledJob.LeaseToken__c);
            Assert.areEqual(null, canceledJob.LeaseExpiresAt__c);
            Assert.areEqual(null, canceledJob.AsyncApexJobId__c);
            Assert.areEqual(1, [SELECT COUNT() FROM JobRun__c WHERE Job__c = :firstJob.Id]);
            Assert.areEqual(2, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :firstScheduler.Id]);
            Assert.areEqual(2, scheduleProvider.getCallCount('scheduleEvery'));
            Assert.areEqual(2, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('queryScheduledJobById'));
            Assert.areEqual(2, workerProvider.getCallCount('scheduleWorkerForJobs'));
            Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
            List<QueueEvent__e> canceledEvents = publishedEventsByType(eventProvider, 'CANCELED');
            Assert.areEqual(1, canceledEvents.size());
            Assert.areEqual(String.valueOf(firstJob.Id), canceledEvents[0].JobId__c);
        }
    }

    @IsTest
    static void should_continue_scheduler_iteration_when_upserting_after_completed_job() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-upsert-completed-' + System.now().getTime();
            Id firstScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 845);
            Id secondScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 846);
            Datetime firstRunAt = System.now().addMinutes(5);
            Datetime secondRunAt = System.now().addMinutes(10);
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns('scheduleEvery', new List<Object>{ firstScheduledJobId, secondScheduledJobId });
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobsByNamePrefix',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            firstScheduledJobId,
                            'WAITING',
                            '0 0 0 * * ?',
                            'Apex Queue Every Scheduler Test',
                            firstRunAt
                        )
                    }
                );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            Job__c firstJob = service.upsertJobScheduler(
                queueName,
                'completed-history',
                new Queues.RepeatOptions().every(5).startAt(firstRunAt).lim(5),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
                    .data(new Map<String, Object>{ 'version' => 'old' })
            );
            firstJob.State__c = Queues.State.COMPLETED.name();
            update firstJob;
            JobScheduler__c firstScheduler = queryOnlyScheduler(queueName, 'completed-history');

            // Act
            Job__c secondJob = service.upsertJobScheduler(
                queueName,
                'completed-history',
                new Queues.RepeatOptions().every(10).startAt(secondRunAt).lim(5),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
                    .data(new Map<String, Object>{ 'version' => 'new' })
            );

            // Assert
            JobScheduler__c updatedScheduler = queryOnlyScheduler(queueName, 'completed-history');
            Assert.areEqual(firstScheduler.Id, updatedScheduler.Id);
            Assert.areEqual(2, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
            Assert.areEqual('scheduler:' + firstScheduler.Id + ':1', firstJob.ExternalId__c);
            Assert.areEqual('scheduler:' + firstScheduler.Id + ':2', secondJob.ExternalId__c);
            Assert.areNotEqual(firstJob.Id, secondJob.Id);
            Assert.areEqual(
                Queues.State.COMPLETED.name(),
                [
                    SELECT State__c
                    FROM Job__c
                    WHERE Id = :firstJob.Id
                ]
                .State__c
            );
            Assert.areEqual(2, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :firstScheduler.Id]);
            Assert.areEqual(2, scheduleProvider.getCallCount('scheduleEvery'));
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(2, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_reject_upsert_when_scheduler_limit_is_already_exhausted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-upsert-limit-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 847);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'limit-exhausted');
            scheduler.ScheduledJobId__c = scheduledJobId;
            scheduler.IterationCount__c = 5;
            scheduler.Limit__c = 5;
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>());
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                (QueuesWorkerService) Test.createStub(
                    QueuesWorkerService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                )
            );
            String failureMessage;

            // Act
            try {
                service.upsertJobScheduler(
                    queueName,
                    'limit-exhausted',
                    new Queues.RepeatOptions().every(5).startAt(System.now().addMinutes(5)).lim(5),
                    new Queues.JobTemplate(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                    )
                );
                Assert.fail('Expected exhausted scheduler limit to reject upsert.');
            } catch (Queues.ConfigurationException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            JobScheduler__c updatedScheduler = queryOnlyScheduler(queueName, 'limit-exhausted');
            Assert.areEqual(
                'Queue "' + queueName + '" job scheduler "limit-exhausted" does not have a future run.',
                failureMessage
            );
            Assert.areEqual(scheduledJobId, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(5, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
            Assert.areEqual(0, scheduleProvider.getCallCount('scheduleEvery'));
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('queryScheduledJobsByNamePrefix'));
        }
    }

    @IsTest
    static void should_upsert_pattern_scheduler_from_cron_next_fire_time() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-pattern-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 802);
            Datetime firstRunAt = System.now().addDays(1);
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('schedulePattern', scheduledJobId);
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobById',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            scheduledJobId,
                            'WAITING',
                            '0 0 0 * * ?',
                            'Apex Queue Pattern Scheduler Test',
                            firstRunAt
                        )
                    }
                );
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                (QueuesWorkerService) Test.createStub(
                    QueuesWorkerService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                )
            );

            // Act
            Job__c job = service.upsertJobScheduler(
                queueName,
                'invoice-cron',
                new Queues.RepeatOptions().pattern('0 0 0 * * ?'),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
            );

            // Assert
            JobScheduler__c scheduler = queryOnlyScheduler(queueName, 'invoice-cron');
            Assert.areNotEqual(null, job.Id);
            Assert.areEqual(scheduler.Id, job.JobScheduler__c);
            Assert.areEqual(firstRunAt, job.AvailableAt__c);
            Assert.areEqual('0 0 0 * * ?', scheduler.Pattern__c);
            Assert.areEqual(scheduledJobId, scheduler.ScheduledJobId__c);
            Assert.areEqual(1, scheduleProvider.getCallCount('schedulePattern'));
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('queryScheduledJobById'));
        }
    }

    @IsTest
    static void should_upsert_queueable_scheduler_without_worker_wake() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-queueable-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 850);
            Datetime firstRunAt = System.now().addMinutes(5);
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', scheduledJobId);
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider queueableProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                newDefinitionService(queueName, true),
                new QueuesErrorService(),
                new QueuesJobSchedulerRepository(),
                new QueuesJobService(),
                newProcessorService(queueName),
                (QueuesQueueableDispatchService) Test.createStub(
                    QueuesQueueableDispatchService.class,
                    queueableProvider
                ),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            Job__c job = service.upsertJobScheduler(
                queueName,
                'queueable-granular',
                new Queues.RepeatOptions().every(5).startAt(firstRunAt),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS),
                Queues.ExecutionMode.QUEUEABLE_SERIAL
            );

            // Assert
            JobScheduler__c scheduler = queryOnlyScheduler(queueName, 'queueable-granular');
            Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), scheduler.ExecutionMode__c);
            Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), job.ExecutionMode__c);
            Assert.areEqual(null, job.QueueableDispatch__c);
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
            Assert.areEqual(0, queueableProvider.getCallCount('createWaitingDispatch'));
        }
    }

    @IsTest
    static void should_dispatch_due_queueable_scheduler_occurrence_when_materializing_next() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-queueable-mat-' + System.now().getTime();
            Id currentScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 851);
            Id nextScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 852);
            Datetime dueRunAt = System.now().addMinutes(-1);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'queueable-materializer');
            scheduler.ScheduledJobId__c = currentScheduledJobId;
            scheduler.NextRunAt__c = dueRunAt;
            scheduler.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            insert scheduler;

            Job__c dueJob = QueuesTestSupport.job(
                queueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.DELAYED.name(),
                'scheduler:' + scheduler.Id + ':1'
            );
            dueJob.JobScheduler__c = scheduler.Id;
            dueJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            dueJob.AvailableAt__c = dueRunAt;
            insert dueJob;

            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = queueName,
                Mode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(),
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            insert dispatch;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', nextScheduledJobId);
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider queueableProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('createWaitingDispatch', dispatch)
                .withReturn('enqueueDispatcher', QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 853));
            QueuesJobSchedulerService service = newSchedulerService(
                newDefinitionService(queueName, true),
                new QueuesErrorService(),
                new QueuesJobSchedulerRepository(),
                new QueuesJobService(),
                newProcessorService(queueName),
                (QueuesQueueableDispatchService) Test.createStub(
                    QueuesQueueableDispatchService.class,
                    queueableProvider
                ),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            service.materializeJobSchedulerAndRecordError(scheduler.Id, currentScheduledJobId);

            // Assert
            Job__c dispatchedJob = [
                SELECT QueueableDispatch__c
                FROM Job__c
                WHERE Id = :dueJob.Id
            ];
            Job__c nextJob = queryOnlySchedulerJob(scheduler.Id, 2);
            List<Object> dispatchArgs = queueableProvider.getLastArgs('createWaitingDispatch');
            Assert.areEqual(dispatch.Id, dispatchedJob.QueueableDispatch__c);
            Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), nextJob.ExecutionMode__c);
            Assert.areEqual(null, nextJob.QueueableDispatch__c);
            Assert.areEqual(queueName, dispatchArgs[0]);
            Assert.areEqual(null, dispatchArgs[1]);
            Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT, dispatchArgs[2]);
            Assert.areEqual(1, queueableProvider.getCallCount('createWaitingDispatch'));
            Assert.areEqual(1, queueableProvider.getCallCount('enqueueDispatcher'));
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_skip_queueable_scheduler_dispatch_when_occurrence_is_not_due() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-queueable-not-due-' + System.now().getTime();
            Id currentScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 855);
            Id nextScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 856);
            Datetime futureRunAt = System.now().addMinutes(5);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'queueable-not-due');
            scheduler.ScheduledJobId__c = currentScheduledJobId;
            scheduler.NextRunAt__c = futureRunAt;
            scheduler.EveryMinutes__c = 5;
            scheduler.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            insert scheduler;

            Job__c futureJob = QueuesTestSupport.job(
                queueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.DELAYED.name(),
                'scheduler:' + scheduler.Id + ':1'
            );
            futureJob.JobScheduler__c = scheduler.Id;
            futureJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            futureJob.AvailableAt__c = futureRunAt;
            insert futureJob;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', nextScheduledJobId);
            QueuesTestSupport.RecordingStubProvider queueableProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                newDefinitionService(queueName, true),
                new QueuesErrorService(),
                new QueuesJobSchedulerRepository(),
                new QueuesJobService(),
                newProcessorService(queueName),
                (QueuesQueueableDispatchService) Test.createStub(
                    QueuesQueueableDispatchService.class,
                    queueableProvider
                ),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesWorkerService()
            );

            // Act
            service.materializeJobSchedulerAndRecordError(scheduler.Id, currentScheduledJobId);

            // Assert
            Assert.areEqual(0, queueableProvider.getCallCount('createWaitingDispatch'));
            Assert.areEqual(0, queueableProvider.getCallCount('enqueueDispatcher'));
        }
    }

    @IsTest
    static void should_upsert_invocable_scheduler_without_worker_or_queueable_dispatch() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-invocable-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 854);
            Datetime firstRunAt = System.now().addMinutes(5);
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', scheduledJobId);
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider queueableProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                newDefinitionService(queueName, true),
                new QueuesErrorService(),
                new QueuesJobSchedulerRepository(),
                new QueuesJobService(),
                newProcessorService(queueName),
                (QueuesQueueableDispatchService) Test.createStub(
                    QueuesQueueableDispatchService.class,
                    queueableProvider
                ),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            Job__c job = service.upsertJobScheduler(
                queueName,
                'invocable-granular',
                new Queues.RepeatOptions().every(5).startAt(firstRunAt),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS),
                Queues.ExecutionMode.INVOCABLE
            );

            // Assert
            JobScheduler__c scheduler = queryOnlyScheduler(queueName, 'invocable-granular');
            Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), scheduler.ExecutionMode__c);
            Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), job.ExecutionMode__c);
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
            Assert.areEqual(0, queueableProvider.getCallCount('createWaitingDispatch'));
        }
    }

    @IsTest
    static void should_materialize_every_scheduler_and_self_reschedule_from_persisted_state() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-every-mat-' + System.now().getTime();
            Id currentScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 803);
            Id nextScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 804);
            Datetime previousRunAt = System.now().addMinutes(5);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'every-materializer');
            scheduler.ScheduledJobId__c = currentScheduledJobId;
            scheduler.NextRunAt__c = previousRunAt;
            scheduler.EveryMinutes__c = 5;
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', nextScheduledJobId);
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesWorkerService) Test.createStub(
                    QueuesWorkerService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                )
            );

            // Act
            service.materializeJobSchedulerAndRecordError(scheduler.Id, currentScheduledJobId);

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT ScheduledJobId__c, IterationCount__c, NextRunAt__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Job__c materializedJob = queryOnlySchedulerJob(scheduler.Id, 2);
            Assert.areEqual(nextScheduledJobId, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(2, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
            Assert.areEqual(previousRunAt.addMinutes(5), updatedScheduler.NextRunAt__c);
            Assert.areEqual(previousRunAt.addMinutes(5), materializedJob.AvailableAt__c);
            Assert.areEqual(1, scheduleProvider.getCallCount('scheduleEvery'));
        }
    }

    @IsTest
    static void should_skip_materialization_when_cancel_wins_scheduler_lock() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-cancel-wins-' + System.now().getTime();
            Id schedulerId = QueuesTestSupport.fakeId(JobScheduler__c.SObjectType, 1);
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 2);
            JobScheduler__c canceledScheduler = QueuesTestSupport.jobScheduler(queueName, 'cancel-wins');
            canceledScheduler.Id = schedulerId;
            canceledScheduler.IsActive__c = false;
            canceledScheduler.ScheduledJobId__c = null;
            canceledScheduler.NextRunAt__c = null;
            QueuesTestSupport.RecordingStubProvider schedulerRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryJobSchedulersByIdForUpdate', new List<JobScheduler__c>{ canceledScheduler });
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                newDefinitionService(queueName, true),
                new QueuesErrorService(),
                (QueuesJobSchedulerRepository) Test.createStub(
                    QueuesJobSchedulerRepository.class,
                    schedulerRepositoryProvider
                ),
                new QueuesJobService(),
                newProcessorService(queueName),
                new QueuesQueueableDispatchService(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesWorkerService()
            );

            // Act
            service.materializeJobSchedulerAndRecordError(schedulerId, scheduledJobId);

            // Assert
            Assert.areEqual(1, schedulerRepositoryProvider.getCallCount('queryJobSchedulersByIdForUpdate'));
            Assert.areEqual(0, scheduleProvider.getCallCount('scheduleEvery'));
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :schedulerId]);
        }
    }

    @IsTest
    static void should_materialize_overdue_every_scheduler_at_next_future_interval() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-every-overdue-mat-' + System.now().getTime();
            Id currentScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 835);
            Id nextScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 836);
            Datetime staleRunAt = System.now().addMinutes(-11);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'every-overdue-materializer');
            scheduler.ScheduledJobId__c = currentScheduledJobId;
            scheduler.NextRunAt__c = staleRunAt;
            scheduler.EveryMinutes__c = 5;
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', nextScheduledJobId);
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesWorkerService) Test.createStub(
                    QueuesWorkerService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                )
            );

            // Act
            service.materializeJobSchedulerAndRecordError(scheduler.Id, currentScheduledJobId);

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT ScheduledJobId__c, IterationCount__c, NextRunAt__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Job__c materializedJob = queryOnlySchedulerJob(scheduler.Id, 2);
            Assert.areEqual(nextScheduledJobId, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(2, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
            Assert.isTrue(updatedScheduler.NextRunAt__c > System.now());
            Assert.areEqual(updatedScheduler.NextRunAt__c, materializedJob.AvailableAt__c);
            Assert.areEqual(1, scheduleProvider.getCallCount('scheduleEvery'));
        }
    }

    @IsTest
    static void should_preserve_every_cadence_when_subsequent_run_is_due_now() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            Datetime previousRunAt = Datetime.newInstanceGmt(2026, 1, 1, 12, 0, 0);
            Datetime currentNow = previousRunAt.addMinutes(5);
            JobScheduler__c scheduler = new JobScheduler__c(NextRunAt__c = previousRunAt, EveryMinutes__c = 5);

            // Act
            Datetime nextRunAt = QueuesJobSchedulerService.resolveSubsequentEveryRunAt(scheduler, 2, currentNow);

            // Assert
            Assert.areEqual(previousRunAt.addMinutes(10), nextRunAt);
        }
    }

    @IsTest
    static void should_materialize_pattern_scheduler_without_self_rescheduling() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-pattern-mat-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 805);
            Datetime nextRunAt = System.now().addDays(2);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'pattern-materializer');
            scheduler.ScheduledJobId__c = scheduledJobId;
            scheduler.EveryMinutes__c = null;
            scheduler.Pattern__c = '0 0 0 * * ?';
            scheduler.NextRunAt__c = System.now().addDays(1);
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobById',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            scheduledJobId,
                            'WAITING',
                            scheduler.Pattern__c,
                            'Apex Queue Pattern Scheduler Test',
                            nextRunAt
                        )
                    }
                );
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                (QueuesWorkerService) Test.createStub(
                    QueuesWorkerService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                )
            );

            // Act
            service.materializeJobSchedulerAndRecordError(scheduler.Id, scheduledJobId);

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT ScheduledJobId__c, IterationCount__c, NextRunAt__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Job__c materializedJob = queryOnlySchedulerJob(scheduler.Id, 2);
            Assert.areEqual(scheduledJobId, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(2, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
            Assert.areEqual(nextRunAt, updatedScheduler.NextRunAt__c);
            Assert.areEqual(nextRunAt, materializedJob.AvailableAt__c);
            Assert.areEqual(0, scheduleProvider.getCallCount('scheduleEvery'));
        }
    }

    @IsTest
    static void should_reject_repeat_options_that_define_both_variants() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-invalid-' + System.now().getTime();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesWorkerService()
            );
            String failureMessage;

            // Act
            try {
                service.upsertJobScheduler(
                    queueName,
                    'invalid',
                    new Queues.RepeatOptions().every(5).pattern('0 0 0 * * ?'),
                    new Queues.JobTemplate(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                    )
                );
                Assert.fail('Expected invalid scheduler repeat options to fail.');
            } catch (Queues.ConfigurationException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            Assert.areEqual(
                'Queue "' +
                    queueName +
                    '" job scheduler "invalid" cannot define both repeatOpts.every(...) and repeatOpts.pattern(...).',
                failureMessage
            );
        }
    }

    @IsTest
    static void should_reject_every_scheduler_when_window_is_invalid() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-invalid-window-' + System.now().getTime();
            Datetime currentNow = System.now();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesWorkerService()
            );
            String failureMessage;

            // Act
            try {
                service.upsertJobScheduler(
                    queueName,
                    'invalid-window',
                    new Queues.RepeatOptions().every(5).startAt(currentNow.addMinutes(10)).endAt(currentNow),
                    new Queues.JobTemplate(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                    )
                );
                Assert.fail('Expected invalid scheduler window to fail.');
            } catch (Queues.ConfigurationException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            Assert.areEqual(
                'Queue "' + queueName + '" job scheduler "invalid-window" has startAt after endAt.',
                failureMessage
            );
        }
    }

    @IsTest
    static void should_resolve_lifecycle_headroom_and_batch_sizes() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Assert
            Assert.areEqual(false, QueuesJobSchedulerService.hasHeadroomForLifecycle(501, 10, 5, 2000));
            Assert.areEqual(false, QueuesJobSchedulerService.hasHeadroomForLifecycle(502, 9, 5, 2000));
            Assert.areEqual(true, QueuesJobSchedulerService.hasHeadroomForLifecycle(502, 10, 5, 2000));
            Assert.areEqual(0, QueuesJobSchedulerService.resolveLifecycleJobBatchSize(501));
            Assert.areEqual(1, QueuesJobSchedulerService.resolveLifecycleJobBatchSize(502));
        }
    }

    @IsTest
    static void should_construct_with_default_collaborators() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Assert
            Assert.areNotEqual(null, new QueuesJobSchedulerService());
        }
    }

    @IsTest
    static void should_cancel_scheduler_and_pending_jobs() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-cancel-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 806);
            Id staleScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 845);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'cancel-me');
            scheduler.ScheduledJobId__c = scheduledJobId;
            insert scheduler;

            Job__c pendingJob = QueuesTestSupport.job(
                queueName,
                'pending-job',
                Queues.State.WAITING.name(),
                'pending-job'
            );
            pendingJob.JobScheduler__c = scheduler.Id;
            pendingJob.LeaseToken__c = 'lease-token';
            pendingJob.LeaseExpiresAt__c = System.now().addMinutes(5);
            pendingJob.AsyncApexJobId__c = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 806);
            insert pendingJob;
            insert QueuesTestSupport.jobRun(pendingJob.Id, 1, Queues.RunStatus.FAILED.name());

            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobsByNamePrefix',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            scheduledJobId,
                            'WAITING',
                            '0 0 0 * * ?',
                            'Apex Queue Every Scheduler Test'
                        ),
                        QueuesTestSupport.cronTrigger(
                            staleScheduledJobId,
                            'WAITING',
                            '0 0 0 * * ?',
                            'Apex Queue Every Scheduler Test'
                        )
                    }
                );
            QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService(),
                QueuesTestSupport.newRecordingQueueEventService(queueName, 'CANCELED', eventProvider)
            );

            // Act
            Boolean missingCanceled = service.cancelJobScheduler(queueName, 'missing');
            Boolean canceled = service.cancelJobScheduler(queueName, 'cancel-me');
            Boolean alreadyCanceled = service.cancelJobScheduler(queueName, 'cancel-me');

            // Assert
            Assert.areEqual(false, missingCanceled);
            Assert.areEqual(true, canceled);
            Assert.areEqual(false, alreadyCanceled);
            JobScheduler__c canceledScheduler = [
                SELECT IsActive__c, ScheduledJobId__c, NextRunAt__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Job__c canceledJob = [
                SELECT State__c, FailedReason__c, JobScheduler__c, LeaseToken__c, LeaseExpiresAt__c, AsyncApexJobId__c
                FROM Job__c
                WHERE Id = :pendingJob.Id
            ];
            Assert.areEqual(false, canceledScheduler.IsActive__c);
            Assert.areEqual(null, canceledScheduler.ScheduledJobId__c);
            Assert.areEqual(null, canceledScheduler.NextRunAt__c);
            Assert.areEqual(Queues.State.CANCELED.name(), canceledJob.State__c);
            Assert.areEqual(CANCELED_BY_SCHEDULER_CANCEL_REASON, canceledJob.FailedReason__c);
            Assert.areEqual(scheduler.Id, canceledJob.JobScheduler__c);
            Assert.areEqual(null, canceledJob.LeaseToken__c);
            Assert.areEqual(null, canceledJob.LeaseExpiresAt__c);
            Assert.areEqual(null, canceledJob.AsyncApexJobId__c);
            Assert.areEqual(1, [SELECT COUNT() FROM JobRun__c WHERE Job__c = :pendingJob.Id]);
            Assert.areEqual(2, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('queryScheduledJobById'));
            Assert.areEqual(1, eventProvider.getCallCount('publishQueueEvents'));
            List<QueueEvent__e> canceledEvents = publishedEventsByType(eventProvider, 'CANCELED');
            Assert.areEqual(1, canceledEvents.size());
            Assert.areEqual(String.valueOf(pendingJob.Id), canceledEvents[0].JobId__c);
        }
    }

    @IsTest
    static void should_reactivate_canceled_scheduler_when_upserting_same_identity() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-cancel-upsert-' + System.now().getTime();
            String jobSchedulerId = 'cancel-upsert';
            Id firstScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 851);
            Id secondScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 852);
            Datetime firstRunAt = System.now().addMinutes(5);
            Datetime secondRunAt = System.now().addMinutes(10);
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns('scheduleEvery', new List<Object>{ firstScheduledJobId, secondScheduledJobId });
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>())
                .withReturn(
                    'queryScheduledJobById',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            firstScheduledJobId,
                            'WAITING',
                            '0 0 0 1 1 ? 2099',
                            'Apex Queue Every Scheduler Test'
                        )
                    }
                );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );
            Job__c firstJob = service.upsertJobScheduler(
                queueName,
                jobSchedulerId,
                new Queues.RepeatOptions().every(5).startAt(firstRunAt),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
            );
            JobScheduler__c originalScheduler = queryOnlyScheduler(queueName, jobSchedulerId);

            // Act
            Boolean canceled = service.cancelJobScheduler(queueName, jobSchedulerId);
            Job__c secondJob = service.upsertJobScheduler(
                queueName,
                jobSchedulerId,
                new Queues.RepeatOptions().every(10).startAt(secondRunAt).lim(5),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
            );

            // Assert
            JobScheduler__c reactivatedScheduler = [
                SELECT Id, IsActive__c, ScheduledJobId__c, IterationCount__c, NextRunAt__c
                FROM JobScheduler__c
                WHERE Id = :originalScheduler.Id
            ];
            Job__c canceledJob = [SELECT State__c, FailedReason__c FROM Job__c WHERE Id = :firstJob.Id];
            Assert.isTrue(canceled);
            Assert.areEqual(originalScheduler.Id, reactivatedScheduler.Id);
            Assert.isTrue(reactivatedScheduler.IsActive__c);
            Assert.areEqual(secondScheduledJobId, reactivatedScheduler.ScheduledJobId__c);
            Assert.areEqual(2, QueuesSupport.toInteger(reactivatedScheduler.IterationCount__c, 0));
            Assert.areEqual(secondRunAt, reactivatedScheduler.NextRunAt__c);
            Assert.areEqual(Queues.State.CANCELED.name(), canceledJob.State__c);
            Assert.areEqual(CANCELED_BY_SCHEDULER_CANCEL_REASON, canceledJob.FailedReason__c);
            Assert.areEqual(originalScheduler.Id, secondJob.JobScheduler__c);
            Assert.areEqual('scheduler:' + originalScheduler.Id + ':2', secondJob.ExternalId__c);
            Assert.areEqual(2, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :originalScheduler.Id]);
            Assert.areEqual(2, scheduleProvider.getCallCount('scheduleEvery'));
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(2, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_freeze_scheduler_when_lifecycle_has_no_headroom() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-lifecycle-no-headroom-' + System.now().getTime();
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'lifecycle-no-headroom');
            insert scheduler;
            consumeDmlRowsUntilLifecycleHeadroomIsExhausted(queueName);

            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>());
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );

            // Act
            Boolean canceled = service.cancelJobScheduler(queueName, 'lifecycle-no-headroom');
            Boolean removed = service.removeJobScheduler(queueName, 'lifecycle-no-headroom');

            // Assert
            JobScheduler__c frozenScheduler = [
                SELECT IsActive__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Assert.areEqual(false, canceled);
            Assert.areEqual(false, removed);
            Assert.areEqual(false, frozenScheduler.IsActive__c);
        }
    }

    @IsTest
    static void should_not_abort_scheduler_schedule_when_cancel_jobs_fail() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-cancel-fail-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 849);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'cancel-fail');
            scheduler.ScheduledJobId__c = scheduledJobId;
            insert scheduler;

            Job__c pendingJob = QueuesTestSupport.job(
                queueName,
                'pending-job',
                Queues.State.WAITING.name(),
                'pending-job'
            );
            pendingJob.JobScheduler__c = scheduler.Id;
            insert pendingJob;

            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobsByNamePrefix',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            scheduledJobId,
                            'WAITING',
                            '0 0 0 * * ?',
                            'Apex Queue Every Scheduler Test'
                        )
                    }
                );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryJobsBySchedulerIdForUpdate', new List<Job__c>{ pendingJob })
                .withException('updateJobs', new Queues.QueueException('update exploded'));
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService(),
                new QueuesJobSchedulerRepository(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesErrorService(),
                true
            );
            String failureMessage;

            // Act
            try {
                service.cancelJobScheduler(queueName, 'cancel-fail');
                Assert.fail('Expected scheduler cancel to fail before aborting native schedules.');
            } catch (Queues.QueueException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            JobScheduler__c activeScheduler = [
                SELECT IsActive__c, ScheduledJobId__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Job__c uncanceledJob = [
                SELECT State__c
                FROM Job__c
                WHERE Id = :pendingJob.Id
            ];
            Assert.areEqual('update exploded', failureMessage);
            Assert.areEqual(true, activeScheduler.IsActive__c);
            Assert.areEqual(scheduledJobId, activeScheduler.ScheduledJobId__c);
            Assert.areEqual(Queues.State.WAITING.name(), uncanceledJob.State__c);
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_remove_scheduler_and_linked_jobs() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-remove-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 846);
            Id staleScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 847);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'remove-me');
            scheduler.ScheduledJobId__c = scheduledJobId;
            insert scheduler;

            Job__c waitingJob = QueuesTestSupport.job(queueName, 'waiting-job', Queues.State.WAITING.name(), 'waiting');
            waitingJob.JobScheduler__c = scheduler.Id;
            Job__c completedJob = QueuesTestSupport.job(
                queueName,
                'completed-job',
                Queues.State.COMPLETED.name(),
                'completed'
            );
            completedJob.JobScheduler__c = scheduler.Id;
            insert new List<Job__c>{ waitingJob, completedJob };
            insert QueuesTestSupport.jobRun(waitingJob.Id, 1, Queues.RunStatus.FAILED.name());

            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobsByNamePrefix',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            scheduledJobId,
                            'WAITING',
                            '0 0 0 * * ?',
                            'Apex Queue Every Scheduler Test'
                        ),
                        QueuesTestSupport.cronTrigger(
                            staleScheduledJobId,
                            'WAITING',
                            '0 0 0 * * ?',
                            'Apex Queue Every Scheduler Test'
                        )
                    }
                );
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );

            // Act
            Boolean missingRemoved = service.removeJobScheduler(queueName, 'missing');
            Boolean removed = service.removeJobScheduler(queueName, 'remove-me');
            Boolean alreadyRemoved = service.removeJobScheduler(queueName, 'remove-me');

            // Assert
            Assert.areEqual(false, missingRemoved);
            Assert.areEqual(true, removed);
            Assert.areEqual(false, alreadyRemoved);
            Assert.areEqual(0, [SELECT COUNT() FROM JobScheduler__c WHERE Id = :scheduler.Id]);
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id]);
            Assert.areEqual(0, [SELECT COUNT() FROM JobRun__c WHERE Job__c = :waitingJob.Id]);
            Assert.areEqual(2, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('queryScheduledJobById'));
        }
    }

    @IsTest
    static void should_not_abort_scheduler_schedule_when_remove_jobs_fail() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-remove-fail-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 850);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'remove-fail');
            scheduler.ScheduledJobId__c = scheduledJobId;
            insert scheduler;

            Job__c waitingJob = QueuesTestSupport.job(queueName, 'waiting-job', Queues.State.WAITING.name(), 'waiting');
            waitingJob.JobScheduler__c = scheduler.Id;
            insert waitingJob;

            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobsByNamePrefix',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            scheduledJobId,
                            'WAITING',
                            '0 0 0 * * ?',
                            'Apex Queue Every Scheduler Test'
                        )
                    }
                );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('hasJobsBySchedulerId', false)
                .withReturn('queryJobsBySchedulerIdForUpdate', new List<Job__c>{ waitingJob })
                .withException('deleteJobs', new Queues.QueueException('delete exploded'));
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService(),
                new QueuesJobSchedulerRepository(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesErrorService(),
                true
            );
            String failureMessage;

            // Act
            try {
                service.removeJobScheduler(queueName, 'remove-fail');
                Assert.fail('Expected scheduler removal to fail before aborting native schedules.');
            } catch (Queues.QueueException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            Assert.areEqual('delete exploded', failureMessage);
            Assert.areEqual(1, [SELECT COUNT() FROM JobScheduler__c WHERE Id = :scheduler.Id]);
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE Id = :waitingJob.Id]);
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_remove_scheduler_without_linked_jobs() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-remove-empty-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 852);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'remove-empty');
            scheduler.ScheduledJobId__c = scheduledJobId;
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobsByNamePrefix',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            scheduledJobId,
                            'WAITING',
                            '0 0 0 * * ?',
                            'Apex Queue Every Scheduler Test'
                        )
                    }
                );
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );

            // Act
            Boolean removed = service.removeJobScheduler(queueName, 'remove-empty');

            // Assert
            Assert.areEqual(true, removed);
            Assert.areEqual(0, [SELECT COUNT() FROM JobScheduler__c WHERE Id = :scheduler.Id]);
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_reject_removing_scheduler_with_active_jobs() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-remove-active-' + System.now().getTime();
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'remove-active');
            insert scheduler;

            Job__c activeJob = QueuesTestSupport.job(queueName, 'active-job', Queues.State.ACTIVE.name(), 'active');
            activeJob.JobScheduler__c = scheduler.Id;
            insert activeJob;

            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );
            String failureMessage;

            // Act
            try {
                service.removeJobScheduler(queueName, 'remove-active');
                Assert.fail('Expected active scheduler job removal to be rejected.');
            } catch (Queues.InvalidOperationException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            Assert.areEqual('Active scheduler jobs cannot be removed.', failureMessage);
            Assert.areEqual(1, [SELECT COUNT() FROM JobScheduler__c WHERE Id = :scheduler.Id]);
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE Id = :activeJob.Id]);
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_read_scheduler_values_and_counts() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-read-' + System.now().getTime();
            Id parentJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 807);
            Datetime startAt = System.now().addMinutes(3);
            Datetime endAt = startAt.addHours(1);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'read-me');
            scheduler.Limit__c = 4;
            scheduler.StartAt__c = startAt;
            scheduler.EndAt__c = endAt;
            scheduler.Options__c = QueuesSupport.serialize(
                new Map<String, Object>{
                    'attempts' => 5,
                    'priority' => 9,
                    'category' => 'billing',
                    'groupKey' => 'ORDER-READ',
                    'correlationId' => 'scheduler-read-correlation',
                    'backoffType' => Queues.Backoff.EXPONENTIAL.name(),
                    'backoffValue' => 2,
                    'backoffJitter' => 0.25,
                    'parentJobId' => parentJobId
                }
            );
            insert scheduler;

            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesWorkerService()
            );

            // Act
            Queues.JobScheduler fetched = service.getJobScheduler(queueName, 'read-me');
            Queues.JobScheduler missing = service.getJobScheduler(queueName, 'missing');
            List<Queues.JobScheduler> fetchedSchedulers = service.getJobSchedulers(queueName, 0, 10, false);
            List<Queues.JobScheduler> emptyRange = service.getJobSchedulers(queueName, 2, 1, true);
            Integer schedulerCount = service.getJobSchedulersCount(queueName);

            // Assert
            Assert.areEqual(null, missing);
            Assert.areEqual(1, fetchedSchedulers.size());
            Assert.areEqual(0, emptyRange.size());
            Assert.areEqual(1, schedulerCount);
            Assert.areEqual('read-me', fetched.getJobSchedulerId());
            Assert.areEqual(QueuesTestSupport.DEFAULT_JOB_NAME, fetched.getJobName());
            Assert.areEqual(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, fetched.getProcessor());
            Assert.areEqual(4, fetched.getLimit());
            Assert.areEqual(startAt, fetched.getStartAt());
            Assert.areEqual(endAt, fetched.getEndAt());
            Assert.areEqual(5, fetched.getEveryMinutes());
            Assert.areEqual(scheduler.NextRunAt__c, fetched.getNextRunAt());
            Assert.areEqual(5, fetched.getTemplate().getOpts().getAttempts());
            Assert.areEqual(9, fetched.getTemplate().getOpts().getPriority());
            Assert.areEqual('billing', fetched.getTemplate().getOpts().getCategory());
            Assert.areEqual('ORDER-READ', fetched.getTemplate().getOpts().getGroupKey());
            Assert.areEqual('scheduler-read-correlation', fetched.getTemplate().getOpts().getCorrelationId());
            Assert.areEqual(Queues.Backoff.EXPONENTIAL, fetched.getTemplate().getOpts().getBackoffType());
            Assert.areEqual(2, fetched.getTemplate().getOpts().getBackoffValue());
            Assert.areEqual(0.25, fetched.getTemplate().getOpts().getBackoffJitter());
            Assert.areEqual(parentJobId, fetched.getTemplate().getOpts().getParentJobId());
        }
    }

    @IsTest
    static void should_skip_materialization_when_scheduler_is_not_runnable() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-skip-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 808);
            JobScheduler__c inactiveScheduler = QueuesTestSupport.jobScheduler(queueName, 'inactive');
            inactiveScheduler.IsActive__c = false;
            inactiveScheduler.ScheduledJobId__c = scheduledJobId;
            insert inactiveScheduler;

            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesWorkerService()
            );

            // Act
            service.materializeJobSchedulerAndRecordError(null, null);
            service.materializeJobSchedulerAndRecordError(
                QueuesTestSupport.fakeId(JobScheduler__c.SObjectType, 809),
                null
            );
            service.materializeJobSchedulerAndRecordError(inactiveScheduler.Id, scheduledJobId);

            // Assert
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :inactiveScheduler.Id]);
        }
    }

    @IsTest
    static void should_deactivate_pattern_scheduler_when_limit_is_reached() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-pattern-limit-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 810);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'pattern-limit');
            scheduler.ScheduledJobId__c = scheduledJobId;
            scheduler.Pattern__c = '0 0 0 * * ?';
            scheduler.EveryMinutes__c = null;
            scheduler.Limit__c = 1;
            scheduler.IterationCount__c = 1;
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobById',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            scheduledJobId,
                            'WAITING',
                            scheduler.Pattern__c,
                            'Apex Queue Pattern Scheduler Test',
                            System.now().addDays(1)
                        )
                    }
                );
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );

            // Act
            service.materializeJobSchedulerAndRecordError(scheduler.Id, scheduledJobId);

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT IsActive__c, ScheduledJobId__c, NextRunAt__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Assert.areEqual(false, updatedScheduler.IsActive__c);
            Assert.areEqual(null, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(null, updatedScheduler.NextRunAt__c);
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_deactivate_every_scheduler_when_limit_is_reached() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-every-limit-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 811);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'every-limit');
            scheduler.ScheduledJobId__c = scheduledJobId;
            scheduler.Limit__c = 1;
            scheduler.IterationCount__c = 1;
            insert scheduler;

            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesWorkerService()
            );

            // Act
            service.materializeJobSchedulerAndRecordError(scheduler.Id, scheduledJobId);

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT IsActive__c, ScheduledJobId__c, NextRunAt__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Assert.areEqual(false, updatedScheduler.IsActive__c);
            Assert.areEqual(null, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(null, updatedScheduler.NextRunAt__c);
        }
    }

    @IsTest
    static void should_skip_pattern_materialization_when_scheduled_trigger_is_missing() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-pattern-missing-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 812);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'pattern-missing');
            scheduler.ScheduledJobId__c = scheduledJobId;
            scheduler.EveryMinutes__c = null;
            scheduler.Pattern__c = '0 0 0 * * ?';
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobById', new List<CronTrigger>());
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );

            // Act
            service.materializeJobSchedulerAndRecordError(scheduler.Id, scheduledJobId);

            // Assert
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id]);
        }
    }

    @IsTest
    static void should_deactivate_every_scheduler_when_next_run_is_missing() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-every-missing-next-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 837);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'every-missing-next');
            scheduler.ScheduledJobId__c = scheduledJobId;
            scheduler.NextRunAt__c = null;
            scheduler.EveryMinutes__c = 5;
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesWorkerService()
            );

            // Act
            service.materializeJobSchedulerAndRecordError(scheduler.Id, scheduledJobId);

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT IsActive__c, ScheduledJobId__c, NextRunAt__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Assert.areEqual(false, updatedScheduler.IsActive__c);
            Assert.areEqual(null, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(null, updatedScheduler.NextRunAt__c);
            Assert.areEqual(0, scheduleProvider.getCallCount('scheduleEvery'));
        }
    }

    @IsTest
    static void should_return_existing_scheduler_job_when_occurrence_was_already_materialized() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-existing-job-' + System.now().getTime();
            Id currentScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 813);
            Id nextScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 814);
            Datetime previousRunAt = System.now().addMinutes(5);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'existing-job');
            scheduler.ScheduledJobId__c = currentScheduledJobId;
            scheduler.NextRunAt__c = previousRunAt;
            scheduler.EveryMinutes__c = 5;
            insert scheduler;

            Job__c existingJob = QueuesTestSupport.job(
                queueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.DELAYED.name(),
                'scheduler:' + scheduler.Id + ':2'
            );
            existingJob.JobScheduler__c = scheduler.Id;
            existingJob.AvailableAt__c = previousRunAt.addMinutes(5);
            insert existingJob;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', nextScheduledJobId);
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesWorkerService) Test.createStub(
                    QueuesWorkerService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                )
            );

            // Act
            service.materializeJobSchedulerAndRecordError(scheduler.Id, currentScheduledJobId);

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT ScheduledJobId__c, IterationCount__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Assert.areEqual(nextScheduledJobId, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(2, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE ExternalId__c = :existingJob.ExternalId__c]);
        }
    }

    @IsTest
    static void should_rollback_upsert_and_abort_schedule_when_job_insert_fails() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-upsert-fail-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 815);
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', scheduledJobId);
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobById',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            scheduledJobId,
                            'WAITING',
                            '0 0 0 1 1 ? 2099',
                            'Apex Queue Every Scheduler Test',
                            System.now().addMinutes(5)
                        )
                    }
                );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryJobsBySchedulerIdForUpdate', new List<Job__c>())
                .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                .withReturn('mapToJob', new Job__c())
                .withReturn('insertJobsAllowingPartialSuccess', buildFailedInsertSaveResults());
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService(),
                new QueuesJobSchedulerRepository(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesErrorService(),
                true
            );
            String failureMessage;

            // Act
            try {
                service.upsertJobScheduler(
                    queueName,
                    'insert-fails',
                    new Queues.RepeatOptions().every(5).startAt(System.now().addMinutes(5)),
                    new Queues.JobTemplate(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                    )
                );
                Assert.fail('Expected scheduler upsert to fail when job insert fails.');
            } catch (Queues.QueueException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            Assert.areNotEqual(null, failureMessage);
            Assert.areEqual(0, [SELECT COUNT() FROM JobScheduler__c WHERE QueueName__c = :queueName]);
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_abort_pattern_schedule_when_next_fire_time_is_missing() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-pattern-no-fire-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 816);
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('schedulePattern', scheduledJobId);
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobById', new List<CronTrigger>());
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );
            String failureMessage;

            // Act
            try {
                service.upsertJobScheduler(
                    queueName,
                    'missing-fire-time',
                    new Queues.RepeatOptions().pattern('0 0 0 * * ?'),
                    new Queues.JobTemplate(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                    )
                );
                Assert.fail('Expected scheduler upsert to fail when cron next fire time is missing.');
            } catch (Queues.ConfigurationException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            Assert.areEqual(
                'Queue "' + queueName + '" job scheduler "missing-fire-time" does not have a future run.',
                failureMessage
            );
        }
    }

    @IsTest
    static void should_fail_pattern_upsert_when_schedule_id_is_missing() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-pattern-no-id-' + System.now().getTime();
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('schedulePattern', null);
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );
            String failureMessage;

            // Act
            try {
                service.upsertJobScheduler(
                    queueName,
                    'missing-schedule-id',
                    new Queues.RepeatOptions().pattern('0 0 0 * * ?'),
                    new Queues.JobTemplate(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                    )
                );
                Assert.fail('Expected scheduler upsert to fail when schedule id is missing.');
            } catch (Queues.ConfigurationException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            Assert.areEqual(
                'Queue "' + queueName + '" job scheduler "missing-schedule-id" does not have a future run.',
                failureMessage
            );
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('queryScheduledJobById'));
        }
    }

    @IsTest
    static void should_rollback_materialization_and_abort_next_schedule_when_enqueue_fails() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-materialize-fail-' + System.now().getTime();
            Id currentScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 817);
            Id nextScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 818);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'materialize-fails');
            scheduler.ScheduledJobId__c = currentScheduledJobId;
            scheduler.NextRunAt__c = System.now().addMinutes(5);
            scheduler.EveryMinutes__c = 5;
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', nextScheduledJobId);
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobById',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            nextScheduledJobId,
                            'WAITING',
                            '0 0 0 1 1 ? 2099',
                            'Apex Queue Every Scheduler Test',
                            System.now().addMinutes(10)
                        )
                    }
                );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                .withReturn('mapToJob', new Job__c())
                .withReturn('insertJobsAllowingPartialSuccess', buildFailedInsertSaveResults());
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService(),
                new QueuesJobSchedulerRepository(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesErrorService(),
                true
            );
            String failureMessage;

            // Act
            try {
                service.materializeJobSchedulerAndRecordError(scheduler.Id, currentScheduledJobId);
                Assert.fail('Expected materialization to fail when job insert fails.');
            } catch (Queues.QueueException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT ScheduledJobId__c, IterationCount__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Assert.areNotEqual(null, failureMessage);
            Assert.areEqual(currentScheduledJobId, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(1, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_record_materializer_error_and_rethrow_when_enqueue_fails() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-materializer-error-' + System.now().getTime();
            Id currentScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 848);
            Id nextScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 849);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'materializer-error');
            scheduler.ScheduledJobId__c = currentScheduledJobId;
            scheduler.NextRunAt__c = System.now().addMinutes(5);
            scheduler.EveryMinutes__c = 5;
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', nextScheduledJobId);
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobById',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            nextScheduledJobId,
                            'WAITING',
                            '0 0 0 1 1 ? 2099',
                            'Apex Queue Every Scheduler Test',
                            System.now().addMinutes(10)
                        )
                    }
                );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                .withReturn('mapToJob', new Job__c())
                .withReturn('insertJobsAllowingPartialSuccess', buildFailedInsertSaveResults());
            QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService(),
                new QueuesJobSchedulerRepository(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                newRecordingErrorService(eventRepositoryProvider),
                true
            );
            String failureMessage;

            // Act
            try {
                service.materializeJobSchedulerAndRecordError(scheduler.Id, currentScheduledJobId);
                Assert.fail('Expected materializer to rethrow materialization failure.');
            } catch (Queues.QueueException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) eventRepositoryProvider.getLastArgs(
                'publishQueueError'
            )[0];
            Assert.areNotEqual(null, failureMessage);
            Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
            Assert.areEqual(queueName, errorEvent.QueueName__c);
            Assert.areEqual('job scheduler materializer: materializer-error', errorEvent.Component__c);
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_return_duplicate_scheduler_job_when_insert_race_is_detected() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-duplicate-race-' + System.now().getTime();
            Id currentScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 819);
            Id nextScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 820);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'duplicate-race');
            scheduler.ScheduledJobId__c = currentScheduledJobId;
            scheduler.NextRunAt__c = System.now().addMinutes(5);
            scheduler.EveryMinutes__c = 5;
            insert scheduler;

            String externalId = 'scheduler:' + scheduler.Id + ':2';
            Job__c duplicateJob = QueuesTestSupport.job(
                queueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.DELAYED.name(),
                externalId
            );
            duplicateJob.JobScheduler__c = scheduler.Id;
            insert duplicateJob;

            Job__c mappedJob = QueuesTestSupport.job(
                queueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.DELAYED.name(),
                externalId
            );
            Map<String, Job__c> duplicateJobsByExternalId = new Map<String, Job__c>{
                QueuesSupport.normalizeCaseInsensitiveKey(externalId) => duplicateJob
            };
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', nextScheduledJobId);
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns(
                    'getJobsByExternalId',
                    new List<Object>{ new Map<String, Job__c>(), duplicateJobsByExternalId }
                )
                .withReturn('mapToJob', mappedJob)
                .withReturn('insertJobsAllowingPartialSuccess', buildDuplicateInsertSaveResults(mappedJob));
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider),
                new QueuesJobSchedulerRepository(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesErrorService(),
                true
            );

            // Act
            service.materializeJobSchedulerAndRecordError(scheduler.Id, currentScheduledJobId);

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT ScheduledJobId__c, IterationCount__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Assert.areEqual(nextScheduledJobId, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(2, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_skip_job_scheduler_maintenance_when_queue_is_inactive() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-inactive-queue-' + System.now().getTime();
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'inactive-queue');
            scheduler.ScheduledJobId__c = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 821);
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesWorkerService(),
                new QueuesJobSchedulerRepository(),
                new QueuesJobService(),
                new QueuesErrorService(),
                false
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            Assert.areEqual(0, repaired.total);
            Assert.areEqual(0, scheduleProvider.getCallCount('scheduleEvery'));
        }
    }

    @IsTest
    static void should_skip_job_scheduler_maintenance_when_scheduler_has_no_repeat_mode() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-no-mode-maint-' + System.now().getTime();
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'no-mode-maintenance');
            scheduler.ScheduledJobId__c = null;
            scheduler.EveryMinutes__c = null;
            scheduler.Pattern__c = null;
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            Assert.areEqual(0, repaired.total);
            Assert.areEqual(0, scheduleProvider.getCallCount('scheduleEvery'));
            Assert.areEqual(0, scheduleProvider.getCallCount('schedulePattern'));
        }
    }

    @IsTest
    static void should_skip_job_scheduler_maintenance_when_no_schedulers_are_active() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-no-active-maint-' + System.now().getTime();
            QueuesTestSupport.RecordingStubProvider schedulerRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryActiveJobSchedulers', new List<JobScheduler__c>());
            QueuesTestSupport.RecordingStubProvider definitionProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                (QueuesDefinitionService) Test.createStub(QueuesDefinitionService.class, definitionProvider),
                new QueuesErrorService(),
                (QueuesJobSchedulerRepository) Test.createStub(
                    QueuesJobSchedulerRepository.class,
                    schedulerRepositoryProvider
                ),
                new QueuesJobService(),
                newProcessorService(queueName),
                new QueuesQueueableDispatchService(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesJobSchedulerSchedule(),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            Assert.areEqual(0, repaired.total);
            Assert.areEqual(0, definitionProvider.getCallCount('findQueueDefinitionsByQueueNamesAsSystem'));
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('queryScheduledJobsByNamePrefix'));
        }
    }

    @IsTest
    static void should_skip_job_scheduler_maintenance_when_identity_changes_before_lock() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-maint-identity-' + System.now().getTime();
            Id schedulerId = QueuesTestSupport.fakeId(JobScheduler__c.SObjectType, 3);
            JobScheduler__c discoveredScheduler = QueuesTestSupport.jobScheduler(queueName, 'identity-changed');
            discoveredScheduler.Id = schedulerId;
            discoveredScheduler.ScheduledJobId__c = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 4);
            JobScheduler__c lockedScheduler = (JobScheduler__c) QueuesSupport.clone(discoveredScheduler);
            lockedScheduler.ScheduledJobId__c = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 5);
            QueuesTestSupport.RecordingStubProvider schedulerRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryActiveJobSchedulers', new List<JobScheduler__c>{ discoveredScheduler })
                .withReturn('queryJobSchedulersByIdForUpdate', new List<JobScheduler__c>{ lockedScheduler });
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                newDefinitionService(queueName, true),
                new QueuesErrorService(),
                (QueuesJobSchedulerRepository) Test.createStub(
                    QueuesJobSchedulerRepository.class,
                    schedulerRepositoryProvider
                ),
                new QueuesJobService(),
                newProcessorService(queueName),
                new QueuesQueueableDispatchService(),
                (QueuesScheduleRepository) Test.createStub(
                    QueuesScheduleRepository.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>())
                ),
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            Assert.areEqual(0, repaired.total);
            Assert.areEqual(1, schedulerRepositoryProvider.getCallCount('queryJobSchedulersByIdForUpdate'));
            Assert.areEqual(0, scheduleProvider.getCallCount('scheduleEvery'));
            Assert.areEqual(0, scheduleProvider.getCallCount('schedulePattern'));
        }
    }

    @IsTest
    static void should_skip_job_scheduler_maintenance_when_lifecycle_has_no_headroom() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-maint-no-headroom-' + System.now().getTime();
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'maintenance-no-headroom');
            scheduler.ScheduledJobId__c = null;
            insert scheduler;
            consumeDmlRowsUntilLifecycleHeadroomIsExhausted(queueName);

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>());
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            JobScheduler__c skippedScheduler = [
                SELECT ScheduledJobId__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Assert.areEqual(0, repaired.total);
            Assert.areEqual(null, skippedScheduler.ScheduledJobId__c);
            Assert.areEqual(0, scheduleProvider.getCallCount('scheduleEvery'));
            Assert.areEqual(0, scheduleProvider.getCallCount('schedulePattern'));
        }
    }

    @IsTest
    static void should_record_error_when_job_scheduler_maintenance_fails() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-maint-error-' + System.now().getTime();
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'maintenance-error');
            QueuesTestSupport.RecordingStubProvider schedulerRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryActiveJobSchedulers', new List<JobScheduler__c>{ scheduler });
            QueuesTestSupport.RecordingStubProvider definitionProvider = new QueuesTestSupport.RecordingStubProvider()
                .withException(
                    'findQueueDefinitionsByQueueNamesAsSystem',
                    new Queues.QueueException('metadata exploded')
                );
            QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                (QueuesDefinitionService) Test.createStub(QueuesDefinitionService.class, definitionProvider),
                newRecordingErrorService(eventRepositoryProvider),
                (QueuesJobSchedulerRepository) Test.createStub(
                    QueuesJobSchedulerRepository.class,
                    schedulerRepositoryProvider
                ),
                new QueuesJobService(),
                newProcessorService(queueName),
                new QueuesQueueableDispatchService(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesJobSchedulerSchedule(),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            Assert.areEqual(0, repaired.total);
            Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
        }
    }

    @IsTest
    static void should_abort_duplicate_pattern_schedules_when_primary_schedule_is_healthy() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-pattern-healthy-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 822);
            Id duplicateScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 823);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'pattern-healthy');
            scheduler.ScheduledJobId__c = scheduledJobId;
            scheduler.EveryMinutes__c = null;
            scheduler.Pattern__c = '0 0 0 * * ?';
            insert scheduler;
            String jobNamePrefix = QueuesJobSchedulerSchedule.resolvePatternJobNamePrefix(scheduler.Id);

            CronTrigger primaryJob = QueuesTestSupport.cronTrigger(
                scheduledJobId,
                'WAITING',
                scheduler.Pattern__c,
                jobNamePrefix + 'primary'
            );
            CronTrigger duplicateJob = QueuesTestSupport.cronTrigger(
                duplicateScheduledJobId,
                'WAITING',
                scheduler.Pattern__c,
                jobNamePrefix + 'duplicate'
            );
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>{ primaryJob, duplicateJob })
                .withReturn('queryScheduledJobById', new List<CronTrigger>{ duplicateJob });
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            Assert.areEqual(1, repaired.total);
            Assert.areEqual(1, repaired.getCount(queueName));
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('queryScheduledJobById'));
        }
    }

    @IsTest
    static void should_repair_pattern_scheduler_when_native_schedule_is_missing() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-pattern-repair-' + System.now().getTime();
            Id oldScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 824);
            Id repairedScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 825);
            Datetime nextRunAt = System.now().addHours(2);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'pattern-repair');
            scheduler.ScheduledJobId__c = oldScheduledJobId;
            scheduler.EveryMinutes__c = null;
            scheduler.Pattern__c = '0 0 0 * * ?';
            insert scheduler;

            CronTrigger repairedJob = QueuesTestSupport.cronTrigger(
                repairedScheduledJobId,
                'WAITING',
                scheduler.Pattern__c,
                QueuesJobSchedulerSchedule.resolvePatternJobNamePrefix(scheduler.Id) + 'repaired',
                nextRunAt
            );
            CronTrigger oldJob = QueuesTestSupport.cronTrigger(
                oldScheduledJobId,
                'WAITING',
                scheduler.Pattern__c,
                QueuesJobSchedulerSchedule.resolvePatternJobNamePrefix(scheduler.Id) + 'old'
            );
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('schedulePattern', repairedScheduledJobId);
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>())
                .withReturns(
                    'queryScheduledJobById',
                    new List<Object>{ new List<CronTrigger>{ repairedJob }, new List<CronTrigger>{ oldJob } }
                );
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT ScheduledJobId__c, NextRunAt__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Assert.areEqual(1, repaired.total);
            Assert.areEqual(1, repaired.getCount(queueName));
            Assert.areEqual(repairedScheduledJobId, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(nextRunAt, updatedScheduler.NextRunAt__c);
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_record_error_when_pattern_repair_has_no_future_run() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-pattern-repair-error-' + System.now().getTime();
            Id oldScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 826);
            Id repairedScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 827);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'pattern-repair-error');
            scheduler.ScheduledJobId__c = oldScheduledJobId;
            scheduler.EveryMinutes__c = null;
            scheduler.Pattern__c = '0 0 0 * * ?';
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('schedulePattern', repairedScheduledJobId);
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>())
                .withReturn('queryScheduledJobById', new List<CronTrigger>());
            QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService(),
                new QueuesJobSchedulerRepository(),
                new QueuesJobService(),
                newRecordingErrorService(eventRepositoryProvider),
                true
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            Assert.areEqual(0, repaired.total);
            Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
        }
    }

    @IsTest
    static void should_abort_duplicate_every_schedules_when_primary_schedule_is_healthy() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-every-healthy-' + System.now().getTime();
            Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 828);
            Id duplicateScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 829);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'every-healthy');
            scheduler.ScheduledJobId__c = scheduledJobId;
            insert scheduler;
            String jobNamePrefix = QueuesJobSchedulerSchedule.resolveEveryJobNamePrefix(scheduler.Id);

            CronTrigger primaryJob = QueuesTestSupport.cronTrigger(
                scheduledJobId,
                'WAITING',
                '0 0 0 1 1 ? 2099',
                jobNamePrefix + 'primary'
            );
            CronTrigger duplicateJob = QueuesTestSupport.cronTrigger(
                duplicateScheduledJobId,
                'WAITING',
                '0 0 0 1 1 ? 2099',
                jobNamePrefix + 'duplicate'
            );
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>{ primaryJob, duplicateJob })
                .withReturn('queryScheduledJobById', new List<CronTrigger>{ duplicateJob });
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                new QueuesJobSchedulerSchedule(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            Assert.areEqual(1, repaired.total);
            Assert.areEqual(1, repaired.getCount(queueName));
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('queryScheduledJobById'));
        }
    }

    @IsTest
    static void should_repair_every_scheduler_when_native_schedule_is_missing() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-every-repair-' + System.now().getTime();
            Id oldScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 830);
            Id repairedScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 831);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'every-repair');
            scheduler.ScheduledJobId__c = oldScheduledJobId;
            scheduler.NextRunAt__c = System.now().addHours(1);
            insert scheduler;

            CronTrigger oldJob = QueuesTestSupport.cronTrigger(
                oldScheduledJobId,
                'WAITING',
                '0 0 0 1 1 ? 2099',
                QueuesJobSchedulerSchedule.resolveEveryJobNamePrefix(scheduler.Id) + 'old'
            );
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', repairedScheduledJobId);
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>())
                .withReturns('queryScheduledJobById', new List<Object>{ new List<CronTrigger>{ oldJob } });
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT ScheduledJobId__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Assert.areEqual(1, repaired.total);
            Assert.areEqual(1, repaired.getCount(queueName));
            Assert.areEqual(repairedScheduledJobId, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_repair_overdue_every_scheduler_from_current_time() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-every-overdue-' + System.now().getTime();
            Id oldScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 832);
            Id repairedScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 833);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'every-overdue');
            scheduler.ScheduledJobId__c = oldScheduledJobId;
            scheduler.NextRunAt__c = System.now().addMinutes(-10);
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', repairedScheduledJobId);
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(
                    QueuesScheduleRepository.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>())
                        .withReturn('queryScheduledJobById', new List<CronTrigger>())
                ),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            Assert.areEqual(1, repaired.total);
            Assert.areEqual(1, repaired.getCount(queueName));
            Assert.areEqual(1, scheduleProvider.getCallCount('scheduleEvery'));
        }
    }

    @IsTest
    static void should_repair_every_scheduler_when_scheduled_job_id_is_missing() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-every-missing-id-' + System.now().getTime();
            Id repairedScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 838);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'every-missing-id');
            scheduler.ScheduledJobId__c = null;
            scheduler.NextRunAt__c = System.now().addHours(1);
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', repairedScheduledJobId);
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>());
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            JobScheduler__c updatedScheduler = [
                SELECT ScheduledJobId__c
                FROM JobScheduler__c
                WHERE Id = :scheduler.Id
            ];
            Assert.areEqual(1, repaired.total);
            Assert.areEqual(1, repaired.getCount(queueName));
            Assert.areEqual(repairedScheduledJobId, updatedScheduler.ScheduledJobId__c);
            Assert.areEqual(1, scheduleProvider.getCallCount('scheduleEvery'));
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('queryScheduledJobById'));
        }
    }

    @IsTest
    static void should_record_error_when_every_repair_fails() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-every-repair-error-' + System.now().getTime();
            Id oldScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 834);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'every-repair-error');
            scheduler.ScheduledJobId__c = oldScheduledJobId;
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withException('scheduleEvery', new Queues.QueueException('schedule exploded'));
            QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                queueName,
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesScheduleRepository) Test.createStub(
                    QueuesScheduleRepository.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>())
                        .withReturn('queryScheduledJobById', new List<CronTrigger>())
                ),
                new QueuesWorkerService(),
                new QueuesJobSchedulerRepository(),
                new QueuesJobService(),
                newRecordingErrorService(eventRepositoryProvider),
                true
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenanceAsSystem();

            // Assert
            Assert.areEqual(0, repaired.total);
            Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
        }
    }

    @IsTest
    static void should_abandon_materialized_event_when_scheduler_materialization_rolls_back() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-event-rollback-' + System.now().getTime();
            Id currentScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 961);
            Id nextScheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 962);
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'event-rollback');
            scheduler.ScheduledJobId__c = currentScheduledJobId;
            insert scheduler;

            QueuesTestSupport.RecordingStubProvider schedulerRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryJobSchedulersByIdForUpdate', new List<JobScheduler__c>{ scheduler })
                .withReturn('queryJobSchedulersById', new List<JobScheduler__c>{ scheduler })
                .withException('updateJobScheduler', new Queues.QueueException('scheduler update exploded'));
            QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('scheduleEvery', nextScheduledJobId);
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobById', new List<CronTrigger>());
            QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider errorEventProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobSchedulerService service = newSchedulerService(
                newDefinitionService(queueName, true),
                newRecordingErrorService(errorEventProvider),
                (QueuesJobSchedulerRepository) Test.createStub(
                    QueuesJobSchedulerRepository.class,
                    schedulerRepositoryProvider
                ),
                new QueuesJobService(),
                newProcessorService(queueName),
                new QueuesQueueableDispatchService(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                (QueuesJobSchedulerSchedule) Test.createStub(QueuesJobSchedulerSchedule.class, scheduleProvider),
                (QueuesWorkerService) Test.createStub(
                    QueuesWorkerService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                ),
                QueuesTestSupport.newRecordingQueueEventService(queueName, 'SCHEDULER_MATERIALIZED', eventProvider)
            );

            // Act
            try {
                service.materializeJobSchedulerAndRecordError(scheduler.Id, String.valueOf(currentScheduledJobId));
                Assert.fail('Expected scheduler materialization to fail.');
            } catch (Queues.QueueException ex) {
                Assert.areEqual('scheduler update exploded', ex.getMessage());
            }

            // Assert
            Assert.areEqual(0, eventProvider.getCallCount('publishQueueEvents'));
            Assert.areEqual(1, errorEventProvider.getCallCount('publishQueueError'));
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id]);
        }
    }

    private static QueuesJobSchedulerService newSchedulerService(
        final String queueName,
        final QueuesJobSchedulerSchedule scheduleService,
        final QueuesScheduleRepository scheduleRepository,
        final QueuesWorkerService workerService
    ) {
        return newSchedulerService(
            queueName,
            scheduleService,
            scheduleRepository,
            workerService,
            new QueuesJobSchedulerRepository(),
            new QueuesJobService(),
            new QueuesErrorService(),
            true
        );
    }

    private static QueuesJobSchedulerService newSchedulerService(
        final String queueName,
        final QueuesJobSchedulerSchedule scheduleService,
        final QueuesScheduleRepository scheduleRepository,
        final QueuesWorkerService workerService,
        final QueuesQueueEventService queueEventService
    ) {
        return newSchedulerService(
            newDefinitionService(queueName, true),
            new QueuesErrorService(),
            new QueuesJobSchedulerRepository(),
            new QueuesJobService(),
            newProcessorService(queueName),
            new QueuesQueueableDispatchService(),
            scheduleRepository,
            scheduleService,
            workerService,
            queueEventService
        );
    }

    private static QueuesJobSchedulerService newSchedulerService(
        final String queueName,
        final QueuesJobSchedulerSchedule scheduleService,
        final QueuesScheduleRepository scheduleRepository,
        final QueuesWorkerService workerService,
        final QueuesJobSchedulerRepository schedulerRepository,
        final QueuesJobService jobService,
        final QueuesErrorService errorService,
        final Boolean queueIsActive
    ) {
        return newSchedulerService(
            newDefinitionService(queueName, queueIsActive),
            errorService,
            schedulerRepository,
            jobService,
            newProcessorService(queueName),
            new QueuesQueueableDispatchService(),
            scheduleRepository,
            scheduleService,
            workerService
        );
    }

    private static QueuesJobSchedulerService newSchedulerService(
        final QueuesDefinitionService definitionService,
        final QueuesErrorService errorService,
        final QueuesJobSchedulerRepository schedulerRepository,
        final QueuesJobService jobService,
        final QueuesProcessorService processorService,
        final QueuesQueueableDispatchService queueableDispatchService,
        final QueuesScheduleRepository scheduleRepository,
        final QueuesJobSchedulerSchedule scheduleService,
        final QueuesWorkerService workerService
    ) {
        return newSchedulerService(
            definitionService,
            errorService,
            schedulerRepository,
            jobService,
            processorService,
            queueableDispatchService,
            scheduleRepository,
            scheduleService,
            workerService,
            new QueuesQueueEventService()
        );
    }

    private static QueuesJobSchedulerService newSchedulerService(
        final QueuesDefinitionService definitionService,
        final QueuesErrorService errorService,
        final QueuesJobSchedulerRepository schedulerRepository,
        final QueuesJobService jobService,
        final QueuesProcessorService processorService,
        final QueuesQueueableDispatchService queueableDispatchService,
        final QueuesScheduleRepository scheduleRepository,
        final QueuesJobSchedulerSchedule scheduleService,
        final QueuesWorkerService workerService,
        final QueuesQueueEventService queueEventService
    ) {
        return new QueuesJobSchedulerService(
            definitionService,
            errorService,
            schedulerRepository,
            jobService,
            processorService,
            queueableDispatchService,
            scheduleRepository,
            scheduleService,
            new QueuesFlowProducerService(),
            workerService,
            queueEventService
        );
    }

    private static List<QueueEvent__e> publishedEventsByType(
        final QueuesTestSupport.RecordingStubProvider eventProvider,
        final String eventType
    ) {
        List<QueueEvent__e> matchingEvents = new List<QueueEvent__e>();
        for (List<Object> callArgs : eventProvider.getCalls('publishQueueEvents')) {
            for (QueueEvent__e eventRecord : (List<QueueEvent__e>) callArgs[0]) {
                if (eventRecord.Type__c == eventType) {
                    matchingEvents.add(eventRecord);
                }
            }
        }
        return matchingEvents;
    }

    private static QueuesErrorService newRecordingErrorService(
        final QueuesTestSupport.RecordingStubProvider eventRepositoryProvider
    ) {
        QueuesErrorService.resetEmittedErrorFingerprints();
        return new QueuesErrorService(
            new QueuesErrorRepository(),
            (QueuesErrorEventRepository) Test.createStub(QueuesErrorEventRepository.class, eventRepositoryProvider)
        );
    }

    private static QueuesDefinitionService newDefinitionService(final String queueName, final Boolean isActive) {
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
        queueDefinition.IsActive__c = isActive;
        return (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('requireQueueDefinition', queueDefinition)
                .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                .withReturn(
                    'findQueueDefinitionsByQueueNamesAsSystem',
                    new Map<String, QueueDefinition__mdt>{ queueName => queueDefinition }
                )
        );
    }

    private static QueuesProcessorService newProcessorService(final String queueName) {
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition(
            queueName,
            QueuesTestSupport.DEFAULT_JOB_NAME,
            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            true
        );
        return (QueuesProcessorService) Test.createStub(
            QueuesProcessorService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'requireProcessors',
                    new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
                )
        );
    }

    private static JobScheduler__c queryOnlyScheduler(final String queueName, final String jobSchedulerId) {
        return [
            SELECT
                Id,
                ScheduledJobId__c,
                IterationCount__c,
                EveryMinutes__c,
                Pattern__c,
                NextRunAt__c,
                ExecutionMode__c,
                JobName__c,
                ProcessorClass__c
            FROM JobScheduler__c
            WHERE QueueName__c = :queueName AND Name = :jobSchedulerId
            LIMIT 1
        ];
    }

    private static Job__c queryOnlySchedulerJob(final Id schedulerId, final Integer iterationCount) {
        String externalId = 'scheduler:' + schedulerId + ':' + iterationCount;
        return [
            SELECT Id, JobScheduler__c, ExternalId__c, AvailableAt__c, ExecutionMode__c, QueueableDispatch__c
            FROM Job__c
            WHERE JobScheduler__c = :schedulerId AND ExternalId__c = :externalId
            LIMIT 1
        ];
    }

    @SuppressWarnings('PMD.OperationWithLimitsInLoop')
    private static void consumeDmlRowsUntilLifecycleHeadroomIsExhausted(final String queueName) {
        Job__c anchorJob = QueuesTestSupport.job(
            queueName,
            'headroom-anchor',
            Queues.State.COMPLETED.name(),
            'headroom-anchor'
        );
        insert anchorJob;

        Integer rowsToInsert = Limits.getLimitDmlRows() - Limits.getDmlRows() - 500;
        Integer insertedRows = 0;
        while (insertedRows < rowsToInsert) {
            List<JobRun__c> jobRuns = new List<JobRun__c>();
            Integer chunkSize = Math.min(1000, rowsToInsert - insertedRows);
            for (Integer i = 0; i < chunkSize; i++) {
                jobRuns.add(
                    new JobRun__c(
                        Job__c = anchorJob.Id,
                        AttemptNumber__c = insertedRows + i + 1,
                        StartedAt__c = System.now(),
                        Status__c = Queues.RunStatus.COMPLETED.name()
                    )
                );
            }
            insert jobRuns;
            insertedRows += chunkSize;
        }
    }

    private static List<Database.SaveResult> buildFailedInsertSaveResults() {
        return Database.insert(new List<Job__c>{ new Job__c() }, false);
    }

    private static List<Database.SaveResult> buildDuplicateInsertSaveResults(final Job__c job) {
        return Database.insert(new List<Job__c>{ (Job__c) QueuesSupport.clone(job) }, false);
    }

    private class RacingJobSchedulerRepository extends QueuesJobSchedulerRepository {
        private Integer insertAttempts = 0;
        private Integer queryCount = 0;
        private Boolean insertedCompetingScheduler = false;

        public override List<JobScheduler__c> queryJobSchedulersByExternalId(final String externalId) {
            queryCount++;
            if (queryCount == 1) {
                return new List<JobScheduler__c>();
            }
            return super.queryJobSchedulersByExternalId(externalId);
        }

        public override JobScheduler__c insertJobScheduler(final JobScheduler__c jobScheduler) {
            insertAttempts++;
            if (!insertedCompetingScheduler) {
                insertedCompetingScheduler = true;
                insert new JobScheduler__c(
                    Name = jobScheduler.Name,
                    QueueName__c = jobScheduler.QueueName__c,
                    ExternalId__c = jobScheduler.ExternalId__c,
                    IsActive__c = true,
                    IterationCount__c = 0
                );
            }
            return super.insertJobScheduler(jobScheduler);
        }
    }

    private class FailingJobSchedulerRepository extends QueuesJobSchedulerRepository {
        public override JobScheduler__c insertJobScheduler(final JobScheduler__c jobScheduler) {
            insert new JobScheduler__c();
            return jobScheduler;
        }
    }

    private class UnreconciledDuplicateJobSchedulerRepository extends QueuesJobSchedulerRepository {
        private Boolean insertedCompetingScheduler = false;

        public override List<JobScheduler__c> queryJobSchedulersByExternalId(final String externalId) {
            return new List<JobScheduler__c>();
        }

        public override JobScheduler__c insertJobScheduler(final JobScheduler__c jobScheduler) {
            if (!insertedCompetingScheduler) {
                insertedCompetingScheduler = true;
                insert new JobScheduler__c(
                    Name = jobScheduler.Name,
                    QueueName__c = jobScheduler.QueueName__c,
                    ExternalId__c = jobScheduler.ExternalId__c,
                    IsActive__c = true,
                    IterationCount__c = 0
                );
            }
            return super.insertJobScheduler(jobScheduler);
        }
    }
}
