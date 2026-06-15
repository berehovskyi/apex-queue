@IsTest(IsParallel=false)
private class QueuesMaintenanceScheduleTest {
    // <editor-fold desc="Schedule Lookup">
    @IsTest
    static void should_return_existing_job_id_when_maintenance_job_is_already_scheduled() {
        // Arrange
        Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 1);
        CronTrigger scheduledJob = QueuesTestSupport.cronTrigger(
            scheduledJobId,
            'WAITING',
            QueuesMaintenanceSchedule.DEFAULT_CRON_EXPRESSION,
            QueuesMaintenanceSchedule.resolveJobName()
        );
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryScheduledJobs', new List<CronTrigger>{ scheduledJob });
        QueuesScheduleRepository repositoryStub = (QueuesScheduleRepository) Test.createStub(
            QueuesScheduleRepository.class,
            repositoryProvider
        );

        QueuesMaintenanceSchedule service = new QueuesMaintenanceSchedule(repositoryStub);

        // Act
        String returnedJobId = service.schedule(QueuesMaintenanceSchedule.DEFAULT_CRON_EXPRESSION);
        Boolean isScheduled = service.isScheduled();

        // Assert
        Assert.areEqual(scheduledJobId, returnedJobId);
        Assert.areEqual(true, isScheduled);
        Assert.areEqual(2, repositoryProvider.getCallCount('queryScheduledJobs'));
    }

    @IsTest
    static void should_report_unscheduled_when_no_maintenance_job_exists() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns('queryScheduledJobs', new List<Object>{ new List<CronTrigger>(), new List<CronTrigger>() });
        QueuesScheduleRepository repositoryStub = (QueuesScheduleRepository) Test.createStub(
            QueuesScheduleRepository.class,
            repositoryProvider
        );

        QueuesMaintenanceSchedule service = new QueuesMaintenanceSchedule(repositoryStub);

        // Act
        Boolean isScheduled = service.isScheduled();
        Integer unscheduledCount = service.unschedule();

        // Assert
        Assert.areEqual(false, isScheduled);
        Assert.areEqual(0, unscheduledCount);
        Assert.areEqual(2, repositoryProvider.getCallCount('queryScheduledJobs'));
    }

    @IsTest
    static void should_schedule_and_unschedule_runtime_maintenance_when_no_existing_job_is_found() {
        // Arrange
        QueuesScheduleRepository repositoryStub = (QueuesScheduleRepository) Test.createStub(
            QueuesScheduleRepository.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('queryScheduledJobs', new List<CronTrigger>())
        );

        QueuesMaintenanceSchedule service = new QueuesMaintenanceSchedule(repositoryStub);

        // Act
        String scheduledJobId = service.schedule();
        Integer unscheduledCount = new QueuesMaintenanceSchedule().unschedule();

        // Assert
        Assert.areEqual(true, String.isNotBlank(scheduledJobId));
        Assert.areEqual(1, unscheduledCount);
    }

    @IsTest
    static void should_reschedule_runtime_maintenance_when_custom_cron_expression_is_provided() {
        // Arrange
        QueuesScheduleRepository repositoryStub = (QueuesScheduleRepository) Test.createStub(
            QueuesScheduleRepository.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('queryScheduledJobs', new List<CronTrigger>())
        );

        QueuesMaintenanceSchedule service = new QueuesMaintenanceSchedule(repositoryStub);

        // Act
        String scheduledJobId = service.reschedule('0 15 * * * ?');

        // Assert
        Assert.areEqual(true, String.isNotBlank(scheduledJobId));
    }

    @IsTest
    static void should_return_existing_job_when_concurrent_schedule_wins_race_after_initial_lookup() {
        // Arrange
        Id scheduledJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 2);
        CronTrigger scheduledJob = QueuesTestSupport.cronTrigger(
            scheduledJobId,
            'WAITING',
            QueuesMaintenanceSchedule.DEFAULT_CRON_EXPRESSION,
            QueuesMaintenanceSchedule.resolveJobName()
        );
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns(
                'queryScheduledJobs',
                new List<Object>{ new List<CronTrigger>(), new List<CronTrigger>{ scheduledJob } }
            );
        QueuesMaintenanceSchedule service = new ConcurrentRaceMaintenanceSchedule(
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, repositoryProvider)
        );

        // Act
        String returnedJobId = service.schedule(QueuesMaintenanceSchedule.DEFAULT_CRON_EXPRESSION);

        // Assert
        Assert.areEqual(scheduledJobId, returnedJobId);
        Assert.areEqual(2, repositoryProvider.getCallCount('queryScheduledJobs'));
    }

    @IsTest
    static void should_rethrow_schedule_failure_when_retry_lookup_still_finds_no_job() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns('queryScheduledJobs', new List<Object>{ new List<CronTrigger>(), new List<CronTrigger>() });
        QueuesMaintenanceSchedule service = new ConcurrentRaceMaintenanceSchedule(
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, repositoryProvider)
        );

        String failureMessage;

        // Act
        try {
            service.schedule(QueuesMaintenanceSchedule.DEFAULT_CRON_EXPRESSION);
            Assert.fail('Expected schedule failure to be rethrown.');
        } catch (Queues.QueueException ex) {
            failureMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual('Concurrent schedule already created.', failureMessage);
        Assert.areEqual(2, repositoryProvider.getCallCount('queryScheduledJobs'));
    }

    @IsTest
    static void should_continue_unscheduling_remaining_jobs_when_one_abort_fails() {
        // Arrange
        Id firstJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 3);
        Id secondJobId = QueuesTestSupport.fakeId(CronTrigger.SObjectType, 4);
        QueuesScheduleRepository repositoryStub = (QueuesScheduleRepository) Test.createStub(
            QueuesScheduleRepository.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobs',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            firstJobId,
                            'WAITING',
                            QueuesMaintenanceSchedule.DEFAULT_CRON_EXPRESSION,
                            QueuesMaintenanceSchedule.resolveJobName()
                        ),
                        QueuesTestSupport.cronTrigger(
                            secondJobId,
                            'WAITING',
                            QueuesMaintenanceSchedule.DEFAULT_CRON_EXPRESSION,
                            QueuesMaintenanceSchedule.resolveJobName()
                        )
                    }
                )
        );
        PartialAbortFailureMaintenanceSchedule service = new PartialAbortFailureMaintenanceSchedule(repositoryStub);

        // Act
        Integer unscheduledCount = service.unschedule();

        // Assert
        Assert.areEqual(1, unscheduledCount);
        Assert.areEqual(2, service.abortCalls);
        Assert.areEqual(2, service.abortedJobIds.size());
        Assert.areEqual(firstJobId, service.abortedJobIds[0]);
        Assert.areEqual(secondJobId, service.abortedJobIds[1]);
    }

    @IsTest
    static void should_schedule_is_scheduled_and_unschedule_maintenance_through_queues_facade() {
        // Act
        Boolean scheduledBefore = Queues.isMaintenanceScheduled();
        Id scheduledJobId = Queues.scheduleMaintenance();
        Boolean scheduledAfter = Queues.isMaintenanceScheduled();
        Id idempotentJobId = Queues.scheduleMaintenance();
        Integer unscheduledCount = Queues.unscheduleMaintenance();
        Boolean scheduledFinal = Queues.isMaintenanceScheduled();

        // Assert
        Assert.areEqual(false, scheduledBefore);
        Assert.areNotEqual(null, scheduledJobId);
        Assert.areEqual(true, scheduledAfter);
        Assert.areEqual(scheduledJobId, idempotentJobId);
        Assert.areEqual(1, unscheduledCount);
        Assert.areEqual(false, scheduledFinal);
    }

    @IsTest
    static void should_schedule_maintenance_with_custom_cron_through_queues_facade() {
        // Act
        Id scheduledJobId = Queues.scheduleMaintenance('0 30 * * * ?');
        Integer unscheduledCount = Queues.unscheduleMaintenance();

        // Assert
        Assert.areNotEqual(null, scheduledJobId);
        Assert.areEqual(1, unscheduledCount);
    }

    private class ConcurrentRaceMaintenanceSchedule extends QueuesMaintenanceSchedule {
        private ConcurrentRaceMaintenanceSchedule(final QueuesScheduleRepository repository) {
            super(repository);
        }

        protected override Id scheduleNew(final String cronExpression) {
            throw new Queues.QueueException('Concurrent schedule already created.');
        }
    }

    private class PartialAbortFailureMaintenanceSchedule extends QueuesMaintenanceSchedule {
        public Integer abortCalls = 0;
        public List<String> abortedJobIds = new List<String>();

        private PartialAbortFailureMaintenanceSchedule(final QueuesScheduleRepository repository) {
            super(repository);
        }

        protected override void abortJob(final Id jobId) {
            abortCalls++;
            abortedJobIds.add(jobId);
            if (abortCalls == 1) {
                throw new Queues.QueueException('Abort failed.');
            }
        }
    }
    // </editor-fold>
}
