@IsTest(IsParallel=false)
private class QueuesJobSchedulerScheduleTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_build_one_shot_cron_expression_for_every_scheduler() {
        // Arrange
        Datetime runAt = Datetime.newInstance(2099, 1, 2, 3, 4, 5);

        // Act
        String cronExpression = QueuesJobSchedulerSchedule.buildOneShotCronExpression(runAt);

        // Assert
        Assert.areEqual('5 4 3 2 1 ? 2099', cronExpression);
        Assert.isTrue(QueuesJobSchedulerSchedule.resolveEveryJobNamePrefix().startsWith('Apex Queue Every Scheduler '));
        Assert.isTrue(
            QueuesJobSchedulerSchedule.resolvePatternJobNamePrefix().startsWith('Apex Queue Pattern Scheduler ')
        );
    }

    @IsTest
    static void should_schedule_pattern_and_every_materializers() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(
                'schedule-helper-' + System.now().getTime(),
                'nightly'
            );
            insert scheduler;
            QueuesJobSchedulerSchedule schedule = new QueuesJobSchedulerSchedule();

            // Act
            String patternScheduledJobId = schedule.schedulePattern(scheduler.Id, '0 0 0 1 1 ? 2099');
            String everyScheduledJobId = schedule.scheduleEvery(
                scheduler.Id,
                Datetime.newInstanceGmt(2099, 1, 2, 3, 4, 5)
            );
            Set<String> scheduledJobIds = new Set<String>{ patternScheduledJobId, everyScheduledJobId };

            try {
                // Assert
                Assert.areNotEqual(null, patternScheduledJobId);
                Assert.areNotEqual(null, everyScheduledJobId);
                List<CronTrigger> scheduledJobs = Database.queryWithBinds(
                    'SELECT Id, CronJobDetail.Name, CronExpression FROM CronTrigger WHERE Id IN :scheduledJobIds',
                    new Map<String, Object>{ 'scheduledJobIds' => scheduledJobIds },
                    System.AccessLevel.SYSTEM_MODE
                );
                Assert.areEqual(2, scheduledJobs.size());
            } finally {
                System.abortJob(patternScheduledJobId);
                System.abortJob(everyScheduledJobId);
            }
        }
    }

    @IsTest
    static void should_ignore_materializer_execute_when_scheduler_id_is_missing() {
        // Arrange
        QueuesJobSchedulerMaterializer materializer = new QueuesJobSchedulerMaterializer(null);

        // Act
        Test.startTest();
        System.schedule(
            'Apex Queue Missing Scheduler Test ' + System.now().getTime(),
            '0 0 0 1 1 ? 2099',
            materializer
        );
        Test.stopTest();

        // Assert
        Assert.areEqual(0, [SELECT COUNT() FROM Job__c]);
    }
}
