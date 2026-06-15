@IsTest(IsParallel=false)
private class QueuesScheduleRepositoryTest {
    // <editor-fold desc="Queries">
    @IsTest
    static void should_query_scheduled_jobs_when_cron_trigger_exists() {
        // Arrange
        String jobName = 'Queues Schedule Repo ' + System.now().getTime();
        String scheduledJobId = System.schedule(jobName, '0 0 0 1 1 ? 2099', new NoOpSchedulable());
        QueuesScheduleRepository repository = new QueuesScheduleRepository();

        try {
            // Act
            List<CronTrigger> scheduledJobs = repository.queryScheduledJobs(
                jobName,
                new Set<String>{ 'DELETED', 'COMPLETE' }
            );

            // Assert
            Assert.areEqual(1, scheduledJobs.size());
            Assert.areEqual(scheduledJobId, scheduledJobs[0].Id);
            Assert.areEqual(jobName, scheduledJobs[0].CronJobDetail.Name);
        } finally {
            System.abortJob(scheduledJobId);
        }
    }

    @IsTest
    static void should_query_scheduled_jobs_by_name_prefix_when_cron_trigger_exists() {
        // Arrange
        String jobNamePrefix = 'Queues:repo-prefix-' + System.now().getTime();
        String scheduledJobId = System.schedule(
            jobNamePrefix + ':1713300000000:deadbeef',
            '0 0 0 1 1 ? 2099',
            new NoOpSchedulable()
        );
        QueuesScheduleRepository repository = new QueuesScheduleRepository();

        try {
            // Act
            List<CronTrigger> scheduledJobs = repository.queryScheduledJobsByNamePrefix(
                jobNamePrefix,
                new Set<String>{ 'DELETED', 'COMPLETE' }
            );

            // Assert
            Assert.areEqual(1, scheduledJobs.size());
            Assert.areEqual(scheduledJobId, scheduledJobs[0].Id);
            Assert.areEqual(true, scheduledJobs[0].CronJobDetail.Name.startsWith(jobNamePrefix));
        } finally {
            System.abortJob(scheduledJobId);
        }
    }

    @IsTest
    static void should_query_scheduled_job_by_id_when_cron_trigger_exists() {
        // Arrange
        String jobName = 'Queues Schedule Repo Id ' + System.now().getTime();
        String scheduledJobId = System.schedule(jobName, '0 0 0 1 1 ? 2099', new NoOpSchedulable());
        QueuesScheduleRepository repository = new QueuesScheduleRepository();

        try {
            // Act
            List<CronTrigger> scheduledJobs = repository.queryScheduledJobById(
                scheduledJobId,
                new Set<String>{ 'DELETED', 'COMPLETE' }
            );

            // Assert
            Assert.areEqual(1, scheduledJobs.size());
            Assert.areEqual(scheduledJobId, scheduledJobs[0].Id);
            Assert.areEqual(jobName, scheduledJobs[0].CronJobDetail.Name);
        } finally {
            System.abortJob(scheduledJobId);
        }
    }

    @IsTest
    static void should_abort_scheduled_job_when_job_id_is_provided() {
        // Arrange
        String jobName = 'Queues Schedule Abort ' + System.now().getTime();
        String scheduledJobId = System.schedule(jobName, '0 0 0 1 1 ? 2099', new NoOpSchedulable());
        QueuesScheduleRepository repository = new QueuesScheduleRepository();

        // Act
        repository.abortJob(scheduledJobId);
        List<CronTrigger> scheduledJobs = [
            SELECT Id, State
            FROM CronTrigger
            WHERE Id = :scheduledJobId
        ];

        // Assert
        Assert.isTrue(scheduledJobs.isEmpty() || scheduledJobs[0].State == 'DELETED');
    }
    // </editor-fold>

    private class NoOpSchedulable implements Schedulable {
        public void execute(final SchedulableContext ctx) {
        }
    }
}
