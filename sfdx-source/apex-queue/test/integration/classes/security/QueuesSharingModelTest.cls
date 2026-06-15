@SuppressWarnings('PMD.ApexUnitTestClassShouldHaveAsserts')
@IsTest
private class QueuesSharingModelTest {
    private static final String OWNER_ALIAS = 'qown';
    private static final String USER_ALIAS = 'quser';
    private static final String UNASSIGNED_ALIAS = 'qnone';
    private static final String READ_ONLY_ALIAS = 'qread';
    private static final String ADMIN_ALIAS = 'qadmn';

    @TestSetup
    static void setupUsers() {
        Profile minimumAccessProfile = [
            SELECT Id, PermissionsAuthorApex, PermissionsModifyAllData, PermissionsViewAllData
            FROM Profile
            WHERE Name = 'Minimum Access - Salesforce'
            LIMIT 1
        ];
        Assert.isFalse(minimumAccessProfile.PermissionsAuthorApex);
        Assert.isFalse(minimumAccessProfile.PermissionsModifyAllData);
        Assert.isFalse(minimumAccessProfile.PermissionsViewAllData);

        insert new List<User>{
            newUser(minimumAccessProfile.Id, OWNER_ALIAS),
            newUser(minimumAccessProfile.Id, USER_ALIAS),
            newUser(minimumAccessProfile.Id, UNASSIGNED_ALIAS),
            newUser(minimumAccessProfile.Id, READ_ONLY_ALIAS),
            newUser(minimumAccessProfile.Id, ADMIN_ALIAS)
        };

        Map<String, User> usersByAlias = new Map<String, User>();
        for (User userRecord : [SELECT Id, Alias FROM User WHERE Alias IN :allAliases()]) {
            usersByAlias.put(userRecord.Alias, userRecord);
        }

        Map<String, PermissionSet> permissionSetsByName = new Map<String, PermissionSet>();
        for (PermissionSet permissionSetRecord : [
            SELECT Id, Name
            FROM PermissionSet
            WHERE Name IN ('QueuesUser', 'QueuesReadOnly', 'QueuesAdmin')
        ]) {
            permissionSetsByName.put(permissionSetRecord.Name, permissionSetRecord);
        }

        insert new List<PermissionSetAssignment>{
            new PermissionSetAssignment(
                AssigneeId = usersByAlias.get(OWNER_ALIAS).Id,
                PermissionSetId = permissionSetsByName.get('QueuesAdmin').Id
            ),
            new PermissionSetAssignment(
                AssigneeId = usersByAlias.get(USER_ALIAS).Id,
                PermissionSetId = permissionSetsByName.get('QueuesUser').Id
            ),
            new PermissionSetAssignment(
                AssigneeId = usersByAlias.get(READ_ONLY_ALIAS).Id,
                PermissionSetId = permissionSetsByName.get('QueuesReadOnly').Id
            ),
            new PermissionSetAssignment(
                AssigneeId = usersByAlias.get(ADMIN_ALIAS).Id,
                PermissionSetId = permissionSetsByName.get('QueuesAdmin').Id
            )
        };
    }

    @IsTest
    static void should_deny_direct_queries_without_object_access() {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(UNASSIGNED_ALIAS);
        String queueName = 'sharing-model-unassigned-' + System.now().getTime();

        System.runAs(owner) {
            seedQueueRecords(queueName);
            assertCounts(1, new SharingProbe().countAll());
        }

        System.runAs(observer) {
            try {
                new SharingProbe().countAll();
                Assert.fail('Expected direct queries to require object access.');
            } catch (SecurityException ex) {
                assertAccessFailure(ex);
            }
        }
    }

    @IsTest
    static void should_keep_other_users_private_rows_hidden_from_user() {
        assertVisibilityFor(USER_ALIAS, 0, 1);
    }

    @IsTest
    static void should_grant_read_only_record_visibility_through_permission_set() {
        assertVisibilityFor(READ_ONLY_ALIAS, 1);
    }

    @IsTest
    static void should_grant_admin_record_visibility_through_permission_set() {
        assertVisibilityFor(ADMIN_ALIAS, 1);
    }

    @IsTest
    static void should_block_facade_job_lookup_without_object_access() {
        assertFacadeJobLookupDeniedFor(UNASSIGNED_ALIAS);
    }

    @IsTest
    static void should_grant_read_only_facade_job_lookup_through_permission_set() {
        assertFacadeJobLookupFor(READ_ONLY_ALIAS, true);
    }

    @IsTest
    static void should_grant_admin_facade_job_lookup_through_permission_set() {
        assertFacadeJobLookupFor(ADMIN_ALIAS, true);
    }

    @IsTest
    static void should_block_facade_stats_without_object_access() {
        assertFacadeStatsDeniedFor(UNASSIGNED_ALIAS);
    }

    @IsTest
    static void should_grant_read_only_facade_stats_through_permission_set() {
        assertFacadeStatsFor(READ_ONLY_ALIAS, 1);
    }

    @IsTest
    static void should_block_facade_scheduler_lookup_without_object_access() {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(UNASSIGNED_ALIAS);
        String queueName = 'sharing-scheduler-hidden-' + System.now().getTime();

        System.runAs(owner) {
            seedSchedulerRecords(queueName);
            Assert.isNotNull(Queues.of(queueName).getJobScheduler('nightly'));
            Assert.areEqual(1, Queues.of(queueName).getJobSchedulersCount());
        }

        System.runAs(observer) {
            try {
                Queues.of(queueName).getJobScheduler('nightly');
                Assert.fail('Expected scheduler lookup to require object access.');
            } catch (QueryException ex) {
                assertAccessFailure(ex);
            }

            try {
                Queues.of(queueName).getJobSchedulersCount();
                Assert.fail('Expected scheduler count to require object access.');
            } catch (QueryException ex) {
                assertAccessFailure(ex);
            }

            try {
                Queues.of(queueName).getJobSchedulers();
                Assert.fail('Expected scheduler list to require object access.');
            } catch (QueryException ex) {
                assertAccessFailure(ex);
            }
        }
    }

    @IsTest
    static void should_block_read_only_facade_job_mutation() {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(READ_ONLY_ALIAS);
        String queueName = 'sharing-job-deny-' + System.now().getTime();
        Id jobId;

        System.runAs(owner) {
            jobId = seedQueueRecords(queueName);
        }

        System.runAs(observer) {
            try {
                Queues.job(jobId).cancel();
                Assert.fail('Expected read-only job mutation to be rejected.');
            } catch (TypeException ex) {
                assertAccessFailure(ex);
            }
        }

        System.runAs(owner) {
            Assert.areEqual(Queues.State.WAITING, Queues.job(jobId).getState());
        }
    }

    @IsTest
    static void should_grant_admin_facade_job_mutation() {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(ADMIN_ALIAS);
        String queueName = 'sharing-job-admin-' + System.now().getTime();
        Id jobId;

        System.runAs(owner) {
            jobId = seedQueueRecords(queueName);
        }

        System.runAs(observer) {
            Queues.job(jobId).cancel();
            Assert.areEqual(Queues.State.CANCELED, Queues.job(jobId).getState());
        }
    }

    @IsTest
    static void should_grant_user_own_job_mutation_across_shared_runtime() {
        User owner = queryUser(OWNER_ALIAS);
        User userRecord = queryUser(USER_ALIAS);
        String queueName = 'sharing-job-user-' + System.now().getTime();
        Id jobId;

        System.runAs(owner) {
            insert QueuesTestSupport.runtime(queueName, false, 0);
        }

        System.runAs(userRecord) {
            Job__c job = QueuesTestSupport.job(
                queueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'job'
            );
            insert job;
            jobId = job.Id;

            Queues.job(jobId).cancel();
            Assert.areEqual(Queues.State.CANCELED, Queues.job(jobId).getState());
        }
    }

    @IsTest
    static void should_grant_user_job_creation_through_public_facade() {
        User userRecord = queryUser(USER_ALIAS);
        String queueName = 'sharing-job-create-' + System.now().getTime();
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

        try {
            useFacadeServiceFor(queueName, workerProvider);

            System.runAs(userRecord) {
                Queues.Job job = Queues.of(queueName)
                    .add(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, null);
                List<Queues.Job> bulkJobs = Queues.of(queueName)
                    .addBulk(
                        new List<Queues.JobData>{
                            new Queues.JobData(
                                QueuesTestSupport.DEFAULT_JOB_NAME,
                                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                                null
                            ),
                            new Queues.JobData(
                                QueuesTestSupport.DEFAULT_JOB_NAME,
                                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                                null
                            )
                        }
                    );

                Assert.areEqual(Queues.State.WAITING, job.getState());
                Assert.areEqual(2, bulkJobs.size());
                Assert.areEqual(3, Queues.of(queueName).getWaitingCount());
            }

            Assert.areEqual(2, workerProvider.getCallCount('scheduleWorkerForJobs'));
        } finally {
            QueuesTestSupport.resetFacadeService();
        }
    }

    @IsTest
    static void should_grant_user_flow_creation_through_public_facade() {
        User userRecord = queryUser(USER_ALIAS);
        String queueName = 'sharing-flow-create-' + System.now().getTime();
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

        try {
            useFacadeServiceFor(queueName, workerProvider);

            System.runAs(userRecord) {
                Queues.JobNode root = Queues.flowProducer()
                    .add(
                        new Queues.FlowJob('parent', queueName, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
                            .child(new Queues.FlowJob('child', queueName, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS))
                    );

                Assert.areEqual(Queues.State.WAITING_CHILDREN, root.getJob().getState());
                Assert.areEqual(1, root.getChildren().size());
                Assert.areEqual(Queues.State.WAITING, root.getChildren()[0].getJob().getState());
            }

            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        } finally {
            QueuesTestSupport.resetFacadeService();
        }
    }

    @IsTest
    static void should_grant_user_scheduler_creation_through_public_facade() {
        User userRecord = queryUser(USER_ALIAS);
        String queueName = 'sharing-scheduler-create-' + System.now().getTime();
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

        try {
            useFacadeServiceFor(queueName, workerProvider);

            System.runAs(userRecord) {
                Queues.Job firstOccurrence = Queues.of(queueName)
                    .upsertJobScheduler(
                        'single-occurrence',
                        new Queues.RepeatOptions().every(5).lim(1),
                        new Queues.JobTemplate(
                            QueuesTestSupport.DEFAULT_JOB_NAME,
                            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                        )
                    );

                Assert.areEqual(Queues.State.DELAYED, firstOccurrence.getState());
                Assert.areEqual(1, Queues.of(queueName).getJobSchedulersCount());
            }

            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        } finally {
            QueuesTestSupport.resetFacadeService();
        }
    }

    @IsTest
    static void should_grant_user_shared_runtime_visibility() {
        User owner = queryUser(OWNER_ALIAS);
        User userRecord = queryUser(USER_ALIAS);
        String queueName = 'sharing-runtime-access-' + System.now().getTime();

        System.runAs(owner) {
            insert QueuesTestSupport.runtime(queueName, false, 0);
        }

        System.runAs(userRecord) {
            Assert.areEqual(1, new QueuesRuntimeRepository().queryRuntimes(queueName).size());
            Assert.areEqual(
                1,
                new QueuesRuntimeRepository(System.AccessLevel.SYSTEM_MODE).queryRuntimes(queueName).size()
            );
        }
    }

    @IsTest
    static void should_keep_inherited_sharing_for_system_mode_job_repository() {
        User owner = queryUser(OWNER_ALIAS);
        User userRecord = queryUser(USER_ALIAS);
        String queueName = 'sharing-job-access-' + System.now().getTime();
        Id jobId;

        System.runAs(owner) {
            jobId = seedQueueRecords(queueName);
        }

        System.runAs(userRecord) {
            Assert.areEqual(0, new QueuesJobRepository().queryJobsByIds(new Set<Id>{ jobId }).size());
            Assert.areEqual(
                0,
                new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE).queryJobsByIds(new Set<Id>{ jobId }).size()
            );
        }
    }

    @IsTest
    static void should_block_user_cross_owner_job_mutation() {
        User owner = queryUser(OWNER_ALIAS);
        User userRecord = queryUser(USER_ALIAS);
        String queueName = 'sharing-job-user-deny-' + System.now().getTime();
        Id jobId;

        System.runAs(owner) {
            jobId = seedQueueRecords(queueName);
        }

        System.runAs(userRecord) {
            try {
                Queues.job(jobId).cancel();
                Assert.fail('Expected user job mutation to remain ownership-scoped.');
            } catch (Queues.JobNotFoundException ex) {
                assertAccessFailure(ex);
            }
        }
    }

    @IsTest
    static void should_block_user_worker_reordering() {
        User userRecord = queryUser(USER_ALIAS);

        System.runAs(userRecord) {
            try {
                Queues.of('sharing-user-worker-deny').moveWorkerToFront();
                Assert.fail('Expected worker reordering to require QueuesManageWorkers.');
            } catch (Queues.QueueException ex) {
                Assert.areEqual('QueuesManageWorkers permission is required.', ex.getMessage());
            }
        }
    }

    @IsTest
    static void should_block_read_only_queue_cancel() {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(READ_ONLY_ALIAS);
        String queueName = 'sharing-queue-deny-' + System.now().getTime();
        Id jobId;

        useFacadeServiceFor(queueName);
        System.runAs(owner) {
            jobId = seedQueueRecords(queueName);
        }

        System.runAs(observer) {
            try {
                Queues.of(queueName).cancel();
                Assert.fail('Expected read-only queue cancel to be rejected.');
            } catch (TypeException ex) {
                assertAccessFailure(ex);
            }
        }

        System.runAs(owner) {
            Assert.areEqual(Queues.State.WAITING, Queues.job(jobId).getState());
        }
    }

    @IsTest
    static void should_grant_admin_queue_cancel() {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(ADMIN_ALIAS);
        String queueName = 'sharing-queue-admin-' + System.now().getTime();
        Id jobId;

        useFacadeServiceFor(queueName);
        System.runAs(owner) {
            jobId = seedQueueRecords(queueName);
        }

        System.runAs(observer) {
            Assert.areEqual(1, Queues.of(queueName).cancel());
            Assert.areEqual(Queues.State.CANCELED, Queues.job(jobId).getState());
        }
    }

    @IsTest
    static void should_block_read_only_scheduler_cancel() {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(READ_ONLY_ALIAS);
        String queueName = 'sharing-scheduler-deny-' + System.now().getTime();
        SchedulerSeed seed;

        System.runAs(owner) {
            seed = seedSchedulerRecords(queueName);
        }

        System.runAs(observer) {
            try {
                Queues.of(queueName).cancelJobScheduler('nightly');
                Assert.fail('Expected read-only scheduler cancel to be rejected.');
            } catch (TypeException ex) {
                assertAccessFailure(ex);
            }
        }

        System.runAs(owner) {
            Assert.areEqual(Queues.State.WAITING, Queues.job(seed.jobId).getState());
            Assert.isTrue(queryScheduler(seed.schedulerId).IsActive__c);
        }
    }

    @IsTest
    static void should_grant_admin_scheduler_cancel() {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(ADMIN_ALIAS);
        String queueName = 'sharing-scheduler-admin-' + System.now().getTime();
        SchedulerSeed seed;

        System.runAs(owner) {
            seed = seedSchedulerRecords(queueName);
        }

        System.runAs(observer) {
            Assert.isTrue(Queues.of(queueName).cancelJobScheduler('nightly'));
            Assert.areEqual(Queues.State.CANCELED, Queues.job(seed.jobId).getState());
            Assert.areEqual(false, queryScheduler(seed.schedulerId).IsActive__c);
        }
    }

    private static void assertVisibilityFor(final String aliasValue, final Integer expectedCount) {
        assertVisibilityFor(aliasValue, expectedCount, expectedCount);
    }

    private static void assertVisibilityFor(
        final String aliasValue,
        final Integer expectedCount,
        final Integer expectedRuntimeCount
    ) {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(aliasValue);
        String queueName = 'sharing-model-' + aliasValue + '-' + System.now().getTime();

        System.runAs(owner) {
            seedQueueRecords(queueName);
            assertCounts(1, new SharingProbe().countAll());
        }

        System.runAs(observer) {
            assertCounts(expectedCount, expectedRuntimeCount, new SharingProbe().countAll());
        }
    }

    private static void assertFacadeJobLookupFor(final String aliasValue, final Boolean shouldSeeJob) {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(aliasValue);
        String queueName = 'sharing-facade-' + aliasValue + '-' + System.now().getTime();
        Id jobId;

        System.runAs(owner) {
            jobId = seedQueueRecords(queueName);
            Assert.areEqual(Queues.State.WAITING, Queues.job(jobId).getState());
        }

        System.runAs(observer) {
            if (shouldSeeJob == true) {
                Assert.areEqual(Queues.State.WAITING, Queues.job(jobId).getState());
                return;
            }

            Assert.fail('Use assertFacadeJobLookupDeniedFor for users without object access.');
        }
    }

    private static void assertFacadeJobLookupDeniedFor(final String aliasValue) {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(aliasValue);
        String queueName = 'sharing-facade-' + aliasValue + '-' + System.now().getTime();
        Id jobId;

        System.runAs(owner) {
            jobId = seedQueueRecords(queueName);
            Assert.areEqual(Queues.State.WAITING, Queues.job(jobId).getState());
        }

        System.runAs(observer) {
            try {
                Queues.job(jobId).getState();
                Assert.fail('Expected job lookup to require object access.');
            } catch (QueryException ex) {
                assertAccessFailure(ex);
            }
        }
    }

    private static void assertFacadeStatsFor(final String aliasValue, final Integer expectedTotal) {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(aliasValue);
        String queueName = 'sharing-stats-' + aliasValue + '-' + System.now().getTime();

        useFacadeServiceFor(queueName);
        System.runAs(owner) {
            seedQueueRecords(queueName);
            Assert.areEqual(1, Queues.of(queueName).getStats().total);
        }

        System.runAs(observer) {
            Assert.areEqual(expectedTotal, Queues.of(queueName).getStats().total);
        }
    }

    private static void assertFacadeStatsDeniedFor(final String aliasValue) {
        User owner = queryUser(OWNER_ALIAS);
        User observer = queryUser(aliasValue);
        String queueName = 'sharing-stats-' + aliasValue + '-' + System.now().getTime();

        useFacadeServiceFor(queueName);
        System.runAs(owner) {
            seedQueueRecords(queueName);
            Assert.areEqual(1, Queues.of(queueName).getStats().total);
        }

        System.runAs(observer) {
            try {
                Queues.of(queueName).getStats();
                Assert.fail('Expected stats lookup to require object access.');
            } catch (QueryException ex) {
                assertAccessFailure(ex);
            }
        }
    }

    private static void assertAccessFailure(final Exception ex) {
        Assert.isTrue(String.isNotBlank(ex.getMessage()), 'Expected an access failure message.');
    }

    private static User newUser(final Id profileId, final String aliasValue) {
        String uniqueSuffix =
            String.valueOf(System.now().getTime()) + String.valueOf(Crypto.getRandomInteger()).replace('-', '0');
        String uniqueName = 'apex.queue.' + aliasValue + '.' + uniqueSuffix;
        return new User(
            Alias = aliasValue,
            CommunityNickname = ('aq' + aliasValue + uniqueSuffix).left(40),
            Email = uniqueName + '@example.com',
            EmailEncodingKey = 'UTF-8',
            LanguageLocaleKey = 'en_US',
            LastName = aliasValue,
            LocaleSidKey = 'en_US',
            ProfileId = profileId,
            TimeZoneSidKey = 'America/Los_Angeles',
            Username = uniqueName + '@example.com'
        );
    }

    private static Set<String> allAliases() {
        return new Set<String>{ OWNER_ALIAS, USER_ALIAS, UNASSIGNED_ALIAS, READ_ONLY_ALIAS, ADMIN_ALIAS };
    }

    private static User queryUser(final String aliasValue) {
        return [SELECT Id FROM User WHERE Alias = :aliasValue LIMIT 1];
    }

    private static void useFacadeServiceFor(final String queueName) {
        useFacadeServiceFor(queueName, null);
    }

    private static void useFacadeServiceFor(
        final String queueName,
        final QueuesTestSupport.RecordingStubProvider workerProvider
    ) {
        Queues.queueService = QueuesTestSupport.newFacadeServiceWithQueueMetadata(queueName, workerProvider);
    }

    private static Id seedQueueRecords(final String queueName) {
        Job__c job = QueuesTestSupport.job(
            queueName,
            QueuesTestSupport.DEFAULT_JOB_NAME,
            Queues.State.WAITING.name(),
            'job'
        );
        insert job;
        insert QueuesTestSupport.jobRun(job.Id, 1, Queues.RunStatus.ACTIVE.name());
        insert QueuesTestSupport.jobScheduler(queueName, 'nightly');
        insert QueuesTestSupport.runtime(queueName, false, 0);
        insert new QueueError__c(
            QueueName__c = queueName,
            Component__c = 'SharingModelTest',
            Job__c = job.Id,
            OccurredAt__c = System.now(),
            Message__c = 'Queue visibility test error.'
        );
        insert new QueueableDispatch__c(
            QueueName__c = queueName,
            State__c = Queues.QueueableDispatchState.WAITING.name(),
            Mode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
            RequestedCount__c = 1,
            DispatchedCount__c = 0,
            DispatchDepth__c = 0
        );
        return job.Id;
    }

    private static SchedulerSeed seedSchedulerRecords(final String queueName) {
        Id jobId = seedQueueRecords(queueName);
        JobScheduler__c scheduler = [
            SELECT Id
            FROM JobScheduler__c
            WHERE QueueName__c = :queueName AND Name = 'nightly'
            LIMIT 1
        ];
        update new Job__c(Id = jobId, JobScheduler__c = scheduler.Id);
        return new SchedulerSeed(jobId, scheduler.Id);
    }

    private static JobScheduler__c queryScheduler(final Id schedulerId) {
        return [SELECT Id, IsActive__c FROM JobScheduler__c WHERE Id = :schedulerId LIMIT 1];
    }

    private static void assertCounts(final Integer expectedCount, final VisibilityCounts actualCounts) {
        assertCounts(expectedCount, expectedCount, actualCounts);
    }

    private static void assertCounts(
        final Integer expectedCount,
        final Integer expectedRuntimeCount,
        final VisibilityCounts actualCounts
    ) {
        Assert.areEqual(expectedCount, actualCounts.jobs, 'Job__c visibility mismatch.');
        Assert.areEqual(expectedCount, actualCounts.jobRuns, 'JobRun__c visibility mismatch.');
        Assert.areEqual(expectedCount, actualCounts.jobSchedulers, 'JobScheduler__c visibility mismatch.');
        Assert.areEqual(expectedCount, actualCounts.queueErrors, 'QueueError__c visibility mismatch.');
        Assert.areEqual(expectedRuntimeCount, actualCounts.queueRuntimes, 'QueueRuntime__c visibility mismatch.');
        Assert.areEqual(expectedCount, actualCounts.queueableDispatches, 'QueueableDispatch__c visibility mismatch.');
    }

    private class VisibilityCounts {
        private Integer jobs;
        private Integer jobRuns;
        private Integer jobSchedulers;
        private Integer queueErrors;
        private Integer queueRuntimes;
        private Integer queueableDispatches;
    }

    private class SchedulerSeed {
        private final Id jobId;
        private final Id schedulerId;

        private SchedulerSeed(final Id jobId, final Id schedulerId) {
            this.jobId = jobId;
            this.schedulerId = schedulerId;
        }
    }

    @SuppressWarnings('PMD.AvoidNonRestrictiveQueries')
    private with sharing class SharingProbe {
        private VisibilityCounts countAll() {
            VisibilityCounts counts = new VisibilityCounts();
            counts.jobs = [SELECT COUNT() FROM Job__c];
            counts.jobRuns = [SELECT COUNT() FROM JobRun__c];
            counts.jobSchedulers = [SELECT COUNT() FROM JobScheduler__c];
            counts.queueErrors = [SELECT COUNT() FROM QueueError__c];
            counts.queueRuntimes = [SELECT COUNT() FROM QueueRuntime__c];
            counts.queueableDispatches = [SELECT COUNT() FROM QueueableDispatch__c];
            return counts;
        }
    }
}
