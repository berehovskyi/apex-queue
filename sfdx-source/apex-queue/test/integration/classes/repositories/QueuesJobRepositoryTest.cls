@IsTest(IsParallel=false)
private class QueuesJobRepositoryTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_query_and_mutate_jobs_when_records_exist() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'job-repo';
            Datetime currentNow = System.now();
            QueuesJobRepository repository = new QueuesJobRepository();

            Job__c waitingJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.WAITING.name(), 'job-1');
            waitingJob.AvailableAt__c = currentNow.addMinutes(-1);
            waitingJob.Priority__c = 1;

            Job__c delayedJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.DELAYED.name(), 'job-2');
            delayedJob.AvailableAt__c = currentNow.addMinutes(5);
            delayedJob.Priority__c = 2;

            Job__c activeJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.ACTIVE.name(), 'job-3');
            activeJob.LeaseToken__c = 'lease-token';
            activeJob.LeaseExpiresAt__c = currentNow.addMinutes(-2);

            Job__c earlierExpiredActiveJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.ACTIVE.name(),
                'job-3b'
            );
            earlierExpiredActiveJob.LeaseToken__c = 'earlier-lease-token';
            earlierExpiredActiveJob.LeaseExpiresAt__c = currentNow.addMinutes(-3);

            Job__c pausedJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.PAUSED.name(), 'job-4');

            repository.insertJobs(
                new List<Job__c>{ waitingJob, delayedJob, activeJob, earlierExpiredActiveJob, pausedJob }
            );
            Test.setCreatedDate(waitingJob.Id, currentNow.addMinutes(-5));
            Test.setCreatedDate(delayedJob.Id, currentNow.addMinutes(-4));
            Test.setCreatedDate(activeJob.Id, currentNow.addMinutes(-3));
            Test.setCreatedDate(earlierExpiredActiveJob.Id, currentNow.addMinutes(-2));
            Test.setCreatedDate(pausedJob.Id, currentNow.addMinutes(-1));

            // Act
            List<Job__c> dueJobs = repository.queryDueJobs(
                queueName,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED }),
                currentNow,
                10
            );
            List<Job__c> jobsByExternalId = repository.queryJobsByExternalIds(
                queueName,
                new Set<String>{ 'JOB-1', 'job-2' }
            );
            List<Job__c> emptyJobsByExternalId = repository.queryJobsByExternalIds(queueName, new Set<String>());
            List<Job__c> jobsById = repository.queryJobsByIds(new Set<Id>{ waitingJob.Id, delayedJob.Id });
            List<Job__c> emptyJobsByParentId = repository.queryJobsByParentIds(new Set<Id>());
            List<Job__c> lockedJobsById = repository.queryJobsByIdsForUpdate(
                new Set<Id>{ waitingJob.Id, delayedJob.Id },
                null
            );
            List<Job__c> lockedWaitingJobsById = repository.queryJobsByIdsForUpdate(
                new Set<Id>{ waitingJob.Id, delayedJob.Id },
                Queues.State.WAITING.name()
            );
            List<AggregateResult> emptyChildStateCounts = repository.queryChildStateCounts(null);
            List<AggregateResult> nextPumpAtRows = repository.queryNextPumpAt(
                queueName,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED }),
                currentNow
            );
            Map<String, Integer> countsByState = repository.queryQueueStateCounts(queueName);
            List<Job__c> expiredActiveJobs = repository.queryExpiredActiveJobsForUpdate(
                queueName,
                Queues.State.ACTIVE.name(),
                currentNow,
                10
            );
            List<Job__c> lockedExpiredActiveJobs = repository.queryExpiredActiveJobsForUpdate(
                queueName,
                Queues.State.ACTIVE.name(),
                currentNow,
                10
            );
            List<Job__c> limitedLockedExpiredActiveJobs = repository.queryExpiredActiveJobsForUpdate(
                queueName,
                Queues.State.ACTIVE.name(),
                currentNow,
                1
            );
            List<Job__c> drainableJobs = repository.queryDrainableJobsForUpdate(
                queueName,
                QueuesSupport.toStateValues(
                    new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED, Queues.State.PAUSED }
                ),
                10
            );
            List<Job__c> lockedDrainableJobs = repository.queryDrainableJobsForUpdate(
                queueName,
                QueuesSupport.toStateValues(
                    new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED, Queues.State.PAUSED }
                ),
                10
            );
            List<Job__c> limitedLockedDrainableJobs = repository.queryDrainableJobsForUpdate(
                queueName,
                QueuesSupport.toStateValues(
                    new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED, Queues.State.PAUSED }
                ),
                2
            );
            List<Database.SaveResult> duplicateInsertResults = repository.insertJobsAllowingPartialSuccess(
                new List<Job__c>{
                    QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.WAITING.name(), 'job-1'),
                    QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.WAITING.name(), 'job-5')
                }
            );

            waitingJob.Priority__c = 9;
            repository.updateJobs(new List<Job__c>{ waitingJob });
            repository.deleteJobs(new List<Job__c>{ pausedJob });

            Job__c updatedWaitingJob = [SELECT Priority__c FROM Job__c WHERE Id = :waitingJob.Id];
            Integer remainingJobs = [SELECT COUNT() FROM Job__c WHERE QueueName__c = :queueName];
            List<Id> expectedLockedJobIds = new List<Id>{ waitingJob.Id, delayedJob.Id };
            expectedLockedJobIds.sort();

            // Assert
            Assert.areEqual(1, dueJobs.size());
            Assert.areEqual(waitingJob.Id, dueJobs[0].Id);
            Assert.areEqual(2, jobsByExternalId.size());
            Assert.areEqual(0, emptyJobsByExternalId.size());
            Assert.areEqual(2, jobsById.size());
            Assert.areEqual(0, emptyJobsByParentId.size());
            Assert.areEqual(2, lockedJobsById.size());
            Assert.areEqual(1, lockedWaitingJobsById.size());
            Assert.areEqual(waitingJob.Id, lockedWaitingJobsById[0].Id);
            Assert.areEqual(0, emptyChildStateCounts.size());
            Assert.areEqual(expectedLockedJobIds[0], lockedJobsById[0].Id);
            Assert.areEqual(expectedLockedJobIds[1], lockedJobsById[1].Id);
            Assert.areEqual(1, nextPumpAtRows.size());
            Assert.areEqual(delayedJob.AvailableAt__c, (Datetime) nextPumpAtRows[0].get('nextPumpAt'));
            Assert.areEqual(4, countsByState.size());
            Assert.areEqual(2, expiredActiveJobs.size());
            Assert.areEqual(2, lockedExpiredActiveJobs.size());
            Assert.areEqual(1, limitedLockedExpiredActiveJobs.size());
            Assert.areEqual(earlierExpiredActiveJob.Id, expiredActiveJobs[0].Id);
            Assert.areEqual(activeJob.Id, expiredActiveJobs[1].Id);
            Assert.areEqual(earlierExpiredActiveJob.Id, lockedExpiredActiveJobs[0].Id);
            Assert.areEqual(activeJob.Id, lockedExpiredActiveJobs[1].Id);
            Assert.areEqual(earlierExpiredActiveJob.Id, limitedLockedExpiredActiveJobs[0].Id);
            Assert.areEqual(3, drainableJobs.size());
            Assert.areEqual(3, lockedDrainableJobs.size());
            Assert.areEqual(2, limitedLockedDrainableJobs.size());
            Assert.areEqual(waitingJob.Id, lockedDrainableJobs[0].Id);
            Assert.areEqual(delayedJob.Id, lockedDrainableJobs[1].Id);
            Assert.areEqual(pausedJob.Id, lockedDrainableJobs[2].Id);
            Assert.areEqual(waitingJob.Id, limitedLockedDrainableJobs[0].Id);
            Assert.areEqual(delayedJob.Id, limitedLockedDrainableJobs[1].Id);
            Assert.areEqual(2, duplicateInsertResults.size());
            Assert.areEqual(false, duplicateInsertResults[0].isSuccess());
            Assert.areEqual(true, duplicateInsertResults[1].isSuccess());
            Assert.areEqual(1, countsByState.get(Queues.State.WAITING.name()));
            Assert.areEqual(1, countsByState.get(Queues.State.DELAYED.name()));
            Assert.areEqual(2, countsByState.get(Queues.State.ACTIVE.name()));
            Assert.areEqual(1, countsByState.get(Queues.State.PAUSED.name()));
            Assert.areEqual(9, QueuesSupport.toInteger(updatedWaitingJob.Priority__c, 0));
            Assert.areEqual(5, remainingJobs);
        }
    }

    @IsTest
    static void should_query_drainable_jobs_across_execution_modes() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'job-repo-drain-modes';
            Datetime currentNow = System.now();
            QueuesJobRepository repository = new QueuesJobRepository();

            Job__c workerJob = QueuesTestSupport.job(queueName, 'worker', Queues.State.WAITING.name(), 'worker');
            Job__c queueableJob = QueuesTestSupport.job(
                queueName,
                'queueable',
                Queues.State.DELAYED.name(),
                'queueable'
            );
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            Job__c invocableJob = QueuesTestSupport.job(
                queueName,
                'invocable',
                Queues.State.PAUSED.name(),
                'invocable'
            );
            invocableJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            Job__c activeQueueableJob = QueuesTestSupport.job(
                queueName,
                'active-queueable',
                Queues.State.ACTIVE.name(),
                'active-queueable'
            );
            activeQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            repository.insertJobs(new List<Job__c>{ workerJob, queueableJob, invocableJob, activeQueueableJob });
            Test.setCreatedDate(workerJob.Id, currentNow.addMinutes(-3));
            Test.setCreatedDate(queueableJob.Id, currentNow.addMinutes(-2));
            Test.setCreatedDate(invocableJob.Id, currentNow.addMinutes(-1));

            // Act
            List<Job__c> drainableJobs = repository.queryDrainableJobsForUpdate(
                queueName,
                QueuesSupport.toStateValues(
                    new Set<Queues.State>{
                        Queues.State.WAITING,
                        Queues.State.DELAYED,
                        Queues.State.PAUSED,
                        Queues.State.WAITING_CHILDREN
                    }
                ),
                10
            );

            // Assert
            Assert.areEqual(3, drainableJobs.size());
            Assert.areEqual(workerJob.Id, drainableJobs[0].Id);
            Assert.areEqual(queueableJob.Id, drainableJobs[1].Id);
            Assert.areEqual(invocableJob.Id, drainableJobs[2].Id);
        }
    }

    @IsTest
    static void should_query_failed_removed_dependencies_for_update() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesJobRepository repository = new QueuesJobRepository();
            Job__c parent = QueuesTestSupport.job(
                'job-repo-dependency',
                'parent',
                Queues.State.WAITING_CHILDREN.name(),
                'parent'
            );
            repository.insertJobs(new List<Job__c>{ parent });
            Job__c removedDependency = QueuesTestSupport.job(
                'job-repo-dependency',
                'removed',
                Queues.State.FAILED.name(),
                'removed'
            );
            removedDependency.Parent__c = parent.Id;
            removedDependency.DependencyFailurePolicy__c = Queues.DependencyFailurePolicy.REMOVE_DEPENDENCY.name();
            Job__c retainedDependency = QueuesTestSupport.job(
                'job-repo-dependency',
                'retained',
                Queues.State.FAILED.name(),
                'retained'
            );
            retainedDependency.Parent__c = parent.Id;
            retainedDependency.DependencyFailurePolicy__c = Queues.DependencyFailurePolicy.IGNORE_DEPENDENCY.name();
            repository.insertJobs(new List<Job__c>{ removedDependency, retainedDependency });
            Datetime staleBefore = System.now().addMinutes(1);

            // Act
            List<Job__c> dependencies = repository.queryFailedRemovedDependenciesForUpdate(new Set<Id>{ parent.Id }, 1);
            List<Job__c> emptyDependencies = repository.queryFailedRemovedDependenciesForUpdate(null, 1);
            List<Job__c> limitedDependencies = repository.queryFailedRemovedDependenciesForUpdate(
                new Set<Id>{ parent.Id },
                0
            );
            List<AggregateResult> queueNames = repository.queryRemovedDependencyMaintenanceJobQueueNames();
            Boolean hasStaleRemovedDependency = repository.hasStaleRemovedDependency(parent.QueueName__c, staleBefore);
            List<Job__c> staleDependencies = repository.queryStaleRemovedDependenciesForUpdate(
                parent.QueueName__c,
                staleBefore,
                1
            );

            // Assert
            Assert.areEqual(1, dependencies.size());
            Assert.areEqual(removedDependency.Id, dependencies[0].Id);
            Assert.areEqual(0, emptyDependencies.size());
            Assert.areEqual(0, limitedDependencies.size());
            Assert.areEqual(parent.QueueName__c, (String) queueNames[0].get('queueName'));
            Assert.areEqual(true, hasStaleRemovedDependency);
            Assert.areEqual(removedDependency.Id, staleDependencies[0].Id);
        }
    }

    @IsTest
    static void should_query_stale_waiting_children_jobs_for_maintenance() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'job-repo-waiting-children';
            QueuesJobRepository repository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
            Job__c waitingParent = QueuesTestSupport.job(
                queueName,
                'waiting-parent',
                Queues.State.WAITING_CHILDREN.name(),
                'waiting-parent'
            );
            Job__c waitingJob = QueuesTestSupport.job(
                queueName,
                'waiting-job',
                Queues.State.WAITING.name(),
                'waiting-job'
            );
            Job__c checkedParent = QueuesTestSupport.job(
                queueName,
                'checked-parent',
                Queues.State.WAITING_CHILDREN.name(),
                'checked-parent'
            );
            Datetime staleBefore = System.now().addMinutes(1);
            checkedParent.DependencyRepairCheckedAt__c = staleBefore.addMinutes(1);
            repository.insertJobs(new List<Job__c>{ waitingParent, waitingJob, checkedParent });

            // Act
            List<AggregateResult> queueNames = repository.queryWaitingChildrenMaintenanceJobQueueNames();
            Boolean hasStaleWaitingChildrenJob = repository.hasStaleWaitingChildrenJob(queueName, staleBefore);
            List<Job__c> staleWaitingChildrenJobs = repository.queryStaleWaitingChildrenJobs(queueName, staleBefore, 1);

            // Assert
            Assert.areEqual(1, queueNames.size());
            Assert.areEqual(queueName, (String) queueNames[0].get('queueName'));
            Assert.areEqual(true, hasStaleWaitingChildrenJob);
            Assert.areEqual(1, staleWaitingChildrenJobs.size());
            Assert.areEqual(waitingParent.Id, staleWaitingChildrenJobs[0].Id);
        }
    }

    @IsTest
    static void should_query_due_jobs_without_name_partition_and_return_job_locators_when_requested() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'job-repo-filtered';
            Datetime currentNow = System.now();
            QueuesJobRepository repository = new QueuesJobRepository();

            Job__c matchingJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.WAITING.name(), 'job-a');
            matchingJob.AvailableAt__c = currentNow.addMinutes(-1);
            Job__c otherJob = QueuesTestSupport.job(queueName, 'sync-payment', Queues.State.WAITING.name(), 'job-b');
            otherJob.AvailableAt__c = currentNow.addMinutes(-1);

            repository.insertJobs(new List<Job__c>{ matchingJob, otherJob });

            // Act
            List<Job__c> dueJobs = repository.queryDueJobs(
                queueName,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING }),
                currentNow,
                null
            );
            Database.QueryLocator claimedLocator = repository.queryClaimedJobLocator(new Set<Id>{ matchingJob.Id });
            Database.QueryLocator emptyLocator = repository.queryClaimedJobLocator(new Set<Id>());

            // Assert
            Assert.areEqual(2, dueJobs.size());
            Assert.areEqual(matchingJob.Id, dueJobs[0].Id);
            Assert.areEqual(otherJob.Id, dueJobs[1].Id);
            Assert.areNotEqual(null, claimedLocator);
            Assert.areNotEqual(null, emptyLocator);
        }
    }

    @IsTest
    static void should_query_scheduler_jobs_for_update_and_check_presence_by_scheduler_id() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'job-repo-scheduler';
            QueuesJobRepository repository = new QueuesJobRepository();
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'scheduler-jobs');
            insert scheduler;

            Job__c waitingJob = QueuesTestSupport.job(queueName, 'waiting', Queues.State.WAITING.name(), 'waiting');
            waitingJob.JobScheduler__c = scheduler.Id;
            Job__c completedJob = QueuesTestSupport.job(queueName, 'completed', Queues.State.COMPLETED.name(), 'done');
            completedJob.JobScheduler__c = scheduler.Id;
            Job__c otherJob = QueuesTestSupport.job(queueName, 'other', Queues.State.WAITING.name(), 'other');
            repository.insertJobs(new List<Job__c>{ waitingJob, completedJob, otherJob });

            // Act
            Boolean hasWaitingSchedulerJobs = repository.hasJobsBySchedulerId(
                scheduler.Id,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING })
            );
            Boolean hasPausedSchedulerJobs = repository.hasJobsBySchedulerId(
                scheduler.Id,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.PAUSED })
            );
            List<Job__c> schedulerJobs = repository.queryJobsBySchedulerIdForUpdate(scheduler.Id, null, 10);
            List<Job__c> waitingSchedulerJobs = repository.queryJobsBySchedulerIdForUpdate(
                scheduler.Id,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING }),
                10
            );
            List<Job__c> pausedSchedulerJobs = repository.queryJobsBySchedulerIdForUpdate(
                scheduler.Id,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.PAUSED }),
                10
            );

            // Assert
            Assert.areEqual(true, hasWaitingSchedulerJobs);
            Assert.areEqual(false, hasPausedSchedulerJobs);
            Assert.areEqual(2, schedulerJobs.size());
            Assert.areEqual(1, waitingSchedulerJobs.size());
            Assert.areEqual(0, pausedSchedulerJobs.size());
            Assert.areEqual(scheduler.Id, schedulerJobs[0].JobScheduler__c);
            Assert.areEqual(scheduler.Id, schedulerJobs[1].JobScheduler__c);
            Assert.areEqual(waitingJob.Id, waitingSchedulerJobs[0].Id);
        }
    }

    @IsTest
    static void should_keep_queueable_jobs_out_of_worker_queries() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'job-repo-worker-mode';
            Datetime currentNow = System.now();
            QueuesJobRepository repository = new QueuesJobRepository();

            Job__c workerWaitingJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.WAITING.name(),
                'job-worker-waiting'
            );
            workerWaitingJob.AvailableAt__c = currentNow.addMinutes(-1);

            Job__c queueWaitingJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.WAITING.name(),
                'job-queue-waiting'
            );
            queueWaitingJob.AvailableAt__c = currentNow.addMinutes(-2);
            queueWaitingJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();

            Job__c workerDelayedJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.DELAYED.name(),
                'job-worker-delayed'
            );
            workerDelayedJob.AvailableAt__c = currentNow.addMinutes(5);

            Job__c queueDelayedJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.DELAYED.name(),
                'job-queue-delayed'
            );
            queueDelayedJob.AvailableAt__c = currentNow.addMinutes(1);
            queueDelayedJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();

            Job__c workerActiveJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.ACTIVE.name(),
                'job-worker-active'
            );
            workerActiveJob.LeaseExpiresAt__c = currentNow.addMinutes(-1);

            Job__c queueActiveJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.ACTIVE.name(),
                'job-queue-active'
            );
            queueActiveJob.AvailableAt__c = currentNow.addMinutes(-10);
            queueActiveJob.LeaseExpiresAt__c = currentNow.addMinutes(-2);
            queueActiveJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();

            Job__c queueDelayedActiveJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.ACTIVE.name(),
                'job-queue-active-delayed'
            );
            queueDelayedActiveJob.AvailableAt__c = currentNow.addMinutes(5);
            queueDelayedActiveJob.LeaseExpiresAt__c = currentNow.addMinutes(-2);
            queueDelayedActiveJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();

            repository.insertJobs(
                new List<Job__c>{
                    workerWaitingJob,
                    queueWaitingJob,
                    workerDelayedJob,
                    queueDelayedJob,
                    workerActiveJob,
                    queueActiveJob,
                    queueDelayedActiveJob
                }
            );

            // Act
            List<Job__c> dueJobs = repository.queryDueJobs(
                queueName,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED }),
                currentNow,
                10
            );
            List<AggregateResult> nextPumpAtRows = repository.queryNextPumpAt(
                queueName,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED }),
                currentNow
            );
            List<Job__c> expiredActiveJobs = repository.queryExpiredActiveJobsForUpdate(
                queueName,
                Queues.State.ACTIVE.name(),
                currentNow,
                10
            );
            List<Job__c> lockedExpiredActiveJobs = repository.queryExpiredActiveJobsForUpdate(
                queueName,
                Queues.State.ACTIVE.name(),
                currentNow,
                10
            );
            List<Job__c> lockedExpiredQueueableJobs = repository.queryExpiredActiveQueueableJobsForUpdate(
                queueName,
                Queues.State.ACTIVE.name(),
                currentNow.addMinutes(1),
                10
            );
            List<Job__c> limitedLockedExpiredQueueableJobs = repository.queryExpiredActiveQueueableJobsForUpdate(
                queueName,
                Queues.State.ACTIVE.name(),
                currentNow.addMinutes(1),
                1
            );
            List<AggregateResult> queueableMaintenanceQueueNames = repository.queryQueueableMaintenanceJobQueueNames(
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.ACTIVE })
            );
            Boolean hasExpiredQueueableJob = repository.hasExpiredActiveQueueableJob(
                queueName,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.ACTIVE }),
                currentNow.addMinutes(1)
            );
            Boolean hasFutureExpiredQueueableJob = repository.hasExpiredActiveQueueableJob(
                queueName,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.ACTIVE }),
                currentNow.addMinutes(-1)
            );
            Boolean hasInvalidExpiredQueueableJob = repository.hasExpiredActiveQueueableJob(
                null,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.ACTIVE }),
                currentNow
            );

            // Assert
            Assert.areEqual(1, dueJobs.size());
            Assert.areEqual(workerWaitingJob.Id, dueJobs[0].Id);
            Assert.areEqual(workerDelayedJob.AvailableAt__c, (Datetime) nextPumpAtRows[0].get('nextPumpAt'));
            Assert.areEqual(1, expiredActiveJobs.size());
            Assert.areEqual(workerActiveJob.Id, expiredActiveJobs[0].Id);
            Assert.areEqual(1, lockedExpiredActiveJobs.size());
            Assert.areEqual(workerActiveJob.Id, lockedExpiredActiveJobs[0].Id);
            Assert.areEqual(1, lockedExpiredQueueableJobs.size());
            Assert.areEqual(queueActiveJob.Id, lockedExpiredQueueableJobs[0].Id);
            Assert.areEqual(1, limitedLockedExpiredQueueableJobs.size());
            Assert.areEqual(queueActiveJob.Id, limitedLockedExpiredQueueableJobs[0].Id);
            Assert.areEqual(1, queueableMaintenanceQueueNames.size());
            Assert.areEqual(queueName, (String) queueableMaintenanceQueueNames[0].get('queueName'));
            Assert.areEqual(true, hasExpiredQueueableJob);
            Assert.areEqual(false, hasFutureExpiredQueueableJob);
            Assert.areEqual(false, hasInvalidExpiredQueueableJob);
        }
    }

    @IsTest
    static void should_query_stale_initiated_queueable_jobs_for_recovery() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'job-repo-queueable-initiated';
            Datetime currentNow = System.now();
            QueuesJobRepository repository = new QueuesJobRepository();
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = queueName,
                State__c = Queues.QueueableDispatchState.COMPLETED.name(),
                RequestedCount__c = 4,
                DispatchedCount__c = 4,
                DispatchDepth__c = 0
            );
            insert dispatch;

            Job__c staleJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.WAITING.name(), 'stale');
            staleJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            staleJob.QueueableDispatch__c = dispatch.Id;
            staleJob.QueueableDispatchInitiatedAt__c = currentNow.addMinutes(-10);
            staleJob.AvailableAt__c = currentNow.addMinutes(-1);

            Job__c directStaleJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.WAITING.name(),
                'direct-stale'
            );
            directStaleJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            directStaleJob.QueueableDispatchInitiatedAt__c = currentNow.addMinutes(-9);
            directStaleJob.AvailableAt__c = currentNow.addMinutes(-1);

            Job__c futureJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.DELAYED.name(), 'future');
            futureJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            futureJob.QueueableDispatch__c = dispatch.Id;
            futureJob.QueueableDispatchInitiatedAt__c = currentNow.addMinutes(-10);
            futureJob.AvailableAt__c = currentNow.addMinutes(5);

            Job__c freshJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.WAITING.name(), 'fresh');
            freshJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            freshJob.QueueableDispatch__c = dispatch.Id;
            freshJob.QueueableDispatchInitiatedAt__c = currentNow.addMinutes(-1);
            freshJob.AvailableAt__c = currentNow.addMinutes(-1);

            Job__c submittedJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.WAITING.name(),
                'submitted'
            );
            submittedJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            submittedJob.QueueableDispatch__c = dispatch.Id;
            submittedJob.QueueableDispatchInitiatedAt__c = currentNow.addMinutes(-10);
            submittedJob.AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 701);
            submittedJob.AvailableAt__c = currentNow.addMinutes(-1);

            Job__c workerJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.WAITING.name(), 'worker');
            workerJob.QueueableDispatch__c = dispatch.Id;
            workerJob.QueueableDispatchInitiatedAt__c = currentNow.addMinutes(-10);
            workerJob.AvailableAt__c = currentNow.addMinutes(-1);

            repository.insertJobs(
                new List<Job__c>{ staleJob, directStaleJob, futureJob, freshJob, submittedJob, workerJob }
            );
            Set<String> stateValues = QueuesSupport.toStateValues(
                new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED }
            );

            // Act
            List<AggregateResult> maintenanceQueueNames = repository
                .queryStaleInitiatedQueueableMaintenanceJobQueueNames(stateValues);
            Boolean hasStaleJob = repository.hasStaleInitiatedQueueableJob(
                queueName,
                stateValues,
                currentNow.addMinutes(-5),
                currentNow
            );
            Boolean hasNoOldEnoughJob = repository.hasStaleInitiatedQueueableJob(
                queueName,
                stateValues,
                currentNow.addMinutes(-20),
                currentNow
            );
            Boolean hasInvalidQueueName = repository.hasStaleInitiatedQueueableJob(
                null,
                stateValues,
                currentNow.addMinutes(-5),
                currentNow
            );
            List<Job__c> staleJobs = repository.queryStaleInitiatedQueueableJobsForUpdate(
                queueName,
                stateValues,
                currentNow.addMinutes(-5),
                currentNow,
                10
            );
            List<Job__c> limitedStaleJobs = repository.queryStaleInitiatedQueueableJobsForUpdate(
                queueName,
                stateValues,
                currentNow.addMinutes(-5),
                currentNow,
                1
            );

            // Assert
            Assert.areEqual(1, maintenanceQueueNames.size());
            Assert.areEqual(queueName, (String) maintenanceQueueNames[0].get('queueName'));
            Assert.areEqual(true, hasStaleJob);
            Assert.areEqual(false, hasNoOldEnoughJob);
            Assert.areEqual(false, hasInvalidQueueName);
            Assert.areEqual(2, staleJobs.size());
            Assert.areEqual(staleJob.Id, staleJobs[0].Id);
            Assert.areEqual(directStaleJob.Id, staleJobs[1].Id);
            Assert.areEqual(1, limitedStaleJobs.size());
            Assert.areEqual(staleJob.Id, limitedStaleJobs[0].Id);
        }
    }

    @IsTest
    static void should_query_expired_active_invocable_jobs_separately_from_other_modes() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'job-repo-invocable-expired';
            Datetime currentNow = System.now();
            QueuesJobRepository repository = new QueuesJobRepository();

            Job__c firstExpiredJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.ACTIVE.name(),
                'job-invocable-expired-1'
            );
            firstExpiredJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            firstExpiredJob.LeaseExpiresAt__c = currentNow.addMinutes(-4);

            Job__c secondExpiredJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.ACTIVE.name(),
                'job-invocable-expired-2'
            );
            secondExpiredJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            secondExpiredJob.LeaseExpiresAt__c = currentNow.addMinutes(-2);

            Job__c waitingCandidate = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.WAITING.name(),
                'job-invocable-waiting'
            );
            waitingCandidate.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            waitingCandidate.AvailableAt__c = currentNow.addMinutes(-10);

            Job__c futureInvocableJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.ACTIVE.name(),
                'job-invocable-future'
            );
            futureInvocableJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            futureInvocableJob.LeaseExpiresAt__c = currentNow.addMinutes(2);

            Job__c workerExpiredJob = QueuesTestSupport.job(
                queueName,
                'sync-invoice',
                Queues.State.ACTIVE.name(),
                'job-worker-expired'
            );
            workerExpiredJob.LeaseExpiresAt__c = currentNow.addMinutes(-5);

            repository.insertJobs(
                new List<Job__c>{
                    firstExpiredJob,
                    secondExpiredJob,
                    waitingCandidate,
                    futureInvocableJob,
                    workerExpiredJob
                }
            );

            // Act
            List<AggregateResult> maintenanceQueueNames = repository.queryInvocableMaintenanceJobQueueNames(
                QueuesSupport.toStateValues(
                    new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED, Queues.State.ACTIVE }
                )
            );
            Boolean hasRecoverableJob = repository.hasRecoverableInvocableJob(
                queueName,
                Queues.State.ACTIVE.name(),
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED }),
                currentNow,
                currentNow.addMinutes(-5)
            );
            Boolean hasNoRecoverableJob = repository.hasRecoverableInvocableJob(
                queueName,
                Queues.State.ACTIVE.name(),
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED }),
                currentNow.addMinutes(-10),
                currentNow.addMinutes(-20)
            );
            Boolean hasInvalidRecoverableJob = repository.hasRecoverableInvocableJob(
                null,
                Queues.State.ACTIVE.name(),
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED }),
                currentNow,
                currentNow.addMinutes(-5)
            );
            List<Job__c> emptyExpiredJobs = repository.queryExpiredActiveInvocableJobsForUpdate(
                queueName,
                Queues.State.ACTIVE.name(),
                currentNow,
                0
            );
            List<Job__c> lockedExpiredJobs = repository.queryExpiredActiveInvocableJobsForUpdate(
                queueName,
                Queues.State.ACTIVE.name(),
                currentNow,
                10
            );
            List<Job__c> limitedLockedExpiredJobs = repository.queryExpiredActiveInvocableJobsForUpdate(
                queueName,
                Queues.State.ACTIVE.name(),
                currentNow,
                1
            );
            List<Job__c> lockedStaleDueJobs = repository.queryStaleDueInvocableJobsForUpdate(
                queueName,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED }),
                currentNow.addMinutes(-5),
                10
            );

            // Assert
            Assert.areEqual(1, maintenanceQueueNames.size());
            Assert.areEqual(queueName, (String) maintenanceQueueNames[0].get('queueName'));
            Assert.areEqual(true, hasRecoverableJob);
            Assert.areEqual(false, hasNoRecoverableJob);
            Assert.areEqual(false, hasInvalidRecoverableJob);
            Assert.areEqual(0, emptyExpiredJobs.size());
            Assert.areEqual(2, lockedExpiredJobs.size());
            Assert.areEqual(firstExpiredJob.Id, lockedExpiredJobs[0].Id);
            Assert.areEqual(secondExpiredJob.Id, lockedExpiredJobs[1].Id);
            Assert.areEqual(1, limitedLockedExpiredJobs.size());
            Assert.areEqual(firstExpiredJob.Id, limitedLockedExpiredJobs[0].Id);
            Assert.areEqual(1, lockedStaleDueJobs.size());
            Assert.areEqual(waitingCandidate.Id, lockedStaleDueJobs[0].Id);
        }
    }

    @IsTest
    static void should_lock_due_queueable_dispatch_jobs_in_dispatch_order() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'job-repo-queueable-dispatch';
            Datetime currentNow = System.now();
            QueuesJobRepository repository = new QueuesJobRepository();
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = queueName,
                State__c = Queues.QueueableDispatchState.ACTIVE.name(),
                RequestedCount__c = 3,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            insert dispatch;

            Job__c secondJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.WAITING.name(), 'job-2');
            secondJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            secondJob.QueueableDispatch__c = dispatch.Id;
            secondJob.Priority__c = 2;
            secondJob.AvailableAt__c = currentNow.addMinutes(-1);

            Job__c firstJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.WAITING.name(), 'job-1');
            firstJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            firstJob.QueueableDispatch__c = dispatch.Id;
            firstJob.Priority__c = 1;
            firstJob.AvailableAt__c = currentNow.addMinutes(-1);

            Job__c futureJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.DELAYED.name(), 'job-3');
            futureJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            futureJob.QueueableDispatch__c = dispatch.Id;
            futureJob.Priority__c = 0;
            futureJob.AvailableAt__c = currentNow.addMinutes(5);

            Job__c retryJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.WAITING.name(), 'job-4');
            retryJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            retryJob.QueueableDispatch__c = dispatch.Id;
            retryJob.QueueableDispatchInitiatedAt__c = currentNow.addMinutes(-2);
            retryJob.Priority__c = -1;
            retryJob.AvailableAt__c = currentNow.addMinutes(-1);

            Job__c workerJob = QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.WAITING.name(), 'job-5');
            workerJob.QueueableDispatch__c = dispatch.Id;
            workerJob.Priority__c = 0;
            workerJob.AvailableAt__c = currentNow.addMinutes(-1);

            repository.insertJobs(new List<Job__c>{ secondJob, firstJob, futureJob, retryJob, workerJob });

            Set<String> dispatchableStateValues = QueuesSupport.toStateValues(
                new Set<Queues.State>{ Queues.State.WAITING, Queues.State.DELAYED }
            );

            // Act
            List<Job__c> lockedDispatchJobs = repository.queryQueueableDispatchJobsForUpdate(
                dispatch.Id,
                dispatchableStateValues,
                3
            );

            // Assert
            Assert.areEqual(3, lockedDispatchJobs.size());
            Assert.areEqual(futureJob.Id, lockedDispatchJobs[0].Id);
            Assert.areEqual(firstJob.Id, lockedDispatchJobs[1].Id);
            Assert.areEqual(secondJob.Id, lockedDispatchJobs[2].Id);
        }
    }

    @IsTest
    static void should_report_whether_queue_has_jobs_in_given_state() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'job-repo-has-state';
            QueuesJobRepository repository = new QueuesJobRepository();

            repository.insertJobs(
                new List<Job__c>{
                    QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.ACTIVE.name(), 'job-active'),
                    QueuesTestSupport.job(queueName, 'sync-invoice', Queues.State.WAITING.name(), 'job-waiting')
                }
            );

            // Act
            Boolean hasActiveJobs = repository.hasJobsInState(queueName, Queues.State.ACTIVE.name());
            Boolean hasFailedJobs = repository.hasJobsInState(queueName, Queues.State.FAILED.name());

            // Assert
            Assert.areEqual(true, hasActiveJobs);
            Assert.areEqual(false, hasFailedJobs);
        }
    }

    @IsTest
    static void should_return_empty_results_when_job_query_or_mutation_inputs_are_empty() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesJobRepository repository = new QueuesJobRepository();

            // Act
            List<Job__c> emptyDueJobs = repository.queryDueJobs(
                'job-repo-empty',
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING }),
                System.now(),
                0
            );
            List<Job__c> emptyJobsById = repository.queryJobsByIds(new Set<Id>());
            List<Job__c> emptyLockedJobsById = repository.queryJobsByIdsForUpdate(null, null);
            Boolean hasSchedulerJobs = repository.hasJobsBySchedulerId(
                null,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING })
            );
            List<Job__c> emptySchedulerJobs = repository.queryJobsBySchedulerIdForUpdate(
                null,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING }),
                10
            );
            List<Job__c> emptyAllSchedulerJobs = repository.queryJobsBySchedulerIdForUpdate(null, null, 10);
            List<Job__c> insertedJobs = repository.insertJobs(new List<Job__c>());
            List<Database.SaveResult> insertResults = repository.insertJobsAllowingPartialSuccess(new List<Job__c>());
            List<AggregateResult> emptyQueueableMaintenanceQueueNames = repository
                .queryQueueableMaintenanceJobQueueNames(new Set<String>());
            List<AggregateResult> emptyStaleInitiatedQueueableMaintenanceQueueNames = repository
                .queryStaleInitiatedQueueableMaintenanceJobQueueNames(new Set<String>());
            List<AggregateResult> emptyInvocableMaintenanceQueueNames = repository
                .queryInvocableMaintenanceJobQueueNames(new Set<String>());
            Boolean hasStaleWaitingChildrenJob = repository.hasStaleWaitingChildrenJob(null, System.now());
            Boolean hasStaleRemovedDependency = repository.hasStaleRemovedDependency(null, System.now());
            List<Job__c> emptyStaleWaitingChildrenJobs = repository.queryStaleWaitingChildrenJobs(
                'job-repo-empty',
                System.now(),
                0
            );
            List<Job__c> emptyRemovedDependencies = repository.queryFailedRemovedDependenciesForUpdate(
                new Set<Id>(),
                1
            );
            List<Job__c> emptyStaleRemovedDependencies = repository.queryStaleRemovedDependenciesForUpdate(
                'job-repo-empty',
                System.now(),
                0
            );
            List<Job__c> updatedJobs = repository.updateJobs(new List<Job__c>());
            List<Job__c> deletedJobs = repository.deleteJobs(null);
            List<Job__c> emptyExpiredActiveJobs = repository.queryExpiredActiveJobsForUpdate(
                'job-repo-empty',
                Queues.State.ACTIVE.name(),
                System.now(),
                0
            );
            List<Job__c> emptyExpiredQueueableJobs = repository.queryExpiredActiveQueueableJobsForUpdate(
                'job-repo-empty',
                Queues.State.ACTIVE.name(),
                System.now(),
                0
            );
            List<Job__c> emptyStaleInitiatedQueueableJobs = repository.queryStaleInitiatedQueueableJobsForUpdate(
                'job-repo-empty',
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING }),
                System.now(),
                System.now(),
                0
            );
            List<Job__c> emptyStaleDueInvocableJobs = repository.queryStaleDueInvocableJobsForUpdate(
                'job-repo-empty',
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING }),
                System.now(),
                0
            );
            List<Job__c> emptyDrainableJobs = repository.queryDrainableJobsForUpdate(
                'job-repo-empty',
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING }),
                0
            );
            List<Job__c> emptyQueueableDispatchJobs = repository.queryQueueableDispatchJobsForUpdate(
                null,
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING }),
                1
            );
            List<Job__c> emptyQueueableDispatchJobsById = repository.queryQueueableDispatchJobsForUpdate(
                QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 1),
                QueuesSupport.toStateValues(new Set<Queues.State>{ Queues.State.WAITING }),
                1
            );

            // Assert
            Assert.areEqual(0, emptyDueJobs.size());
            Assert.areEqual(0, emptyJobsById.size());
            Assert.areEqual(0, emptyLockedJobsById.size());
            Assert.areEqual(false, hasSchedulerJobs);
            Assert.areEqual(0, emptySchedulerJobs.size());
            Assert.areEqual(0, emptyAllSchedulerJobs.size());
            Assert.areEqual(0, insertedJobs.size());
            Assert.areEqual(0, insertResults.size());
            Assert.areEqual(0, emptyQueueableMaintenanceQueueNames.size());
            Assert.areEqual(0, emptyStaleInitiatedQueueableMaintenanceQueueNames.size());
            Assert.areEqual(0, emptyInvocableMaintenanceQueueNames.size());
            Assert.areEqual(false, hasStaleWaitingChildrenJob);
            Assert.areEqual(false, hasStaleRemovedDependency);
            Assert.areEqual(0, emptyStaleWaitingChildrenJobs.size());
            Assert.areEqual(0, emptyRemovedDependencies.size());
            Assert.areEqual(0, emptyStaleRemovedDependencies.size());
            Assert.areEqual(0, updatedJobs.size());
            Assert.areEqual(0, deletedJobs.size());
            Assert.areEqual(0, emptyExpiredActiveJobs.size());
            Assert.areEqual(0, emptyExpiredQueueableJobs.size());
            Assert.areEqual(0, emptyStaleInitiatedQueueableJobs.size());
            Assert.areEqual(0, emptyStaleDueInvocableJobs.size());
            Assert.areEqual(0, emptyDrainableJobs.size());
            Assert.areEqual(0, emptyQueueableDispatchJobs.size());
            Assert.areEqual(0, emptyQueueableDispatchJobsById.size());
        }
    }
}
