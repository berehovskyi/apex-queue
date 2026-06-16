@IsTest(IsParallel=true)
private class QueuesTest {
    @IsTest
    static void should_delegate_queue_operations_when_using_facade() {
        // Arrange
        Id firstJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 111);
        Id secondJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 112);
        Id schedulerJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 113);
        Id flowJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 114);
        Queues.JobScheduler schedulerValue = new Queues.JobScheduler(
            'nightly',
            QueuesTestSupport.DEFAULT_JOB_NAME,
            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            1,
            5,
            null,
            null,
            15,
            null,
            System.now().addMinutes(15),
            true,
            new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
        );
        Queues.QueueStats stats = new Queues.QueueStats(QueuesTestSupport.DEFAULT_QUEUE_NAME)
            .withRuntime(true, 2)
            .withStateCount(Queues.State.WAITING, 2)
            .withStateCount(Queues.State.DELAYED, 1)
            .withStateCount(Queues.State.ACTIVE, 1)
            .withStateCount(Queues.State.COMPLETED, 1)
            .withStateCount(Queues.State.FAILED, 1)
            .withStateCount(Queues.State.PAUSED, 1)
            .withStateCount(Queues.State.WAITING_CHILDREN, 1)
            .withStateCount(Queues.State.CANCELED, 1);

        Job__c firstJob = new Job__c(Id = firstJobId);
        Job__c secondJob = new Job__c(Id = secondJobId);
        Queues.JobNode flowNode = new Queues.JobNode(new Queues.Job(flowJobId));
        Queues.MaintenanceResult runtimeMaintenanceResult = new Queues.MaintenanceResult()
            .add(QueuesTestSupport.DEFAULT_QUEUE_NAME, 2);
        Queues.MaintenanceResult jobSchedulerMaintenanceResult = new Queues.MaintenanceResult()
            .add(QueuesTestSupport.DEFAULT_QUEUE_NAME, 1);
        Queues.MaintenanceResult terminalCleanupMaintenanceResult = new Queues.MaintenanceResult()
            .add(QueuesTestSupport.DEFAULT_QUEUE_NAME, 3);
        Queues.WorkerStatus workerStatus = new Queues.WorkerStatus(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 115),
            'Holding'
        );
        QueuesTestSupport.RecordingStubProvider provider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('enqueue', new List<Job__c>{ firstJob, secondJob })
            .withReturn('addFlow', flowNode)
            .withReturn('addFlowBulk', new List<Queues.JobNode>{ flowNode })
            .withReturn('getFlow', flowNode)
            .withReturn('getStats', stats)
            .withReturn('getJobState', Queues.State.ACTIVE)
            .withReturn('wake', 3)
            .withReturn('recoverStalled', 2)
            .withReturn('drain', 4)
            .withReturn('cancel', 5)
            .withReturn('getWorkerStatus', workerStatus)
            .withReturn('moveWorkerToFront', true)
            .withReturn('moveWorkerToEnd', true)
            .withReturn('upsertJobScheduler', new Job__c(Id = schedulerJobId))
            .withReturn('removeJobScheduler', true)
            .withReturn('cancelJobScheduler', true)
            .withReturn('getJobScheduler', schedulerValue)
            .withReturn('getJobSchedulers', new List<Queues.JobScheduler>{ schedulerValue })
            .withReturn('getJobSchedulersCount', 1)
            .withReturn('runRuntimeMaintenance', runtimeMaintenanceResult)
            .withReturn('runJobSchedulerMaintenance', jobSchedulerMaintenanceResult)
            .withReturn('runTerminalCleanupMaintenance', terminalCleanupMaintenanceResult);
        QueuesService serviceStub = (QueuesService) Test.createStub(QueuesService.class, provider);

        try {
            Queues.queueService = serviceStub;

            // Act
            Queues.Queue queue = Queues.of(QueuesTestSupport.DEFAULT_QUEUE_NAME);
            Queues.Job addedJob = queue.add(
                QueuesTestSupport.DEFAULT_JOB_NAME,
                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                new Map<String, Object>{ 'invoiceId' => 'INV-111' }
            );
            Queues.Job addedWithBulkOptions = queue.add(
                QueuesTestSupport.DEFAULT_JOB_NAME,
                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                new Map<String, Object>{ 'invoiceId' => 'INV-112' },
                new Queues.BulkJobOptions().worker()
            );
            List<Queues.Job> addedJobs = queue.addBulk(
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        null
                    )
                }
            );
            List<Queues.Job> dedupedJobs = queue.addBulk(
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'invoiceId' => 'INV-DEDUPED' }
                    )
                },
                new Queues.BulkJobOptions().dedupeKey('bulk-1')
            );
            Queues.FlowJob flow = new Queues.FlowJob(
                QueuesTestSupport.DEFAULT_JOB_NAME,
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
            );
            Queues.JobNode addedFlow = Queues.flowProducer().add(flow);
            Queues.JobNode addedFlowWithOpts = Queues.flowProducer().add(flow, new Queues.FlowOpts().queueable());
            List<Queues.JobNode> addedFlows = Queues.flowProducer().addBulk(new List<Queues.FlowJob>{ flow });
            List<Queues.JobNode> addedFlowsWithOpts = Queues.flowProducer()
                .addBulk(new List<Queues.FlowJob>{ flow }, new Queues.FlowOpts().invocable());
            Queues.JobNode fetchedFlow = Queues.flowProducer().getFlow(flowJobId);

            queue.pause();
            queue.resume();

            Queues.QueueStats actualStats = queue.getStats();
            Queues.State jobState = queue.getJobState(firstJobId);
            Integer waitingCount = queue.getWaitingCount();
            Integer delayedCount = queue.getDelayedCount();
            Integer activeCount = queue.getActiveCount();
            Integer completedCount = queue.getCompletedCount();
            Integer failedCount = queue.getFailedCount();
            Integer pausedCount = queue.getPausedCount();
            Integer waitingChildrenCount = queue.getWaitingChildrenCount();
            Integer canceledCount = queue.getCanceledCount();
            Integer totalCount = queue.count();
            Map<Queues.State, Integer> allCounts = queue.getJobCounts();
            Map<Queues.State, Integer> filteredCounts = queue.getJobCounts(
                new Set<Queues.State>{ Queues.State.WAITING, Queues.State.FAILED }
            );
            Queues.Job fetchedJob = queue.getJob(firstJobId);
            Integer wakeCount = queue.wake();
            Integer recoveredCount = queue.recoverStalled();
            Queues.Job schedulerJob = queue.upsertJobScheduler(
                'nightly',
                new Queues.RepeatOptions().every(15),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
            );
            Queues.Job queueableSchedulerJob = queue.upsertJobScheduler(
                'nightly-queueable',
                new Queues.RepeatOptions().every(15),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS),
                Queues.ExecutionMode.QUEUEABLE_SERIAL
            );
            Queues.Job schedulerJobWithDefaultTemplate = queue.upsertJobScheduler(
                'nightly-default',
                new Queues.RepeatOptions().every(30)
            );
            Boolean removedScheduler = queue.removeJobScheduler('nightly');
            Boolean canceledScheduler = queue.cancelJobScheduler('nightly');
            Queues.JobScheduler fetchedScheduler = queue.getJobScheduler('nightly');
            List<Queues.JobScheduler> fetchedSchedulers = queue.getJobSchedulers();
            Integer schedulerCount = queue.getJobSchedulersCount();
            Integer drainedCount = queue.drain();
            Integer queueCanceledCount = queue.cancel();
            Queues.WorkerStatus returnedWorkerStatus = queue.getWorkerStatus();
            Boolean movedWorkerToFront = queue.moveWorkerToFront();
            Boolean movedWorkerToEnd = queue.moveWorkerToEnd();
            Queues.MaintenanceResult runtimeMaintenance = Queues.runRuntimeMaintenance();
            Queues.MaintenanceResult jobSchedulerMaintenance = Queues.runJobSchedulerMaintenance();
            Queues.MaintenanceResult terminalCleanupMaintenance = Queues.runTerminalCleanupMaintenance();

            // Assert
            Assert.areEqual(firstJobId, addedJob.getId());
            Assert.areEqual(firstJobId, addedWithBulkOptions.getId());
            Assert.areEqual(2, addedJobs.size());
            Assert.areEqual(2, dedupedJobs.size());
            Assert.areEqual(flowJobId, addedFlow.getJob().getId());
            Assert.areEqual(flowJobId, addedFlowWithOpts.getJob().getId());
            Assert.areEqual(flowJobId, fetchedFlow.getJob().getId());
            Assert.areEqual(1, addedFlows.size());
            Assert.areEqual(1, addedFlowsWithOpts.size());
            Queues.BulkJobOptions recordedBulkOpts = (Queues.BulkJobOptions) provider.getCalls('enqueue')[3][2];
            Assert.areEqual('bulk-1', recordedBulkOpts.getDedupeKey());
            Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, queue.getName());
            Assert.areEqual(stats, actualStats);
            Assert.areEqual(Queues.State.ACTIVE, jobState);
            Assert.areEqual(2, waitingCount);
            Assert.areEqual(1, delayedCount);
            Assert.areEqual(1, activeCount);
            Assert.areEqual(1, completedCount);
            Assert.areEqual(1, failedCount);
            Assert.areEqual(1, pausedCount);
            Assert.areEqual(1, waitingChildrenCount);
            Assert.areEqual(1, canceledCount);
            Assert.areEqual(9, totalCount);
            Assert.areEqual(8, allCounts.size());
            Assert.areEqual(
                new Map<Queues.State, Integer>{ Queues.State.WAITING => 2, Queues.State.FAILED => 1 },
                filteredCounts
            );
            Assert.areEqual(firstJobId, fetchedJob.getId());
            Assert.areEqual(3, wakeCount);
            Assert.areEqual(2, recoveredCount);
            Assert.areEqual(schedulerJobId, schedulerJob.getId());
            Assert.areEqual(schedulerJobId, queueableSchedulerJob.getId());
            Assert.areEqual(schedulerJobId, schedulerJobWithDefaultTemplate.getId());
            Assert.areEqual(true, removedScheduler);
            Assert.areEqual(true, canceledScheduler);
            Assert.areEqual(schedulerValue, fetchedScheduler);
            Assert.areEqual(1, fetchedSchedulers.size());
            Assert.areEqual(1, schedulerCount);
            Assert.areEqual(4, drainedCount);
            Assert.areEqual(5, queueCanceledCount);
            Assert.areEqual(workerStatus, returnedWorkerStatus);
            Assert.areEqual(true, movedWorkerToFront);
            Assert.areEqual(true, movedWorkerToEnd);
            Assert.areEqual(2, runtimeMaintenance.total);
            Assert.areEqual(2, runtimeMaintenance.getCount(QueuesTestSupport.DEFAULT_QUEUE_NAME));
            Assert.areEqual(1, jobSchedulerMaintenance.total);
            Assert.areEqual(1, jobSchedulerMaintenance.getCount(QueuesTestSupport.DEFAULT_QUEUE_NAME));
            Assert.areEqual(3, terminalCleanupMaintenance.total);
            Assert.areEqual(3, terminalCleanupMaintenance.getCount(QueuesTestSupport.DEFAULT_QUEUE_NAME));
            Assert.areEqual(2, provider.getCallCount('addFlow'));
            Assert.areEqual(2, provider.getCallCount('addFlowBulk'));
            Assert.areEqual(1, provider.getCallCount('getFlow'));
            Assert.areEqual(1, provider.getCallCount('runRuntimeMaintenance'));
            Assert.areEqual(1, provider.getCallCount('runJobSchedulerMaintenance'));
            Assert.areEqual(1, provider.getCallCount('runTerminalCleanupMaintenance'));
            Assert.areEqual(1, provider.getCallCount('getWorkerStatus'));
            Assert.areEqual(1, provider.getCallCount('moveWorkerToFront'));
            Assert.areEqual(1, provider.getCallCount('moveWorkerToEnd'));
        } finally {
            QueuesTestSupport.resetFacadeService();
        }
    }

    @IsTest
    static void should_reject_invalid_backoff_jitter_options() {
        // Arrange
        List<String> messages = new List<String>();

        // Act
        try {
            new Queues.JobOptions().backoff(Queues.Backoff.FIXED, 1, -0.01);
            Assert.fail('Expected negative jitter to throw.');
        } catch (Queues.QueueException ex) {
            messages.add(ex.getMessage());
        }
        try {
            new Queues.JobOptions().backoff(Queues.Backoff.FIXED, 1, 1.01);
            Assert.fail('Expected oversized jitter to throw.');
        } catch (Queues.QueueException ex) {
            messages.add(ex.getMessage());
        }
        try {
            new Queues.JobOptions().backoff(Queues.Backoff.NONE, 1, 0.1);
            Assert.fail('Expected jitter without backoff to throw.');
        } catch (Queues.QueueException ex) {
            messages.add(ex.getMessage());
        }

        // Assert
        Assert.areEqual(
            new List<String>{
                'jitter must be between 0 and 1',
                'jitter must be between 0 and 1',
                'jitter requires a backoff strategy'
            },
            messages
        );
    }

    @IsTest
    static void should_coerce_decimal_string_values() {
        // Act / Assert
        Assert.areEqual(0.25, QueuesSupport.toDecimal('0.25', 0));
    }

    @IsTest
    static void should_delegate_job_operations_when_using_facade() {
        // Arrange
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 121);
        QueuesTestSupport.RecordingStubProvider provider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns(
                'getJobState',
                new List<Object>{
                    Queues.State.WAITING,
                    Queues.State.WAITING,
                    Queues.State.DELAYED,
                    Queues.State.ACTIVE,
                    Queues.State.PAUSED,
                    Queues.State.COMPLETED,
                    Queues.State.FAILED,
                    Queues.State.WAITING_CHILDREN,
                    Queues.State.CANCELED
                }
            )
            .withReturn('retryJob', new Job__c(Id = jobId))
            .withReturn('pauseJob', new Job__c(Id = jobId))
            .withReturn('resumeJob', new Job__c(Id = jobId))
            .withReturn('promoteJob', new Job__c(Id = jobId))
            .withReturn('discardJob', new Job__c(Id = jobId))
            .withReturn('deferJob', new Job__c(Id = jobId))
            .withReturn('changePriority', new Job__c(Id = jobId))
            .withReturn('updateJobData', new Job__c(Id = jobId))
            .withReturn('updateJobProgress', new Job__c(Id = jobId))
            .withReturn('cancelJob', new Job__c(Id = jobId));
        QueuesService serviceStub = (QueuesService) Test.createStub(QueuesService.class, provider);

        try {
            Queues.queueService = serviceStub;

            // Act
            Queues.Job job = Queues.job(jobId);
            job.retry();
            job.pause();
            job.resume();
            job.promote();
            job.discard();
            job.defer(5);
            job.changePriority(7);
            job.updateData(new Map<String, Object>{ 'invoiceId' => 'INV-121' });
            job.updateProgress(new Map<String, Object>{ 'step' => 'queued' });
            job.cancel();
            job.remove();

            Queues.State jobState = job.getState();
            Boolean waiting = job.isWaiting();
            Boolean delayed = job.isDelayed();
            Boolean active = job.isActive();
            Boolean paused = job.isPaused();
            Boolean completed = job.isCompleted();
            Boolean failed = job.isFailed();
            Boolean waitingChildren = job.isWaitingChildren();
            Boolean canceled = job.isCanceled();

            // Assert
            Assert.areEqual(jobId, job.getId());
            Assert.areEqual(Queues.State.WAITING, jobState);
            Assert.areEqual(true, waiting);
            Assert.areEqual(true, delayed);
            Assert.areEqual(true, active);
            Assert.areEqual(true, paused);
            Assert.areEqual(true, completed);
            Assert.areEqual(true, failed);
            Assert.areEqual(true, waitingChildren);
            Assert.areEqual(true, canceled);
            Assert.areEqual(1, provider.getCallCount('cancelJob'));
            Assert.areEqual(1, provider.getCallCount('removeJob'));
        } finally {
            QueuesTestSupport.resetFacadeService();
        }
    }

    @IsTest
    static void should_expose_queue_value_objects_when_building_primitives() {
        // Arrange
        Id parentJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 131);
        Job__c job = new Job__c(
            Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 132),
            QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
            Name__c = QueuesTestSupport.DEFAULT_JOB_NAME,
            ProcessorClass__c = QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            AttemptsMade__c = 2,
            MaxAttempts__c = 5,
            PauseRequested__c = true,
            Data__c = '{"invoiceId":"INV-132"}',
            Checkpoint__c = '{"cursor":2}',
            Progress__c = '{"step":"queued"}'
        );
        Job__c idempotentJob = (Job__c) QueuesSupport.clone(job);
        idempotentJob.ExternalId__c = 'job-132';
        idempotentJob.QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            idempotentJob.ExternalId__c
        );

        // Act
        Queues.JobOptions opts = new Queues.JobOptions()
            .jobId('job-132')
            .attempts(3)
            .priority(4)
            .delay(1)
            .availableAt(System.now().addMinutes(6))
            .backoff(Queues.Backoff.NONE, 9)
            .parent(parentJobId);
        Queues.JobOptions jitterOpts = new Queues.JobOptions().backoff(Queues.Backoff.EXPONENTIAL, 5, 0.25);
        Queues.JobOptions defaultOpts = new Queues.JobOptions();
        Queues.JobOptions dependencyOpts = new Queues.JobOptions()
            .failParentOnFailure()
            .continueParentOnFailure()
            .ignoreDependencyOnFailure()
            .removeDependencyOnFailure()
            .cancelParentOnCanceled()
            .failParentOnCanceled()
            .ignoreDependencyOnCanceled();
        Queues.BulkJobOptions bulkOpts = new Queues.BulkJobOptions().dedupeKey('bulk-132').queueableConcurrent();
        Queues.BulkJobOptions explicitWorkerBulkOpts = new Queues.BulkJobOptions().queueable().worker();
        Queues.BulkJobOptions invocableBulkOpts = new Queues.BulkJobOptions().queueable().invocable();
        Queues.BulkJobOptions defaultBulkOpts = new Queues.BulkJobOptions();
        Queues.RepeatOptions everyRepeat = new Queues.RepeatOptions()
            .every(15)
            .lim(3)
            .startAt(System.now().addMinutes(1))
            .endAt(System.now().addHours(1));
        Queues.RepeatOptions patternRepeat = new Queues.RepeatOptions().pattern('0 0 0 * * ?');
        Queues.JobTemplate defaultTemplate = new Queues.JobTemplate(
                QueuesTestSupport.DEFAULT_JOB_NAME,
                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
            )
            .data(new Map<String, Object>{ 'invoiceId' => 'INV-DEFAULT-TEMPLATE' });
        Queues.JobTemplate processorOnlyTemplate = new Queues.JobTemplate(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS);
        Queues.JobTemplate template = new Queues.JobTemplate(
                QueuesTestSupport.DEFAULT_JOB_NAME,
                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
            )
            .data(new Map<String, Object>{ 'invoiceId' => 'INV-TEMPLATE' })
            .opts(new Queues.JobOptions().priority(8));
        Queues.JobTemplate scheduledTemplate = new Queues.JobTemplate(
                QueuesTestSupport.DEFAULT_JOB_NAME,
                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
            )
            .data(new Map<String, Object>{ 'invoiceId' => 'INV-SCHEDULED-TEMPLATE' })
            .opts(new Queues.JobOptions().priority(8));
        Map<String, Object> flowData = new Map<String, Object>{ 'invoiceId' => 'INV-FLOW' };
        Queues.FlowJob defaultFlowJob = new Queues.FlowJob(
                'flow-root',
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
            )
            .data(flowData)
            .opts(new Queues.JobOptions().priority(6))
            .children(
                new List<Queues.FlowJob>{
                    new Queues.FlowJob(
                        'flow-child',
                        QueuesTestSupport.DEFAULT_QUEUE_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                    )
                }
            )
            .child(
                new Queues.FlowJob(
                    'flow-child-2',
                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                )
            );
        Queues.FlowJob constructorFlowJob = new Queues.FlowJob(
            'flow-constructor',
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
        );
        Queues.FlowJob constructorFlowJobWithData = new Queues.FlowJob(
                'flow-constructor-data',
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
            )
            .data(flowData);
        Queues.JobNode leafNode = new Queues.JobNode(new Queues.Job(QueuesTestSupport.fakeId(Job__c.SObjectType, 133)));
        Queues.JobNode rootNode = new Queues.JobNode(
            new Queues.Job(QueuesTestSupport.fakeId(Job__c.SObjectType, 134)),
            new List<Queues.JobNode>{ leafNode }
        );
        Queues.FlowOpts workerFlowOpts = new Queues.FlowOpts().queueable().worker();
        Queues.FlowOpts concurrentFlowOpts = new Queues.FlowOpts().queueableConcurrent();
        Queues.FlowOpts invocableFlowOpts = new Queues.FlowOpts().invocable();
        Queues.JobScheduler scheduler = new Queues.JobScheduler(
            'nightly',
            scheduledTemplate.getName(),
            scheduledTemplate.getProcessor(),
            2,
            everyRepeat.getLimit(),
            everyRepeat.getStartAt(),
            everyRepeat.getEndAt(),
            everyRepeat.getEveryMinutes(),
            null,
            everyRepeat.getStartAt(),
            true,
            scheduledTemplate
        );
        Queues.JobContext ctx = new Queues.JobContext(job);
        Queues.JobContext idempotentCtx = new Queues.JobContext(idempotentJob);
        Queues.ProcessResult completed = ctx.complete(new Map<String, Object>{ 'status' => 'done' })
            .withProgress(new Map<String, Object>{ 'step' => 'done' });
        Queues.ProcessResult failed = ctx.fail('failed');
        Queues.ProcessResult paused = ctx.pause(new Map<String, Object>{ 'cursor' => 3 });
        Queues.ProcessResult deferred = ctx.defer(new Map<String, Object>{ 'cursor' => 4 }, 7);
        Queues.QueueStats stats = new Queues.QueueStats(QueuesTestSupport.DEFAULT_QUEUE_NAME)
            .withRuntime(false, 1)
            .withStateCount(Queues.State.WAITING, 1)
            .withStateCount(Queues.State.DELAYED, 2)
            .withStateCount(Queues.State.ACTIVE, 3)
            .withStateCount(Queues.State.COMPLETED, 4)
            .withStateCount(Queues.State.FAILED, 5)
            .withStateCount(Queues.State.PAUSED, 6)
            .withStateCount(Queues.State.WAITING_CHILDREN, 7)
            .withStateCount(Queues.State.CANCELED, 8);

        // Assert
        Assert.areEqual(parentJobId, opts.getParentJobId());
        Assert.areEqual(Queues.Backoff.NONE, opts.getBackoffType());
        Assert.areEqual(0, opts.getBackoffValue());
        Assert.areEqual(0.0, opts.getBackoffJitter());
        Assert.areEqual(Queues.Backoff.EXPONENTIAL, jitterOpts.getBackoffType());
        Assert.areEqual(5, jitterOpts.getBackoffValue());
        Assert.areEqual(0.25, jitterOpts.getBackoffJitter());
        Assert.areEqual(null, defaultOpts.getJobId());
        Assert.areEqual(null, defaultOpts.getDependencyFailurePolicy());
        Assert.areEqual(null, defaultOpts.getDependencyCanceledPolicy());
        Assert.areEqual(Queues.DependencyFailurePolicy.REMOVE_DEPENDENCY, dependencyOpts.getDependencyFailurePolicy());
        Assert.areEqual(
            Queues.DependencyCanceledPolicy.IGNORE_DEPENDENCY,
            dependencyOpts.getDependencyCanceledPolicy()
        );
        Assert.areEqual('bulk-132', bulkOpts.getDedupeKey());
        Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT, bulkOpts.getExecutionMode());
        Assert.areEqual(Queues.ExecutionMode.WORKER, explicitWorkerBulkOpts.getExecutionMode());
        Assert.areEqual(Queues.ExecutionMode.INVOCABLE, invocableBulkOpts.getExecutionMode());
        Assert.areEqual(Queues.ExecutionMode.WORKER, defaultBulkOpts.getExecutionMode());
        Assert.areEqual(15, everyRepeat.getEveryMinutes());
        Assert.areEqual(3, everyRepeat.getLimit());
        Assert.areEqual('0 0 0 * * ?', patternRepeat.getPattern());
        Assert.areEqual(QueuesTestSupport.DEFAULT_JOB_NAME, defaultTemplate.getName());
        Assert.areEqual(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, defaultTemplate.getProcessor());
        Assert.areEqual('INV-DEFAULT-TEMPLATE', ((Map<String, Object>) defaultTemplate.getData()).get('invoiceId'));
        Assert.areEqual(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, processorOnlyTemplate.getName());
        Assert.areEqual(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, processorOnlyTemplate.getProcessor());
        Assert.areEqual(QueuesTestSupport.DEFAULT_JOB_NAME, template.getName());
        Assert.areEqual(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, template.getProcessor());
        Assert.areEqual(8, template.getOpts().getPriority());
        Assert.areEqual(QueuesTestSupport.DEFAULT_JOB_NAME, scheduledTemplate.getName());
        Assert.areEqual(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, scheduledTemplate.getProcessor());
        Assert.areEqual(8, scheduledTemplate.getOpts().getPriority());
        Assert.areEqual('flow-root', defaultFlowJob.getName());
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, defaultFlowJob.getQueueName());
        Assert.areEqual(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, defaultFlowJob.getProcessor());
        Assert.areEqual('INV-FLOW', ((Map<String, Object>) defaultFlowJob.getData()).get('invoiceId'));
        Assert.areEqual(6, defaultFlowJob.getOpts().getPriority());
        Assert.areEqual(2, defaultFlowJob.getChildren().size());
        Assert.areEqual('flow-constructor', constructorFlowJob.getName());
        Assert.areEqual(null, constructorFlowJob.getData());
        Assert.areEqual('flow-constructor-data', constructorFlowJobWithData.getName());
        Assert.areEqual(flowData, constructorFlowJobWithData.getData());
        Assert.areEqual(false, leafNode.hasChildren());
        Assert.areEqual(true, rootNode.hasChildren());
        Assert.areEqual(1, rootNode.getChildren().size());
        Assert.areEqual(Queues.ExecutionMode.WORKER, workerFlowOpts.getExecutionMode());
        Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT, concurrentFlowOpts.getExecutionMode());
        Assert.areEqual(Queues.ExecutionMode.INVOCABLE, invocableFlowOpts.getExecutionMode());
        Assert.areEqual('nightly', scheduler.getJobSchedulerId());
        Assert.areEqual(QueuesTestSupport.DEFAULT_JOB_NAME, scheduler.getJobName());
        Assert.areEqual(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, scheduler.getProcessor());
        Assert.areEqual(2, scheduler.getIterationCount());
        Assert.areEqual(3, scheduler.getLimit());
        Assert.areEqual(everyRepeat.getStartAt(), scheduler.getStartAt());
        Assert.areEqual(everyRepeat.getEndAt(), scheduler.getEndAt());
        Assert.areEqual(15, scheduler.getEveryMinutes());
        Assert.areEqual(null, scheduler.getPattern());
        Assert.areEqual(everyRepeat.getStartAt(), scheduler.getNextRunAt());
        Assert.areEqual(true, scheduler.isActive());
        Assert.areEqual(scheduledTemplate, scheduler.getTemplate());

        Assert.areEqual(job.Id, ctx.jobId());
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, ctx.queueName());
        Assert.areEqual(QueuesTestSupport.DEFAULT_JOB_NAME, ctx.name());
        Assert.areEqual(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, ctx.processorClass());
        Assert.areEqual(
            QueuesSupport.buildQueueScopedExternalId(QueuesTestSupport.DEFAULT_QUEUE_NAME, job.Id),
            ctx.idempotencyKey()
        );
        Assert.areEqual(2, ctx.attemptsMade());
        Assert.areEqual(5, ctx.maxAttempts());
        Assert.areEqual(true, ctx.isPauseRequested());
        Assert.areEqual('INV-132', ((Map<String, Object>) ctx.data()).get('invoiceId'));
        Assert.areEqual(2, QueuesSupport.toInteger(((Map<String, Object>) ctx.checkpoint()).get('cursor'), 0));
        Assert.areEqual('queued', ((Map<String, Object>) ctx.progress()).get('step'));
        Assert.areEqual(idempotentJob.QueueScopedExternalId__c, idempotentCtx.idempotencyKey());

        Assert.areEqual(Queues.Outcome.COMPLETED, completed.getOutcome());
        Assert.areEqual('{"status":"done"}', completed.getReturnValueJson());
        Assert.areEqual('{"step":"done"}', completed.getProgressJson());
        Assert.areEqual('failed', failed.getFailedReason());
        Assert.areEqual('{"cursor":3}', paused.getCheckpointJson());
        Assert.areEqual(7, deferred.getDelay());

        Assert.areEqual(1, stats.getCount(Queues.State.WAITING));
        Assert.areEqual(2, stats.getCount(Queues.State.DELAYED));
        Assert.areEqual(3, stats.getCount(Queues.State.ACTIVE));
        Assert.areEqual(4, stats.getCount(Queues.State.COMPLETED));
        Assert.areEqual(5, stats.getCount(Queues.State.FAILED));
        Assert.areEqual(6, stats.getCount(Queues.State.PAUSED));
        Assert.areEqual(7, stats.getCount(Queues.State.WAITING_CHILDREN));
        Assert.areEqual(8, stats.getCount(Queues.State.CANCELED));
        Assert.areEqual(0, stats.getCount(null));
        Assert.areEqual(8, stats.asJobCounts().size());
        Assert.areEqual(
            new Map<Queues.State, Integer>{ Queues.State.ACTIVE => 3, Queues.State.PAUSED => 6 },
            stats.asJobCounts(new Set<Queues.State>{ Queues.State.ACTIVE, Queues.State.PAUSED })
        );
    }

    @IsTest
    static void should_keep_job_context_idempotency_key_stable_across_attempts() {
        // Arrange
        Job__c logicalJob = new Job__c(
            Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 140),
            QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
            Name__c = QueuesTestSupport.DEFAULT_JOB_NAME,
            ProcessorClass__c = QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            ExternalId__c = 'job-140',
            QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                'job-140'
            ),
            AttemptsMade__c = 0,
            MaxAttempts__c = 3
        );
        Job__c sameLogicalJobLaterAttempt = (Job__c) QueuesSupport.clone(logicalJob);
        sameLogicalJobLaterAttempt.AttemptsMade__c = 2;
        sameLogicalJobLaterAttempt.State__c = Queues.State.FAILED.name();

        Job__c logicalJobWithoutExternalId = new Job__c(
            Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 141),
            QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
            Name__c = QueuesTestSupport.DEFAULT_JOB_NAME,
            ProcessorClass__c = QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            AttemptsMade__c = 0,
            MaxAttempts__c = 3
        );
        Job__c sameDurableJobLaterAttempt = (Job__c) QueuesSupport.clone(logicalJobWithoutExternalId);
        sameDurableJobLaterAttempt.AttemptsMade__c = 2;
        sameDurableJobLaterAttempt.State__c = Queues.State.FAILED.name();

        // Act
        Queues.JobContext firstAttemptCtx = new Queues.JobContext(logicalJob);
        Queues.JobContext laterAttemptCtx = new Queues.JobContext(sameLogicalJobLaterAttempt);
        Queues.JobContext firstDurableCtx = new Queues.JobContext(logicalJobWithoutExternalId);
        Queues.JobContext laterDurableCtx = new Queues.JobContext(sameDurableJobLaterAttempt);

        // Assert
        Assert.areEqual(firstAttemptCtx.idempotencyKey(), laterAttemptCtx.idempotencyKey());
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME + ':job-140', firstAttemptCtx.idempotencyKey());
        Assert.areEqual(firstDurableCtx.idempotencyKey(), laterDurableCtx.idempotencyKey());
        Assert.areEqual(
            QueuesTestSupport.DEFAULT_QUEUE_NAME + ':' + logicalJobWithoutExternalId.Id,
            firstDurableCtx.idempotencyKey()
        );
    }
}
