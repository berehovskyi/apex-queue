@IsTest(IsParallel=false)
private class QueuesServiceTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_delegate_runtime_maintenance_and_queue_errors_when_supporting_services_are_invoked() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            Queues.MaintenanceResult runtimeMaintenanceResult = new Queues.MaintenanceResult()
                .add(QueuesTestSupport.DEFAULT_QUEUE_NAME, 2);
            QueuesTestSupport.RecordingStubProvider maintenanceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('runRuntimeMaintenance', runtimeMaintenanceResult);
            QueuesMaintenanceService maintenanceStub = (QueuesMaintenanceService) Test.createStub(
                QueuesMaintenanceService.class,
                maintenanceProvider
            );
            List<QueueError__c> expectedErrors = new List<QueueError__c>{
                new QueueError__c(
                    QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    Component__c = 'runtime maintenance',
                    Message__c = 'boom',
                    OccurredAt__c = System.now()
                )
            };
            QueuesTestSupport.RecordingStubProvider errorRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('insertQueueErrors', expectedErrors);
            QueuesErrorRepository errorRepositoryStub = (QueuesErrorRepository) Test.createStub(
                QueuesErrorRepository.class,
                errorRepositoryProvider
            );
            QueuesErrorService errorService = new QueuesErrorService(
                errorRepositoryStub,
                new QueuesErrorEventRepository()
            );

            QueuesService service = new QueuesService(
                new QueuesDefinitionService(),
                errorService,
                new QueuesFlowProducerService(),
                new QueuesJobService(),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                maintenanceStub,
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();
            List<QueueError__c> errors = service.insertQueueErrors(expectedErrors);

            // Assert
            Assert.areEqual(2, recovered.total);
            Assert.areEqual(2, recovered.getCount(QueuesTestSupport.DEFAULT_QUEUE_NAME));
            Assert.areEqual(1, maintenanceProvider.getCallCount('runRuntimeMaintenance'));
            Assert.areEqual(1, errorRepositoryProvider.getCallCount('insertQueueErrors'));
            Assert.areEqual(1, errors.size());
        }
    }

    @IsTest
    static void should_delegate_flow_operations_when_supporting_service_is_invoked() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            Queues.FlowJob flow = new Queues.FlowJob(
                QueuesTestSupport.DEFAULT_JOB_NAME,
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
            );
            Queues.FlowOpts opts = new Queues.FlowOpts().queueableConcurrent();
            Queues.JobNode expectedNode = new Queues.JobNode(
                new Queues.Job(QueuesTestSupport.fakeId(Job__c.SObjectType, 1700))
            );
            List<Queues.JobNode> expectedNodes = new List<Queues.JobNode>{ expectedNode };
            QueuesTestSupport.RecordingStubProvider flowProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('add', expectedNode)
                .withReturn('addBulk', expectedNodes)
                .withReturn('getFlow', expectedNode);
            QueuesFlowProducerService flowProducerService = (QueuesFlowProducerService) Test.createStub(
                QueuesFlowProducerService.class,
                flowProvider
            );
            QueuesService service = new QueuesService(
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                flowProducerService,
                new QueuesJobService(),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                new QueuesWorkerService()
            );

            // Act
            Queues.JobNode addedNode = service.addFlow(flow);
            Queues.JobNode addedNodeWithOpts = service.addFlow(flow, opts);
            List<Queues.JobNode> addedNodes = service.addFlowBulk(new List<Queues.FlowJob>{ flow });
            List<Queues.JobNode> addedNodesWithOpts = service.addFlowBulk(new List<Queues.FlowJob>{ flow }, opts);
            Queues.JobNode fetchedNode = service.getFlow(expectedNode.getJob().getId());

            // Assert
            Assert.areEqual(expectedNode, addedNode);
            Assert.areEqual(expectedNode, addedNodeWithOpts);
            Assert.areEqual(expectedNodes, addedNodes);
            Assert.areEqual(expectedNodes, addedNodesWithOpts);
            Assert.areEqual(expectedNode, fetchedNode);
            Assert.areEqual(2, flowProvider.getCallCount('add'));
            Assert.areEqual(2, flowProvider.getCallCount('addBulk'));
            Assert.areEqual(1, flowProvider.getCallCount('getFlow'));
        }
    }

    @IsTest
    static void should_delegate_job_scheduler_operations_when_supporting_service_is_invoked() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            Id materializedJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 1701);
            Queues.JobScheduler schedulerValue = new Queues.JobScheduler(
                'nightly',
                QueuesTestSupport.DEFAULT_JOB_NAME,
                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                1,
                null,
                null,
                null,
                5,
                null,
                System.now().addMinutes(5),
                true,
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
            );
            Queues.MaintenanceResult schedulerMaintenanceResult = new Queues.MaintenanceResult()
                .add(QueuesTestSupport.DEFAULT_QUEUE_NAME, 2);
            QueuesTestSupport.RecordingStubProvider schedulerProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('runJobSchedulerMaintenanceAsSystem', schedulerMaintenanceResult)
                .withReturn('upsertJobScheduler', new Job__c(Id = materializedJobId))
                .withReturn('removeJobScheduler', true)
                .withReturn('cancelJobScheduler', true)
                .withReturn('getJobScheduler', schedulerValue)
                .withReturn('getJobSchedulers', new List<Queues.JobScheduler>{ schedulerValue })
                .withReturn('getJobSchedulersCount', 1);
            QueuesService service = new QueuesService(
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                new QueuesJobService(),
                new QueuesJobAdminService(),
                (QueuesJobSchedulerService) Test.createStub(QueuesJobSchedulerService.class, schedulerProvider),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                new QueuesWorkerService()
            );

            // Act
            Queues.MaintenanceResult repaired = service.runJobSchedulerMaintenance();
            Job__c materializedJob = service.upsertJobScheduler(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                'nightly',
                new Queues.RepeatOptions().every(5),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
            );
            Job__c queueableMaterializedJob = service.upsertJobScheduler(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                'nightly-queueable',
                new Queues.RepeatOptions().every(5),
                new Queues.JobTemplate(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS),
                Queues.ExecutionMode.QUEUEABLE_SERIAL
            );
            Boolean removed = service.removeJobScheduler(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'nightly');
            Boolean canceled = service.cancelJobScheduler(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'nightly');
            Queues.JobScheduler fetched = service.getJobScheduler(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'nightly');
            List<Queues.JobScheduler> fetchedSchedulers = service.getJobSchedulers(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                0,
                24,
                true
            );
            Integer schedulerCount = service.getJobSchedulersCount(QueuesTestSupport.DEFAULT_QUEUE_NAME);

            // Assert
            Assert.areEqual(2, repaired.total);
            Assert.areEqual(2, repaired.getCount(QueuesTestSupport.DEFAULT_QUEUE_NAME));
            Assert.areEqual(materializedJobId, materializedJob.Id);
            Assert.areEqual(materializedJobId, queueableMaterializedJob.Id);
            Assert.areEqual(true, removed);
            Assert.areEqual(true, canceled);
            Assert.areEqual(schedulerValue, fetched);
            Assert.areEqual(1, fetchedSchedulers.size());
            Assert.areEqual(1, schedulerCount);
            Assert.areEqual(1, schedulerProvider.getCallCount('runJobSchedulerMaintenanceAsSystem'));
            Assert.areEqual(2, schedulerProvider.getCallCount('upsertJobScheduler'));
            Assert.areEqual(1, schedulerProvider.getCallCount('removeJobScheduler'));
            Assert.areEqual(1, schedulerProvider.getCallCount('cancelJobScheduler'));
            Assert.areEqual(1, schedulerProvider.getCallCount('getJobScheduler'));
            Assert.areEqual(1, schedulerProvider.getCallCount('getJobSchedulers'));
            Assert.areEqual(1, schedulerProvider.getCallCount('getJobSchedulersCount'));
        }
    }

    @IsTest
    static void should_enqueue_jobs_and_schedule_worker_when_jobs_are_provided() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c mappedJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            mappedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 401);

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'requireProcessors',
                        new Map<String, Processor__mdt>{ processorDefinition.ProcessorClass__c => processorDefinition }
                    )
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                .withReturn('mapToJob', mappedJob)
                .withReturn('insertJobsAllowingPartialSuccess', new List<Database.SaveResult>());
            QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
                QueuesJobService.class,
                jobServiceProvider
            );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
                QueuesWorkerService.class,
                workerProvider
            );

            QueuesService service = new QueuesService(
                definitionStub,
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                jobServiceStub,
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                processorStub,
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                workerStub
            );

            // Act
            List<Job__c> jobs = service.enqueue(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>()
                    )
                }
            );

            // Assert
            Assert.areEqual(1, jobs.size());
            Assert.areEqual(1, jobServiceProvider.getCallCount('mapToJob'));
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobs'));
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobsAllowingPartialSuccess'));
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
            Assert.areNotEqual(null, jobs[0].Id);
        }
    }

    @IsTest
    static void should_enqueue_queueable_bulk_and_skip_worker_scheduling_jobs() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c queueableJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'requireProcessors',
                        new Map<String, Processor__mdt>{ processorDefinition.ProcessorClass__c => processorDefinition }
                    )
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                .withReturn('mapToJob', queueableJob)
                .withReturn(
                    'insertJobsAllowingPartialSuccess',
                    buildInsertSaveResults(new List<Job__c>{ queueableJob })
                );
            QueueableDispatch__c waitingDispatch = new QueueableDispatch__c(
                Id = QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 704)
            );
            QueuesTestSupport.RecordingStubProvider queueableDispatchProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('createWaitingDispatch', waitingDispatch)
                .withReturn('enqueueDispatcher', QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 705));
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

            QueuesService service = new QueuesService(
                definitionStub,
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                processorStub,
                (QueuesQueueableDispatchService) Test.createStub(
                    QueuesQueueableDispatchService.class,
                    queueableDispatchProvider
                ),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            List<Job__c> jobs = service.enqueue(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>(),
                        new Queues.JobOptions().jobId('invoice:QUEUEABLE-IDEMPOTENT')
                    )
                },
                new Queues.BulkJobOptions().dedupeKey('bulk-1').queueable()
            );

            // Assert
            Assert.areEqual(1, jobs.size());
            Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), jobs[0].ExecutionMode__c);
            Assert.areEqual(1, jobServiceProvider.getCallCount('mapToJob'));
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobs'));
            List<Job__c> insertedJobs = (List<Job__c>) jobServiceProvider.getLastArgs('insertJobs')[0];
            Assert.areEqual(0, insertedJobs.size());
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobsAllowingPartialSuccess'));
            Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
            Assert.areEqual(1, queueableDispatchProvider.getCallCount('createWaitingDispatch'));
            Assert.areEqual('bulk-1', queueableDispatchProvider.getLastArgs('createWaitingDispatch')[1]);
            Assert.areEqual(
                Queues.ExecutionMode.QUEUEABLE_SERIAL,
                queueableDispatchProvider.getLastArgs('createWaitingDispatch')[2]
            );
            List<Job__c> dispatchedJobs = (List<Job__c>) queueableDispatchProvider.getLastArgs(
                'createWaitingDispatch'
            )[3];
            Assert.areEqual(
                new List<String>{ Queues.ExecutionMode.QUEUEABLE_SERIAL.name() },
                toExecutionModeValues(dispatchedJobs)
            );
            Assert.areEqual(new List<Id>{ jobs[0].Id }, QueuesSupport.toIds(dispatchedJobs));
            Assert.areEqual(1, queueableDispatchProvider.getCallCount('enqueueDispatcher'));
            QueueableDispatch__c enqueuedDispatch = (QueueableDispatch__c) queueableDispatchProvider.getLastArgs(
                'enqueueDispatcher'
            )[0];
            Assert.areEqual(waitingDispatch.Id, enqueuedDispatch.Id);
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_enqueue_invocable_bulk_without_worker_queueable_or_inline_claim() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c invocableJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:INVOCABLE-IDEMPOTENT'
            );
            invocableJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            invocableJob.AvailableAt__c = System.now().addMinutes(-1);

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'requireProcessors',
                        new Map<String, Processor__mdt>{ processorDefinition.ProcessorClass__c => processorDefinition }
                    )
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                .withReturn('mapToJob', invocableJob)
                .withReturn(
                    'insertJobsAllowingPartialSuccess',
                    buildInsertSaveResults(new List<Job__c>{ invocableJob })
                );
            QueuesTestSupport.RecordingStubProvider queueableDispatchProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

            QueuesService service = new QueuesService(
                definitionStub,
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                processorStub,
                (QueuesQueueableDispatchService) Test.createStub(
                    QueuesQueueableDispatchService.class,
                    queueableDispatchProvider
                ),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            List<Job__c> jobs = service.enqueue(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>(),
                        new Queues.JobOptions().jobId('invoice:INVOCABLE-IDEMPOTENT')
                    )
                },
                new Queues.BulkJobOptions().invocable()
            );

            // Assert
            Assert.areEqual(1, jobs.size());
            Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), jobs[0].ExecutionMode__c);
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobs'));
            Assert.areEqual(0, ((List<Job__c>) jobServiceProvider.getLastArgs('insertJobs')[0]).size());
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobsAllowingPartialSuccess'));
            Assert.areEqual(0, jobServiceProvider.getCallCount('claimInvocableJobs'));
            Assert.areEqual(0, queueableDispatchProvider.getCallCount('createWaitingDispatch'));
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_enqueue_non_idempotent_invocable_bulk_without_partial_insert() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c invocableJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            invocableJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            invocableJob.AvailableAt__c = System.now().addMinutes(-1);

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'requireProcessors',
                        new Map<String, Processor__mdt>{ processorDefinition.ProcessorClass__c => processorDefinition }
                    )
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                .withReturn('mapToJob', invocableJob)
                .withReturn('insertJobs', new List<Job__c>{ invocableJob });
            QueuesTestSupport.RecordingStubProvider queueableDispatchProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

            QueuesService service = new QueuesService(
                definitionStub,
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                processorStub,
                (QueuesQueueableDispatchService) Test.createStub(
                    QueuesQueueableDispatchService.class,
                    queueableDispatchProvider
                ),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            List<Job__c> jobs = service.enqueue(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>()
                    )
                },
                new Queues.BulkJobOptions().invocable()
            );

            // Assert
            Assert.areEqual(1, jobs.size());
            Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), jobs[0].ExecutionMode__c);
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobs'));
            Assert.areEqual(1, ((List<Job__c>) jobServiceProvider.getLastArgs('insertJobs')[0]).size());
            Assert.areEqual(0, jobServiceProvider.getCallCount('insertJobsAllowingPartialSuccess'));
            Assert.areEqual(0, jobServiceProvider.getCallCount('claimInvocableJobs'));
            Assert.areEqual(0, queueableDispatchProvider.getCallCount('createWaitingDispatch'));
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_return_existing_queueable_job_when_duplicate_insert_races_without_dispatching() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c existingJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:QUEUEABLE-DUP-ONLY'
            );
            existingJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            insert existingJob;
            Job__c mappedJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:QUEUEABLE-DUP-ONLY'
            );
            mappedJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();

            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns(
                    'getJobsByExternalId',
                    new List<Object>{
                        new Map<String, Job__c>(),
                        new Map<String, Job__c>{ 'invoice:queueable-dup-only' => existingJob }
                    }
                )
                .withReturn('mapToJob', mappedJob)
                .withReturn('insertJobsAllowingPartialSuccess', buildInsertSaveResults(new List<Job__c>{ mappedJob }));
            QueuesTestSupport.RecordingStubProvider queueableDispatchProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

            QueuesService service = new QueuesService(
                new QueuesDefinitionService(
                    (QueuesDefinitionRepository) Test.createStub(
                        QueuesDefinitionRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
                    )
                ),
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(
                    (QueuesProcessorRepository) Test.createStub(
                        QueuesProcessorRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
                    )
                ),
                (QueuesQueueableDispatchService) Test.createStub(
                    QueuesQueueableDispatchService.class,
                    queueableDispatchProvider
                ),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            List<Job__c> jobs = service.enqueue(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'invoiceId' => 'INV-QUEUEABLE-DUP-ONLY' },
                        new Queues.JobOptions().jobId('invoice:QUEUEABLE-DUP-ONLY')
                    )
                },
                new Queues.BulkJobOptions().queueable()
            );

            // Assert
            Assert.areEqual(1, jobs.size());
            Assert.areEqual(existingJob.Id, jobs[0].Id);
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobsAllowingPartialSuccess'));
            Assert.areEqual(0, queueableDispatchProvider.getCallCount('createWaitingDispatch'));
            Assert.areEqual(0, queueableDispatchProvider.getCallCount('enqueueDispatcher'));
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_dispatch_only_new_queueable_jobs_when_idempotent_insert_partially_duplicates() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c existingJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:QUEUEABLE-DUP-MIXED'
            );
            existingJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            insert existingJob;
            Job__c duplicateMappedJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:QUEUEABLE-DUP-MIXED'
            );
            duplicateMappedJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            Job__c newMappedJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:QUEUEABLE-NEW-MIXED'
            );
            newMappedJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();

            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns(
                    'getJobsByExternalId',
                    new List<Object>{
                        new Map<String, Job__c>(),
                        new Map<String, Job__c>{ 'invoice:queueable-dup-mixed' => existingJob }
                    }
                )
                .withReturns('mapToJob', new List<Object>{ duplicateMappedJob, newMappedJob })
                .withReturn(
                    'insertJobsAllowingPartialSuccess',
                    buildInsertSaveResults(new List<Job__c>{ duplicateMappedJob, newMappedJob })
                );
            QueueableDispatch__c waitingDispatch = new QueueableDispatch__c(
                Id = QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 713)
            );
            QueuesTestSupport.RecordingStubProvider queueableDispatchProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('createWaitingDispatch', waitingDispatch)
                .withReturn('enqueueDispatcher', QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 714));

            QueuesService service = new QueuesService(
                new QueuesDefinitionService(
                    (QueuesDefinitionRepository) Test.createStub(
                        QueuesDefinitionRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
                    )
                ),
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(
                    (QueuesProcessorRepository) Test.createStub(
                        QueuesProcessorRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
                    )
                ),
                (QueuesQueueableDispatchService) Test.createStub(
                    QueuesQueueableDispatchService.class,
                    queueableDispatchProvider
                ),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                new QueuesWorkerService()
            );

            // Act
            List<Job__c> jobs = service.enqueue(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'mode' => 'duplicate' },
                        new Queues.JobOptions().jobId('invoice:QUEUEABLE-DUP-MIXED')
                    ),
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'mode' => 'new' },
                        new Queues.JobOptions().jobId('invoice:QUEUEABLE-NEW-MIXED')
                    )
                },
                new Queues.BulkJobOptions().queueable()
            );

            // Assert
            Assert.areEqual(2, jobs.size());
            Assert.areEqual(existingJob.Id, jobs[0].Id);
            Assert.areEqual(newMappedJob.Id, jobs[1].Id);
            Assert.areEqual(1, queueableDispatchProvider.getCallCount('createWaitingDispatch'));
            List<Job__c> dispatchedJobs = (List<Job__c>) queueableDispatchProvider.getLastArgs(
                'createWaitingDispatch'
            )[3];
            Assert.areEqual(1, dispatchedJobs.size());
            Assert.areEqual(newMappedJob.Id, dispatchedJobs[0].Id);
            Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
            Assert.areEqual(1, ((List<Job__c>) jobServiceProvider.getLastArgs('updateJobs')[0]).size());
            Assert.areEqual(1, queueableDispatchProvider.getCallCount('enqueueDispatcher'));
        }
    }

    @IsTest
    static void should_rollback_queueable_jobs_when_dispatch_creation_fails_during_enqueue() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c queueableJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 405);
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();

            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                .withReturn('mapToJob', queueableJob);
            QueuesTestSupport.RecordingStubProvider queueableDispatchProvider = new QueuesTestSupport.RecordingStubProvider()
                .withException(
                    'createWaitingDispatch',
                    new Queues.QueueException('Queueable dispatch creation exploded.')
                );

            QueuesService service = new QueuesService(
                new QueuesDefinitionService(
                    (QueuesDefinitionRepository) Test.createStub(
                        QueuesDefinitionRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
                    )
                ),
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(
                    (QueuesProcessorRepository) Test.createStub(
                        QueuesProcessorRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
                    )
                ),
                (QueuesQueueableDispatchService) Test.createStub(
                    QueuesQueueableDispatchService.class,
                    queueableDispatchProvider
                ),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                new QueuesWorkerService()
            );

            String failureMessage;

            // Act
            try {
                service.enqueue(
                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    new List<Queues.JobData>{
                        new Queues.JobData(
                            QueuesTestSupport.DEFAULT_JOB_NAME,
                            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                            new Map<String, Object>{ 'invoiceId' => 'INV-DISPATCH-CREATE-FAIL' }
                        )
                    },
                    new Queues.BulkJobOptions().queueable()
                );
                Assert.fail('Expected queueable dispatch creation failure to throw.');
            } catch (Queues.QueueException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            Assert.areEqual('Queueable dispatch creation exploded.', failureMessage);
            Assert.areEqual(1, queueableDispatchProvider.getCallCount('createWaitingDispatch'));
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobs'));
            Assert.areEqual(0, jobServiceProvider.getCallCount('updateJobs'));
            Assert.areEqual(0, queueableDispatchProvider.getCallCount('enqueueDispatcher'));
        }
    }

    @IsTest
    static void should_record_queue_error_after_rollback_when_queueable_dispatch_enqueue_fails() {
        QueuesTestSupport.RecordingStubProvider queueableDispatchProvider;
        String failureMessage;

        Test.startTest();
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c queueableJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 406);
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            QueueableDispatch__c waitingDispatch = new QueueableDispatch__c(
                Id = QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 706)
            );

            queueableDispatchProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('createWaitingDispatch', waitingDispatch)
                .withException('enqueueDispatcher', new Queues.QueueException('Queueable dispatch enqueue exploded.'));

            QueuesService service = new QueuesService(
                new QueuesDefinitionService(
                    (QueuesDefinitionRepository) Test.createStub(
                        QueuesDefinitionRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
                    )
                ),
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(
                    QueuesJobService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                        .withReturn('mapToJob', queueableJob)
                ),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(
                    (QueuesProcessorRepository) Test.createStub(
                        QueuesProcessorRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
                    )
                ),
                (QueuesQueueableDispatchService) Test.createStub(
                    QueuesQueueableDispatchService.class,
                    queueableDispatchProvider
                ),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                new QueuesWorkerService()
            );

            // Act
            try {
                service.enqueue(
                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    new List<Queues.JobData>{
                        new Queues.JobData(
                            QueuesTestSupport.DEFAULT_JOB_NAME,
                            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                            new Map<String, Object>{ 'invoiceId' => 'INV-DISPATCH-ENQUEUE-FAIL' }
                        )
                    },
                    new Queues.BulkJobOptions().queueable()
                );
                Assert.fail('Expected queueable dispatch enqueue failure to throw.');
            } catch (Queues.QueueException ex) {
                failureMessage = ex.getMessage();
            }
        }
        Test.getEventBus().deliver();
        Test.stopTest();

        // Assert
        Assert.areEqual('Queueable dispatch enqueue exploded.', failureMessage);
        Assert.areEqual(1, queueableDispatchProvider.getCallCount('enqueueDispatcher'));
        QueueError__c error = QueuesTestSupport.queryQueueErrorsAsSystem(QueuesTestSupport.DEFAULT_QUEUE_NAME)[0];
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, error.QueueName__c);
        Assert.areEqual('queueable dispatch', error.Component__c);
        Assert.areEqual('Queueable dispatch enqueue exploded.', error.Message__c);
    }

    @IsTest
    static void should_reject_queueable_jobs_when_native_delay_window_is_exceeded() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesService service = new QueuesService();
            List<Queues.JobData> invalidJobs = new List<Queues.JobData>{
                new Queues.JobData(
                    QueuesTestSupport.DEFAULT_JOB_NAME,
                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                    new Map<String, Object>(),
                    new Queues.JobOptions().delay(11)
                ),
                new Queues.JobData(
                    QueuesTestSupport.DEFAULT_JOB_NAME,
                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                    new Map<String, Object>(),
                    new Queues.JobOptions().availableAt(System.now().addMinutes(11))
                )
            };
            List<String> failureMessages = new List<String>();

            // Act
            for (Queues.JobData invalidJob : invalidJobs) {
                try {
                    service.enqueue(
                        QueuesTestSupport.DEFAULT_QUEUE_NAME,
                        new List<Queues.JobData>{ invalidJob },
                        new Queues.BulkJobOptions().queueable()
                    );
                    Assert.fail('Expected invalid queueable delay to throw.');
                } catch (Queues.QueueException ex) {
                    failureMessages.add(ex.getMessage());
                }
            }

            // Assert
            Assert.areEqual(2, failureMessages.size());
            Assert.areEqual(QueuesQueueableTransportService.nativeQueueableDelayMessage(11), failureMessages[0]);
            Assert.areEqual(QueuesQueueableTransportService.nativeQueueableDelayMessage(11), failureMessages[1]);
        }
    }

    @IsTest
    static void should_reject_existing_job_when_bulk_execution_mode_does_not_match() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c existingWorkerJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:MODE-MISMATCH'
            );

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'requireProcessors',
                        new Map<String, Processor__mdt>{ processorDefinition.ProcessorClass__c => processorDefinition }
                    )
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'getJobsByExternalId',
                    new Map<String, Job__c>{ 'invoice:mode-mismatch' => existingWorkerJob }
                )
                .withReturn('getResolvedJob', existingWorkerJob);
            QueuesTestSupport.RecordingStubProvider queueableDispatchProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

            QueuesService service = new QueuesService(
                definitionStub,
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                processorStub,
                (QueuesQueueableDispatchService) Test.createStub(
                    QueuesQueueableDispatchService.class,
                    queueableDispatchProvider
                ),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            String failureMessage;

            // Act
            try {
                service.enqueue(
                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    new List<Queues.JobData>{
                        new Queues.JobData(
                            QueuesTestSupport.DEFAULT_JOB_NAME,
                            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                            new Map<String, Object>(),
                            new Queues.JobOptions().jobId('invoice:MODE-MISMATCH')
                        )
                    },
                    new Queues.BulkJobOptions().queueable()
                );
                Assert.fail('Expected mode mismatch to throw.');
            } catch (Queues.QueueException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            Assert.areEqual('Existing queue job execution mode does not match bulk execution mode.', failureMessage);
            Assert.areEqual(0, jobServiceProvider.getCallCount('mapToJob'));
            Assert.areEqual(0, queueableDispatchProvider.getCallCount('createWaitingDispatch'));
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_create_waiting_dispatch_for_queueable_jobs_and_ignore_empty_inputs() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesQueueableDispatchService service = new QueuesQueueableDispatchService();
            Job__c queueableJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();

            // Act
            QueueableDispatch__c emptyDispatch = service.createWaitingDispatch(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                null,
                Queues.ExecutionMode.QUEUEABLE_SERIAL,
                new List<Job__c>()
            );
            QueueableDispatch__c createdDispatch = service.createWaitingDispatch(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                null,
                Queues.ExecutionMode.QUEUEABLE_SERIAL,
                new List<Job__c>{ queueableJob }
            );
            insert queueableJob;

            // Assert
            Assert.areEqual(null, emptyDispatch);
            Assert.areNotEqual(null, createdDispatch.Id);
            Assert.areEqual(createdDispatch.Id, queueableJob.QueueableDispatch__c);
            QueueableDispatch__c dispatch = [
                SELECT
                    QueueName__c,
                    DedupeKey__c,
                    Mode__c,
                    State__c,
                    RequestedCount__c,
                    DispatchedCount__c,
                    DispatchDepth__c
                FROM QueueableDispatch__c
                WHERE Id = :createdDispatch.Id
            ];
            Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, dispatch.QueueName__c);
            Assert.areEqual(null, dispatch.DedupeKey__c);
            Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), dispatch.Mode__c);
            Assert.areEqual(Queues.QueueableDispatchState.WAITING.name(), dispatch.State__c);
            Assert.areEqual(1, QueuesSupport.toInteger(dispatch.RequestedCount__c, 0));
            Assert.areEqual(0, QueuesSupport.toInteger(dispatch.DispatchedCount__c, 0));
            Assert.areEqual(0, QueuesSupport.toInteger(dispatch.DispatchDepth__c, 0));
        }
    }

    @IsTest
    static void should_create_and_enqueue_waiting_dispatch_as_system() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            Job__c queueableJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();

            // Act
            QueueableDispatch__c dispatch = service.createWaitingDispatchAsSystem(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                null,
                Queues.ExecutionMode.QUEUEABLE_SERIAL,
                new List<Job__c>{ queueableJob }
            );
            insert queueableJob;
            Id dispatcherAsyncApexJobId = service.enqueueDispatcherAsSystem(dispatch);

            // Assert
            QueueableDispatch__c updatedDispatch = [
                SELECT DispatcherAsyncApexJobId__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Assert.areNotEqual(null, dispatch.Id);
            Assert.areEqual(dispatch.Id, queueableJob.QueueableDispatch__c);
            Assert.areEqual(dispatcherAsyncApexJobId, Id.valueOf(updatedDispatch.DispatcherAsyncApexJobId__c));
            Assert.areEqual(1, service.dispatcherCount);
        }
    }

    @IsTest
    static void should_leave_waiting_dispatch_for_maintenance_when_queueable_submission_budget_is_exhausted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                Mode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            insert dispatch;

            // Act
            QueuesSupport.forcedRemainingQueueableSubmissionSlots = 0;
            Id dispatcherAsyncApexJobId;
            try {
                dispatcherAsyncApexJobId = service.enqueueDispatcherAsSystem(dispatch);
            } finally {
                QueuesSupport.forcedRemainingQueueableSubmissionSlots = null;
            }

            // Assert
            QueueableDispatch__c waitingDispatch = [
                SELECT State__c, DispatcherAsyncApexJobId__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Assert.areEqual(null, dispatcherAsyncApexJobId);
            Assert.areEqual(0, service.dispatcherCount);
            Assert.areEqual(Queues.QueueableDispatchState.WAITING.name(), waitingDispatch.State__c);
            Assert.areEqual(null, waitingDispatch.DispatcherAsyncApexJobId__c);
        }
    }

    @IsTest
    static void should_not_defer_user_dispatch_when_internal_queueable_submission_budget_is_exhausted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                Mode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            insert dispatch;

            // Act
            QueuesSupport.forcedRemainingQueueableSubmissionSlots = 0;
            Id dispatcherAsyncApexJobId;
            try {
                dispatcherAsyncApexJobId = service.enqueueDispatcher(dispatch);
            } finally {
                QueuesSupport.forcedRemainingQueueableSubmissionSlots = null;
            }

            // Assert
            Assert.areNotEqual(null, dispatcherAsyncApexJobId);
            Assert.areEqual(1, service.dispatcherCount);
        }
    }

    @IsTest
    static void should_enqueue_dispatcher_and_mark_dispatch_active_when_queueable_starts() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            Job__c queueableJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            QueueableDispatch__c dispatch = service.createWaitingDispatch(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                null,
                Queues.ExecutionMode.QUEUEABLE_SERIAL,
                new List<Job__c>{ queueableJob }
            );
            insert queueableJob;

            // Act
            Id emptyDispatchAsyncApexJobId = service.enqueueDispatcher(null);
            Id dispatcherAsyncApexJobId = service.enqueueDispatcher(dispatch);
            service.startDispatchAsSystem(dispatch.Id, dispatcherAsyncApexJobId);

            // Assert
            QueueableDispatch__c activeDispatch = [
                SELECT State__c, DispatcherAsyncApexJobId__c, StartedAt__c, FinishedAt__c, DispatchedCount__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Job__c dispatchedJob = [
                SELECT State__c, AsyncApexJobId__c, QueueableDispatchInitiatedAt__c
                FROM Job__c
                WHERE Id = :queueableJob.Id
            ];
            Assert.areEqual(null, emptyDispatchAsyncApexJobId);
            Assert.areNotEqual(null, dispatcherAsyncApexJobId);
            Assert.areEqual(1, service.dispatcherCount);
            Assert.areEqual(1, service.executorCount);
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), activeDispatch.State__c);
            Assert.areEqual(dispatcherAsyncApexJobId, Id.valueOf(activeDispatch.DispatcherAsyncApexJobId__c));
            Assert.areNotEqual(null, activeDispatch.StartedAt__c);
            Assert.areNotEqual(null, activeDispatch.FinishedAt__c);
            Assert.areEqual(1, QueuesSupport.toInteger(activeDispatch.DispatchedCount__c, 0));
            Assert.areEqual(Queues.State.ACTIVE.name(), dispatchedJob.State__c);
            Assert.areNotEqual(null, dispatchedJob.AsyncApexJobId__c);
            Assert.areNotEqual(null, dispatchedJob.QueueableDispatchInitiatedAt__c);

            QueueableDispatch__c skippedDispatch = service.startDispatchAsSystem(
                dispatch.Id,
                QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 706)
            );
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), skippedDispatch.State__c);
        }
    }

    @IsTest
    static void should_bridge_initial_dispatcher_from_future_when_stack_is_exhausted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                Mode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            insert dispatch;

            // Act
            QueuesQueueableDispatchService.forceFutureBridge = true;
            Id deferredAsyncApexJobId;
            try {
                deferredAsyncApexJobId = service.enqueueDispatcher(dispatch);
            } finally {
                QueuesQueueableDispatchService.forceFutureBridge = false;
            }
            Id asyncApexJobId = service.submitDispatcherFromFuture(dispatch.Id);
            Id duplicateAsyncApexJobId = service.submitDispatcherFromFuture(dispatch.Id);
            Id missingAsyncApexJobId = service.submitDispatcherFromFuture(
                QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 722)
            );

            // Assert
            QueueableDispatch__c updatedDispatch = [
                SELECT DispatcherAsyncApexJobId__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Assert.areEqual(null, deferredAsyncApexJobId);
            Assert.areEqual(dispatch.Id, service.lastFutureBridgeDispatchId);
            Assert.areEqual(1, service.futureBridgeCount);
            Assert.areEqual(1, service.dispatcherCount);
            Assert.areEqual(asyncApexJobId, Id.valueOf(updatedDispatch.DispatcherAsyncApexJobId__c));
            Assert.areEqual(null, duplicateAsyncApexJobId);
            Assert.areEqual(null, missingAsyncApexJobId);
        }
    }

    @IsTest
    static void should_enqueue_initial_dispatcher_from_future_bridge() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                Mode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
                RequestedCount__c = 0,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            insert dispatch;

            // Act
            Id deferredAsyncApexJobId;
            Test.startTest();
            QueuesQueueableDispatchService.forceFutureBridge = true;
            try {
                deferredAsyncApexJobId = new QueuesQueueableDispatchService().enqueueDispatcher(dispatch);
            } finally {
                QueuesQueueableDispatchService.forceFutureBridge = false;
            }
            Test.stopTest();

            // Assert
            QueueableDispatch__c updatedDispatch = [
                SELECT DispatcherAsyncApexJobId__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Assert.areEqual(null, deferredAsyncApexJobId);
            Assert.areNotEqual(null, updatedDispatch.DispatcherAsyncApexJobId__c);
        }
    }

    @IsTest
    static void should_stop_active_dispatcher_before_executor_submission_when_stack_is_exhausted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            Job__c queueableJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            QueueableDispatch__c dispatch = service.createWaitingDispatch(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                null,
                Queues.ExecutionMode.QUEUEABLE_SERIAL,
                new List<Job__c>{ queueableJob }
            );
            insert queueableJob;

            // Act
            QueuesQueueableDispatchService.forceFutureBridge = true;
            QueueableDispatch__c activeDispatch;
            try {
                activeDispatch = service.startDispatchAsSystem(
                    dispatch.Id,
                    QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 723)
                );
            } finally {
                QueuesQueueableDispatchService.forceFutureBridge = false;
            }

            // Assert
            Assert.areEqual(Queues.QueueableDispatchState.ACTIVE.name(), activeDispatch.State__c);
            Assert.areNotEqual(null, activeDispatch.StartedAt__c);
            Assert.areEqual(0, QueuesSupport.toInteger(activeDispatch.DispatchedCount__c, 0));
            Assert.areEqual(0, service.executorCount);
        }
    }

    @IsTest
    static void should_enqueue_initial_dispatcher_with_dedupe_key_when_provided() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                DedupeKey__c = 'bulk-1',
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            insert dispatch;

            // Act
            Id dispatcherAsyncApexJobId = service.enqueueDispatcher(dispatch);

            // Assert
            QueueableDispatch__c updatedDispatch = [
                SELECT DispatcherAsyncApexJobId__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Assert.areNotEqual(null, dispatcherAsyncApexJobId);
            Assert.areEqual(dispatcherAsyncApexJobId, Id.valueOf(updatedDispatch.DispatcherAsyncApexJobId__c));
            Assert.areEqual(1, service.dispatcherCount);
            Assert.areEqual(dispatch.Id, service.lastDispatcherDispatchId);
            Assert.areEqual(true, service.lastDispatcherUseInitialStackDepth);
            Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, service.lastDispatcherQueueName);
            Assert.areEqual('bulk-1', service.lastDispatcherDedupeKey);
        }
    }

    @IsTest
    static void should_submit_native_initial_dispatcher_with_dedupe_signature_when_key_is_provided() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String dedupeKey = System.now().getTime().toString();
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                DedupeKey__c = dedupeKey,
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                RequestedCount__c = 0,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            insert dispatch;

            // Act
            Test.startTest();
            Id dispatcherAsyncApexJobId = new QueuesQueueableDispatchService().enqueueDispatcher(dispatch);
            Test.stopTest();

            // Assert
            QueueableDispatch__c completedDispatch = [
                SELECT State__c, DispatcherAsyncApexJobId__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Assert.areNotEqual(null, dispatcherAsyncApexJobId);
            Assert.areEqual(dispatcherAsyncApexJobId, Id.valueOf(completedDispatch.DispatcherAsyncApexJobId__c));
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), completedDispatch.State__c);
        }
    }

    @IsTest
    static void should_submit_native_initial_dispatcher_with_compressed_dedupe_signature_when_key_is_long() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                DedupeKey__c = 'bulk-key-that-is-longer-than-thirty-two-bytes',
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                RequestedCount__c = 0,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            insert dispatch;

            // Act
            Test.startTest();
            Id dispatcherAsyncApexJobId = new QueuesQueueableDispatchService().enqueueDispatcher(dispatch);
            Test.stopTest();

            // Assert
            QueueableDispatch__c completedDispatch = [
                SELECT State__c, DispatcherAsyncApexJobId__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Assert.areNotEqual(null, dispatcherAsyncApexJobId);
            Assert.areEqual(dispatcherAsyncApexJobId, Id.valueOf(completedDispatch.DispatcherAsyncApexJobId__c));
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), completedDispatch.State__c);
        }
    }

    @IsTest
    static void should_enqueue_executor_with_native_delay_when_next_dispatch_job_is_delayed() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            Job__c delayedJob = QueuesTestSupport.job(Queues.State.DELAYED.name());
            delayedJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            delayedJob.AvailableAt__c = System.now().addMinutes(5);
            QueueableDispatch__c dispatch = service.createWaitingDispatch(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                null,
                Queues.ExecutionMode.QUEUEABLE_SERIAL,
                new List<Job__c>{ delayedJob }
            );
            insert delayedJob;

            // Act
            service.startDispatchAsSystem(dispatch.Id, QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 715));

            // Assert
            QueueableDispatch__c completedDispatch = [
                SELECT State__c, DispatchedCount__c, FinishedAt__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Job__c dispatchedJob = [
                SELECT State__c, AsyncApexJobId__c, QueueableDispatchInitiatedAt__c
                FROM Job__c
                WHERE Id = :delayedJob.Id
            ];
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), completedDispatch.State__c);
            Assert.areEqual(1, QueuesSupport.toInteger(completedDispatch.DispatchedCount__c, 0));
            Assert.areNotEqual(null, completedDispatch.FinishedAt__c);
            Assert.areEqual(Queues.State.ACTIVE.name(), dispatchedJob.State__c);
            Assert.areNotEqual(null, dispatchedJob.AsyncApexJobId__c);
            Assert.areNotEqual(null, dispatchedJob.QueueableDispatchInitiatedAt__c);
            Assert.areEqual(delayedJob.Id, service.lastExecutorJobId);
            Assert.areEqual(5, service.lastExecutorDelayMinutes);
        }
    }

    @IsTest
    static void should_surface_executor_enqueue_failure_when_dispatching_job() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            service.executorException = new Queues.QueueException('executor limit');
            Job__c queueableJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            QueueableDispatch__c dispatch = service.createWaitingDispatch(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                'bulk-1',
                Queues.ExecutionMode.QUEUEABLE_SERIAL,
                new List<Job__c>{ queueableJob }
            );
            insert queueableJob;

            // Act
            service.startDispatchAsSystem(dispatch.Id, QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 717));

            // Assert
            QueueableDispatch__c completedDispatch = [
                SELECT State__c, DispatchedCount__c, FinishedAt__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Job__c failedJob = [
                SELECT State__c, FailedReason__c, AsyncApexJobId__c
                FROM Job__c
                WHERE Id = :queueableJob.Id
            ];
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), completedDispatch.State__c);
            Assert.areEqual(1, QueuesSupport.toInteger(completedDispatch.DispatchedCount__c, 0));
            Assert.areNotEqual(null, completedDispatch.FinishedAt__c);
            Assert.areEqual(Queues.State.FAILED.name(), failedJob.State__c);
            Assert.areEqual('executor limit', failedJob.FailedReason__c);
            Assert.areEqual(null, failedJob.AsyncApexJobId__c);
        }
    }

    @IsTest
    static void should_not_dispatch_queueable_retry_jobs_that_were_already_initiated() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.ACTIVE.name(),
                RequestedCount__c = 2,
                DispatchedCount__c = 1,
                DispatchDepth__c = 0,
                StartedAt__c = System.now()
            );
            insert dispatch;

            Job__c retryJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            retryJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            retryJob.QueueableDispatch__c = dispatch.Id;
            retryJob.QueueableDispatchInitiatedAt__c = System.now().addMinutes(-1);
            retryJob.Priority__c = -1;

            Job__c originalJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            originalJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            originalJob.QueueableDispatch__c = dispatch.Id;
            originalJob.Priority__c = 0;
            insert new List<Job__c>{ retryJob, originalJob };

            // Act
            service.startDispatchAsSystem(dispatch.Id, QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 716));

            // Assert
            QueueableDispatch__c completedDispatch = [
                SELECT State__c, DispatchedCount__c, FinishedAt__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Set<Id> jobIds = new Set<Id>{ retryJob.Id, originalJob.Id };
            List<Job__c> jobs = [
                SELECT State__c, AsyncApexJobId__c, QueueableDispatchInitiatedAt__c
                FROM Job__c
                WHERE Id IN :jobIds
                ORDER BY Priority__c ASC
            ];
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), completedDispatch.State__c);
            Assert.areEqual(2, QueuesSupport.toInteger(completedDispatch.DispatchedCount__c, 0));
            Assert.areNotEqual(null, completedDispatch.FinishedAt__c);
            Assert.areEqual(1, service.executorCount);
            Assert.areEqual(originalJob.Id, service.lastExecutorJobId);
            Assert.areEqual(Queues.State.WAITING.name(), jobs[0].State__c);
            Assert.areEqual(null, jobs[0].AsyncApexJobId__c);
            Assert.areNotEqual(null, jobs[0].QueueableDispatchInitiatedAt__c);
            Assert.areEqual(Queues.State.ACTIVE.name(), jobs[1].State__c);
            Assert.areNotEqual(null, jobs[1].AsyncApexJobId__c);
            Assert.areNotEqual(null, jobs[1].QueueableDispatchInitiatedAt__c);
        }
    }

    @IsTest
    static void should_fail_only_invalid_queueable_dispatch_job_when_delay_is_mutated_beyond_native_window() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            Datetime currentNow = System.now();

            Job__c invalidDelayJob = QueuesTestSupport.job(Queues.State.DELAYED.name());
            invalidDelayJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            invalidDelayJob.AvailableAt__c = currentNow.addMinutes(12);
            invalidDelayJob.Priority__c = 0;

            Job__c validJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            validJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            validJob.AvailableAt__c = currentNow;
            validJob.Priority__c = 1;

            QueueableDispatch__c dispatch = service.createWaitingDispatch(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                null,
                Queues.ExecutionMode.QUEUEABLE_SERIAL,
                new List<Job__c>{ invalidDelayJob, validJob }
            );
            insert new List<Job__c>{ invalidDelayJob, validJob };

            // Act
            QueueableDispatch__c afterInvalidDelayJob = service.startDispatchAsSystem(
                dispatch.Id,
                QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 713)
            );
            QueueableDispatch__c afterValidJob = service.startDispatchAsSystem(
                dispatch.Id,
                QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 714)
            );

            // Assert
            QueueableDispatch__c completedDispatch = [
                SELECT State__c, DispatchedCount__c, FailedReason__c, FinishedAt__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Set<Id> jobIds = new Set<Id>{ invalidDelayJob.Id, validJob.Id };
            List<Job__c> resolvedJobs = [
                SELECT State__c, FailedReason__c, AsyncApexJobId__c, QueueableDispatchInitiatedAt__c
                FROM Job__c
                WHERE Id IN :jobIds
                ORDER BY Priority__c ASC
            ];
            Assert.areEqual(Queues.QueueableDispatchState.ACTIVE.name(), afterInvalidDelayJob.State__c);
            Assert.areEqual(1, QueuesSupport.toInteger(afterInvalidDelayJob.DispatchedCount__c, 0));
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), afterValidJob.State__c);
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), completedDispatch.State__c);
            Assert.areEqual(2, QueuesSupport.toInteger(completedDispatch.DispatchedCount__c, 0));
            Assert.areEqual(null, completedDispatch.FailedReason__c);
            Assert.areNotEqual(null, completedDispatch.FinishedAt__c);
            Assert.areEqual(Queues.State.FAILED.name(), resolvedJobs[0].State__c);
            Assert.areEqual(
                QueuesQueueableTransportService.nativeQueueableDelayMessage(12),
                resolvedJobs[0].FailedReason__c
            );
            Assert.areEqual(null, resolvedJobs[0].AsyncApexJobId__c);
            Assert.areEqual(null, resolvedJobs[0].QueueableDispatchInitiatedAt__c);
            Assert.areEqual(Queues.State.ACTIVE.name(), resolvedJobs[1].State__c);
            Assert.areNotEqual(null, resolvedJobs[1].AsyncApexJobId__c);
            Assert.areNotEqual(null, resolvedJobs[1].QueueableDispatchInitiatedAt__c);
            Assert.areEqual(1, service.executorCount);
            Assert.areEqual(validJob.Id, service.lastExecutorJobId);
        }
    }

    @IsTest
    static void should_leave_or_complete_active_dispatch_when_no_due_job_exists() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesQueueableDispatchService service = new QueuesQueueableDispatchService();
            QueueableDispatch__c pendingDispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.ACTIVE.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0,
                StartedAt__c = System.now()
            );
            QueueableDispatch__c readyToCompleteDispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.ACTIVE.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 1,
                DispatchDepth__c = 0,
                StartedAt__c = System.now()
            );
            insert new List<QueueableDispatch__c>{ pendingDispatch, readyToCompleteDispatch };

            // Act
            QueueableDispatch__c pendingResult = service.startDispatchAsSystem(
                pendingDispatch.Id,
                QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 707)
            );
            QueueableDispatch__c completedResult = service.startDispatchAsSystem(
                readyToCompleteDispatch.Id,
                QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 708)
            );
            QueueableDispatch__c missingResult = service.startDispatchAsSystem(
                QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 709),
                QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 710)
            );

            // Assert
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), pendingResult.State__c);
            Assert.areNotEqual(null, pendingResult.FinishedAt__c);
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), completedResult.State__c);
            Assert.areNotEqual(null, completedResult.FinishedAt__c);
            Assert.areEqual(null, missingResult);
        }
    }

    @IsTest
    static void should_enqueue_next_dispatcher_from_finalizer_when_due_jobs_remain() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            List<Job__c> dispatchJobs = new List<Job__c>();
            for (Integer index = 0; index < 2; index++) {
                Job__c job = QueuesTestSupport.job(Queues.State.WAITING.name());
                job.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
                job.Priority__c = index;
                dispatchJobs.add(job);
            }
            Job__c firstJob = dispatchJobs[0];
            Job__c remainingJob = dispatchJobs[1];
            QueueableDispatch__c dispatch = service.createWaitingDispatch(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                null,
                Queues.ExecutionMode.QUEUEABLE_SERIAL,
                dispatchJobs
            );
            insert dispatchJobs;

            // Act
            Id dispatcherAsyncApexJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 718);
            service.startDispatchAsSystem(dispatch.Id, dispatcherAsyncApexJobId);
            service.continueDispatch(dispatch.Id, false, true);

            // Assert
            QueueableDispatch__c continuedDispatch = [
                SELECT State__c, DispatcherAsyncApexJobId__c, DispatchedCount__c, DispatchDepth__c, FinishedAt__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Set<Id> jobIds = new Set<Id>{ firstJob.Id, remainingJob.Id };
            List<Job__c> jobs = [
                SELECT State__c
                FROM Job__c
                WHERE Id IN :jobIds
                ORDER BY Priority__c ASC
            ];
            Assert.isTrue(
                new Set<String>{
                        Queues.QueueableDispatchState.ACTIVE.name(),
                        Queues.QueueableDispatchState.COMPLETED.name()
                    }
                    .contains(continuedDispatch.State__c)
            );
            Assert.areEqual(1, QueuesSupport.toInteger(continuedDispatch.DispatchedCount__c, 0));
            Assert.areEqual(1, QueuesSupport.toInteger(continuedDispatch.DispatchDepth__c, 0));
            Assert.areNotEqual(dispatcherAsyncApexJobId, Id.valueOf(continuedDispatch.DispatcherAsyncApexJobId__c));
            Assert.areEqual(1, service.dispatcherCount);
            Assert.areEqual(1, service.executorCount);
            Assert.areEqual(Queues.State.ACTIVE.name(), jobs[0].State__c);
            Assert.areEqual(Queues.State.WAITING.name(), jobs[1].State__c);
        }
    }

    @IsTest
    static void should_dispatch_concurrent_queueable_jobs_through_future_bridge_fanout() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            List<Job__c> dispatchJobs = new List<Job__c>();
            Integer futureCallLimit = Limits.getLimitFutureCalls();
            for (Integer index = 0; index < futureCallLimit + 1; index++) {
                Job__c job = QueuesTestSupport.job(Queues.State.WAITING.name());
                job.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
                job.Priority__c = index;
                dispatchJobs.add(job);
            }
            Job__c firstJob = dispatchJobs[0];
            Job__c remainingJob = dispatchJobs[futureCallLimit];
            QueueableDispatch__c dispatch = service.createWaitingDispatch(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                null,
                Queues.ExecutionMode.QUEUEABLE_CONCURRENT,
                dispatchJobs
            );
            insert dispatchJobs;

            // Act
            service.startDispatchAsSystem(dispatch.Id, QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 718));

            // Assert
            QueueableDispatch__c activeDispatch = [
                SELECT State__c, DispatchedCount__c, DispatchDepth__c, FinishedAt__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Set<Id> jobIds = new Set<Id>{ firstJob.Id, remainingJob.Id };
            List<Job__c> jobs = [
                SELECT State__c, QueueableDispatchInitiatedAt__c
                FROM Job__c
                WHERE Id IN :jobIds
                ORDER BY Priority__c ASC
            ];
            Assert.areEqual(Queues.QueueableDispatchState.ACTIVE.name(), activeDispatch.State__c);
            Assert.areEqual(futureCallLimit, QueuesSupport.toInteger(activeDispatch.DispatchedCount__c, 0));
            Assert.areEqual(0, QueuesSupport.toInteger(activeDispatch.DispatchDepth__c, 0));
            Assert.areEqual(null, activeDispatch.FinishedAt__c);
            Assert.areEqual(futureCallLimit, service.executorCount);
            Assert.areEqual(Queues.State.ACTIVE.name(), jobs[0].State__c);
            Assert.areNotEqual(null, jobs[0].QueueableDispatchInitiatedAt__c);
            Assert.areEqual(Queues.State.WAITING.name(), jobs[1].State__c);
            Assert.areEqual(null, jobs[1].QueueableDispatchInitiatedAt__c);
        }
    }

    @IsTest
    static void should_complete_empty_concurrent_dispatch() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesQueueableDispatchService service = new QueuesQueueableDispatchService();
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                Mode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(),
                State__c = Queues.QueueableDispatchState.ACTIVE.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 1,
                DispatchDepth__c = 0,
                StartedAt__c = System.now()
            );
            insert dispatch;

            // Act
            QueueableDispatch__c completedDispatch = service.startDispatchAsSystem(
                dispatch.Id,
                QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 722)
            );

            // Assert
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), completedDispatch.State__c);
            Assert.areNotEqual(null, completedDispatch.FinishedAt__c);
        }
    }

    @IsTest
    static void should_resolve_concurrent_dispatch_job_failures_without_blocking_other_jobs() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            Datetime currentNow = System.now();

            Job__c invalidDelayJob = QueuesTestSupport.job(Queues.State.DELAYED.name());
            invalidDelayJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            invalidDelayJob.AvailableAt__c = currentNow.addMinutes(12);
            invalidDelayJob.Priority__c = 0;

            Job__c enqueueFailureJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            enqueueFailureJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            enqueueFailureJob.Priority__c = 1;

            Job__c validJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            validJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            validJob.Priority__c = 2;

            QueueableDispatch__c dispatch = service.createWaitingDispatch(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                null,
                Queues.ExecutionMode.QUEUEABLE_CONCURRENT,
                new List<Job__c>{ invalidDelayJob, enqueueFailureJob, validJob }
            );
            insert new List<Job__c>{ invalidDelayJob, enqueueFailureJob, validJob };
            service.executorException = new Queues.QueueException('future bridge failed');
            service.executorExceptionJobIds.add(enqueueFailureJob.Id);

            // Act
            service.startDispatchAsSystem(dispatch.Id, QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 719));

            // Assert
            QueueableDispatch__c completedDispatch = [
                SELECT State__c, DispatchedCount__c, FinishedAt__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Set<Id> jobIds = new Set<Id>{ invalidDelayJob.Id, enqueueFailureJob.Id, validJob.Id };
            List<Job__c> jobs = [
                SELECT State__c, FailedReason__c, QueueableDispatchInitiatedAt__c
                FROM Job__c
                WHERE Id IN :jobIds
                ORDER BY Priority__c ASC
            ];
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), completedDispatch.State__c);
            Assert.areEqual(3, QueuesSupport.toInteger(completedDispatch.DispatchedCount__c, 0));
            Assert.areNotEqual(null, completedDispatch.FinishedAt__c);
            Assert.areEqual(Queues.State.FAILED.name(), jobs[0].State__c);
            Assert.areEqual(QueuesQueueableTransportService.nativeQueueableDelayMessage(12), jobs[0].FailedReason__c);
            Assert.areEqual(null, jobs[0].QueueableDispatchInitiatedAt__c);
            Assert.areEqual(Queues.State.FAILED.name(), jobs[1].State__c);
            Assert.areEqual('future bridge failed', jobs[1].FailedReason__c);
            Assert.areEqual(null, jobs[1].QueueableDispatchInitiatedAt__c);
            Assert.areEqual(Queues.State.ACTIVE.name(), jobs[2].State__c);
            Assert.areNotEqual(null, jobs[2].QueueableDispatchInitiatedAt__c);
        }
    }

    @IsTest
    static void should_submit_executor_from_future_bridge_paths() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();

            Job__c invalidDelayJob = QueuesTestSupport.job(Queues.State.DELAYED.name());
            invalidDelayJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            invalidDelayJob.AvailableAt__c = System.now().addMinutes(12);

            Job__c enqueueFailureJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            enqueueFailureJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();

            Job__c pausedJob = QueuesTestSupport.job(Queues.State.PAUSED.name());
            pausedJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            pausedJob.QueueableDispatchInitiatedAt__c = System.now().addMinutes(-1);

            Job__c workerJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            workerJob.ExecutionMode__c = Queues.ExecutionMode.WORKER.name();

            Job__c validJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            validJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            insert new List<Job__c>{ invalidDelayJob, enqueueFailureJob, pausedJob, workerJob, validJob };
            service.executorException = new Queues.QueueException('executor failed from future');
            service.executorExceptionJobIds.add(enqueueFailureJob.Id);

            // Act
            Test.startTest();
            QueuesQueueableDispatchService.enqueueExecutorFromFuture(null);
            Test.stopTest();
            Job__c missingResult = service.submitExecutorFromFuture(QueuesTestSupport.fakeId(Job__c.SObjectType, 720));
            Job__c invalidDelayResult = service.submitExecutorFromFuture(invalidDelayJob.Id);
            Job__c enqueueFailureResult = service.submitExecutorFromFuture(enqueueFailureJob.Id);
            Job__c pausedResult = service.submitExecutorFromFuture(pausedJob.Id);
            Job__c workerResult = service.submitExecutorFromFuture(workerJob.Id);
            service.executorException = null;
            Job__c validResult = service.submitExecutorFromFuture(validJob.Id);

            // Assert
            Assert.areEqual(null, missingResult);
            Assert.areEqual(Queues.State.FAILED.name(), invalidDelayResult.State__c);
            Assert.areEqual(
                QueuesQueueableTransportService.nativeQueueableDelayMessage(12),
                invalidDelayResult.FailedReason__c
            );
            Assert.areEqual(Queues.State.FAILED.name(), enqueueFailureResult.State__c);
            Assert.areEqual('executor failed from future', enqueueFailureResult.FailedReason__c);
            Assert.areEqual(Queues.State.PAUSED.name(), pausedResult.State__c);
            Assert.areEqual(null, pausedResult.AsyncApexJobId__c);
            Assert.areNotEqual(null, pausedResult.QueueableDispatchInitiatedAt__c);
            Assert.areEqual(Queues.State.WAITING.name(), workerResult.State__c);
            Assert.areEqual(null, workerResult.AsyncApexJobId__c);
            Assert.areEqual(Queues.State.ACTIVE.name(), validResult.State__c);
            Assert.areNotEqual(null, validResult.QueueableDispatchInitiatedAt__c);
            Assert.areEqual(2, service.executorCount);
        }
    }

    @IsTest
    static void should_use_default_future_bridge_for_concurrent_dispatch() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesQueueableDispatchService service = new QueuesQueueableDispatchService();
            Job__c queueableJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            QueueableDispatch__c dispatch = service.createWaitingDispatch(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                null,
                Queues.ExecutionMode.QUEUEABLE_CONCURRENT,
                new List<Job__c>{ queueableJob }
            );
            insert queueableJob;

            // Act
            Test.startTest();
            service.startDispatchAsSystem(dispatch.Id, QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 721));
            Test.stopTest();

            // Assert
            Job__c dispatchedJob = [
                SELECT State__c, QueueableDispatchInitiatedAt__c
                FROM Job__c
                WHERE Id = :queueableJob.Id
            ];
            Assert.areNotEqual(null, dispatchedJob.QueueableDispatchInitiatedAt__c);
            Assert.isTrue(
                new Set<String>{
                        Queues.State.WAITING.name(),
                        Queues.State.ACTIVE.name(),
                        Queues.State.COMPLETED.name(),
                        Queues.State.FAILED.name()
                    }
                    .contains(dispatchedJob.State__c)
            );
        }
    }

    @IsTest
    static void should_continue_dispatch_from_future_bridge_when_stack_is_exhausted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesQueueableDispatchService service = new QueuesQueueableDispatchService();
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.ACTIVE.name(),
                RequestedCount__c = 2,
                DispatchedCount__c = 1,
                DispatchDepth__c = 0,
                StartedAt__c = System.now()
            );
            insert dispatch;
            Job__c remainingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            remainingJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            remainingJob.QueueableDispatch__c = dispatch.Id;
            insert remainingJob;

            // Act
            QueuesQueueableDispatchService.forceFutureBridge = true;
            Test.startTest();
            QueuesQueueableDispatchService.enqueueDispatcherFromFuture(null);
            QueuesQueueableDispatchService.continueDispatchFromFuture(null);
            QueueableDispatch__c bridgedDispatch = service.continueDispatch(dispatch.Id);
            Test.stopTest();
            QueuesQueueableDispatchService.forceFutureBridge = false;

            // Assert
            QueueableDispatch__c continuedDispatch = [
                SELECT State__c, DispatcherAsyncApexJobId__c, DispatchedCount__c, DispatchDepth__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Assert.areEqual(Queues.QueueableDispatchState.ACTIVE.name(), bridgedDispatch.State__c);
            Assert.isTrue(
                new Set<String>{
                        Queues.QueueableDispatchState.ACTIVE.name(),
                        Queues.QueueableDispatchState.COMPLETED.name()
                    }
                    .contains(continuedDispatch.State__c)
            );
            Assert.isTrue(QueuesSupport.toInteger(continuedDispatch.DispatchedCount__c, 0) >= 1);
            Assert.isTrue(QueuesSupport.toInteger(continuedDispatch.DispatchDepth__c, 0) >= 1);
            Assert.areNotEqual(null, continuedDispatch.DispatcherAsyncApexJobId__c);
        }
    }

    @IsTest
    static void should_chain_dispatcher_without_reapplying_stack_options_from_queueable_context() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.ACTIVE.name(),
                RequestedCount__c = 2,
                DispatchedCount__c = 1,
                DispatchDepth__c = 0
            );
            insert dispatch;

            Job__c remainingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            remainingJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            remainingJob.QueueableDispatch__c = dispatch.Id;
            insert remainingJob;

            // Act
            AsyncOptions options = new AsyncOptions();
            options.maximumQueueableStackDepth = 5;
            Test.startTest();
            System.enqueueJob(new ContextDispatchProbe(dispatch.Id), options);
            Test.stopTest();

            // Assert
            QueueableDispatch__c updatedDispatch = [
                SELECT State__c, DispatcherAsyncApexJobId__c, DispatchDepth__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatch.Id
            ];
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), updatedDispatch.State__c);
        }
    }

    @IsTest
    static void should_handle_dispatch_continuation_terminal_and_empty_paths() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesQueueableDispatchService service = new QueuesQueueableDispatchService();
            QueueableDispatch__c completedDispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.COMPLETED.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 1,
                DispatchDepth__c = 0
            );
            QueueableDispatch__c readyToCompleteDispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.ACTIVE.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 1,
                DispatchDepth__c = 0
            );
            QueueableDispatch__c pendingWithoutDueJobs = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.ACTIVE.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            insert new List<QueueableDispatch__c>{ completedDispatch, readyToCompleteDispatch, pendingWithoutDueJobs };

            // Act
            QueueableDispatch__c completedResult = service.continueDispatch(completedDispatch.Id, false, true);
            QueueableDispatch__c readyToCompleteResult = service.continueDispatch(
                readyToCompleteDispatch.Id,
                false,
                true
            );
            QueueableDispatch__c pendingResult = service.continueDispatch(pendingWithoutDueJobs.Id, false, true);
            QueueableDispatch__c missingResult = service.continueDispatch(
                QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 711),
                false,
                true
            );

            // Assert
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), completedResult.State__c);
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), readyToCompleteResult.State__c);
            Assert.areNotEqual(null, readyToCompleteResult.FinishedAt__c);
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), pendingResult.State__c);
            Assert.areNotEqual(null, pendingResult.FinishedAt__c);
            Assert.areEqual(null, missingResult);
        }
    }

    @IsTest
    static void should_recover_stale_queueable_dispatches() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queueable-dispatch-recover';
            Datetime currentNow = System.now();
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            QueueableDispatch__c waitingDispatch = new QueueableDispatch__c(
                QueueName__c = queueName,
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                Mode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0,
                DispatcherAsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 881)
            );
            QueueableDispatch__c activeDispatch = new QueueableDispatch__c(
                QueueName__c = queueName,
                State__c = Queues.QueueableDispatchState.ACTIVE.name(),
                Mode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 1,
                DispatchDepth__c = 0,
                StartedAt__c = currentNow.addMinutes(-10),
                DispatcherAsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 882)
            );
            insert new List<QueueableDispatch__c>{ waitingDispatch, activeDispatch };
            Test.setCreatedDate(waitingDispatch.Id, currentNow.addMinutes(-10));

            // Act
            Integer recoveredCount = service.recoverStaleDispatchesAsSystem(queueName, currentNow.addMinutes(-5), 10);

            // Assert
            Map<Id, QueueableDispatch__c> dispatchesById = new Map<Id, QueueableDispatch__c>(
                [
                    SELECT State__c, DispatcherAsyncApexJobId__c, FinishedAt__c
                    FROM QueueableDispatch__c
                    WHERE Id IN :new Set<Id>{ waitingDispatch.Id, activeDispatch.Id }
                ]
            );
            Assert.areEqual(2, recoveredCount);
            Assert.areEqual(waitingDispatch.Id, service.lastDispatcherDispatchId);
            Assert.areEqual(true, service.lastDispatcherUseInitialStackDepth);
            Assert.areEqual(
                Queues.QueueableDispatchState.WAITING.name(),
                dispatchesById.get(waitingDispatch.Id).State__c
            );
            Assert.areEqual(
                QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 851),
                dispatchesById.get(waitingDispatch.Id).DispatcherAsyncApexJobId__c
            );
            Assert.areEqual(
                Queues.QueueableDispatchState.COMPLETED.name(),
                dispatchesById.get(activeDispatch.Id).State__c
            );
            Assert.areNotEqual(null, dispatchesById.get(activeDispatch.Id).FinishedAt__c);
        }
    }

    @IsTest
    static void should_recover_stale_unsubmitted_waiting_dispatch() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queueable-dispatch-recover-unsubmitted';
            Datetime currentNow = System.now();
            TestableQueueableDispatchService service = new TestableQueueableDispatchService();
            QueueableDispatch__c waitingDispatch = new QueueableDispatch__c(
                QueueName__c = queueName,
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                Mode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            insert waitingDispatch;
            Test.setCreatedDate(waitingDispatch.Id, currentNow.addMinutes(-10));

            // Act
            Integer recoveredCount = service.recoverStaleDispatchesAsSystem(queueName, currentNow.addMinutes(-5), 10);

            // Assert
            QueueableDispatch__c recoveredDispatch = [
                SELECT State__c, DispatcherAsyncApexJobId__c
                FROM QueueableDispatch__c
                WHERE Id = :waitingDispatch.Id
            ];
            Assert.areEqual(1, recoveredCount);
            Assert.areEqual(1, service.dispatcherCount);
            Assert.areEqual(waitingDispatch.Id, service.lastDispatcherDispatchId);
            Assert.areEqual(true, service.lastDispatcherUseInitialStackDepth);
            Assert.areEqual(Queues.QueueableDispatchState.WAITING.name(), recoveredDispatch.State__c);
            Assert.areEqual(
                QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 851),
                recoveredDispatch.DispatcherAsyncApexJobId__c
            );
        }
    }

    @IsTest
    static void should_skip_stale_queueable_dispatch_recovery_when_dispatcher_is_active() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queueable-dispatch-recover-active';
            Id asyncApexJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 883);
            QueueableDispatch__c staleDispatch = new QueueableDispatch__c(
                Id = QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 883),
                QueueName__c = queueName,
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                Mode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0,
                DispatcherAsyncApexJobId__c = asyncApexJobId
            );
            QueuesTestSupport.RecordingStubProvider dispatchRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryStaleDispatchesForUpdate', new List<QueueableDispatch__c>{ staleDispatch });
            QueuesTestSupport.RecordingStubProvider asyncApexJobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryAsyncJobsByIds',
                    new List<AsyncApexJob>{
                        QueuesTestSupport.asyncApexJob(
                            asyncApexJobId,
                            'Queued',
                            QueuesQueueableDispatcher.class.getName()
                        )
                    }
                );
            QueuesQueueableDispatchService service = new QueuesQueueableDispatchService(
                (QueuesQueueableDispatchRepository) Test.createStub(
                    QueuesQueueableDispatchRepository.class,
                    dispatchRepositoryProvider
                ),
                new QueuesJobRepository(),
                (QueuesAsyncApexJobRepository) Test.createStub(
                    QueuesAsyncApexJobRepository.class,
                    asyncApexJobRepositoryProvider
                )
            );

            // Act
            Integer recoveredCount = service.recoverStaleDispatches(queueName, System.now().addMinutes(-5), 10);

            // Assert
            Assert.areEqual(0, recoveredCount);
            Assert.areEqual(1, dispatchRepositoryProvider.getCallCount('queryStaleDispatchesForUpdate'));
            Assert.areEqual(0, dispatchRepositoryProvider.getCallCount('updateDispatch'));
        }
    }

    @IsTest
    static void should_fail_active_dispatch_and_ignore_terminal_or_missing_dispatches() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesQueueableDispatchService service = new QueuesQueueableDispatchService();
            QueueableDispatch__c activeDispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.ACTIVE.name(),
                RequestedCount__c = 3,
                DispatchedCount__c = 1,
                DispatchDepth__c = 0
            );
            QueueableDispatch__c completedDispatch = new QueueableDispatch__c(
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.COMPLETED.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 1,
                DispatchDepth__c = 0
            );
            insert new List<QueueableDispatch__c>{ activeDispatch, completedDispatch };
            Datetime now = System.now();
            Job__c waitingQueueableJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'fail-dispatch-waiting'
            );
            waitingQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            waitingQueueableJob.QueueableDispatch__c = activeDispatch.Id;
            Job__c delayedQueueableJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.DELAYED.name(),
                'fail-dispatch-delayed'
            );
            delayedQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            delayedQueueableJob.QueueableDispatch__c = activeDispatch.Id;
            delayedQueueableJob.AvailableAt__c = now.addMinutes(10);
            Job__c initiatedQueueableJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'fail-dispatch-initiated'
            );
            initiatedQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            initiatedQueueableJob.QueueableDispatch__c = activeDispatch.Id;
            initiatedQueueableJob.QueueableDispatchInitiatedAt__c = now.addMinutes(-1);
            Job__c workerJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'fail-dispatch-worker'
            );
            workerJob.QueueableDispatch__c = activeDispatch.Id;
            insert new List<Job__c>{ waitingQueueableJob, delayedQueueableJob, initiatedQueueableJob, workerJob };

            // Act
            QueueableDispatch__c failedDispatch = service.failDispatchAsSystem(
                activeDispatch.Id,
                'dispatcher exploded'
            );
            QueueableDispatch__c ignoredDispatch = service.failDispatchAsSystem(completedDispatch.Id, 'ignored');
            QueueableDispatch__c missingDispatch = service.failDispatchAsSystem(
                QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 712),
                'missing'
            );

            // Assert
            Assert.areEqual(Queues.QueueableDispatchState.FAILED.name(), failedDispatch.State__c);
            Assert.areEqual('dispatcher exploded', failedDispatch.FailedReason__c);
            Assert.areNotEqual(null, failedDispatch.FinishedAt__c);
            Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), ignoredDispatch.State__c);
            Assert.areEqual(null, missingDispatch);
            Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
            for (Job__c job : [
                SELECT ExternalId__c, State__c, FailedReason__c, QueueableDispatchInitiatedAt__c
                FROM Job__c
                WHERE QueueableDispatch__c = :activeDispatch.Id
            ]) {
                jobsByExternalId.put(job.ExternalId__c, job);
            }
            Assert.areEqual(Queues.State.FAILED.name(), jobsByExternalId.get('fail-dispatch-waiting').State__c);
            Assert.areEqual('dispatcher exploded', jobsByExternalId.get('fail-dispatch-waiting').FailedReason__c);
            Assert.areEqual(Queues.State.FAILED.name(), jobsByExternalId.get('fail-dispatch-delayed').State__c);
            Assert.areEqual('dispatcher exploded', jobsByExternalId.get('fail-dispatch-delayed').FailedReason__c);
            Assert.areEqual(Queues.State.WAITING.name(), jobsByExternalId.get('fail-dispatch-initiated').State__c);
            Assert.areEqual(null, jobsByExternalId.get('fail-dispatch-initiated').FailedReason__c);
            Assert.areNotEqual(null, jobsByExternalId.get('fail-dispatch-initiated').QueueableDispatchInitiatedAt__c);
            Assert.areEqual(Queues.State.WAITING.name(), jobsByExternalId.get('fail-dispatch-worker').State__c);
            Assert.areEqual(null, jobsByExternalId.get('fail-dispatch-worker').FailedReason__c);
        }
    }

    @IsTest
    static void should_create_waiting_dispatch_and_assign_jobs_without_inserting_them() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueableDispatch__c insertedDispatch = new QueueableDispatch__c(
                Id = QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 701),
                QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('insertDispatch', insertedDispatch);
            Job__c queueableJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            QueuesTestSupport.RecordingStubProvider jobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesQueueableDispatchService service = new QueuesQueueableDispatchService(
                (QueuesQueueableDispatchRepository) Test.createStub(
                    QueuesQueueableDispatchRepository.class,
                    repositoryProvider
                ),
                (QueuesJobRepository) Test.createStub(QueuesJobRepository.class, jobRepositoryProvider),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE)
            );

            // Act
            QueueableDispatch__c dispatch = service.createWaitingDispatch(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                'bulk-1',
                Queues.ExecutionMode.QUEUEABLE_SERIAL,
                new List<Job__c>{ queueableJob }
            );

            // Assert
            Assert.areEqual(insertedDispatch.Id, dispatch.Id);
            Assert.areEqual(insertedDispatch.Id, queueableJob.QueueableDispatch__c);
            Assert.areEqual(1, repositoryProvider.getCallCount('insertDispatch'));
            Assert.areEqual(0, jobRepositoryProvider.getCallCount('insertJobs'));
            QueueableDispatch__c requestedDispatch = (QueueableDispatch__c) repositoryProvider.getLastArgs(
                'insertDispatch'
            )[0];
            Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, requestedDispatch.QueueName__c);
            Assert.areEqual('bulk-1', requestedDispatch.DedupeKey__c);
            Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), requestedDispatch.Mode__c);
            Assert.areEqual(Queues.QueueableDispatchState.WAITING.name(), requestedDispatch.State__c);
            Assert.areEqual(1, QueuesSupport.toInteger(requestedDispatch.RequestedCount__c, 0));
        }
    }

    @IsTest
    static void should_return_existing_job_when_duplicate_insert_races_during_enqueue() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c existingJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:DUP-RACE'
            );
            insert existingJob;

            Job__c mappedJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:DUP-RACE'
            );

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'requireProcessors',
                        new Map<String, Processor__mdt>{ processorDefinition.ProcessorClass__c => processorDefinition }
                    )
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns(
                    'getJobsByExternalId',
                    new List<Object>{
                        new Map<String, Job__c>(),
                        new Map<String, Job__c>{ 'invoice:dup-race' => existingJob }
                    }
                )
                .withReturn('mapToJob', mappedJob)
                .withReturn('insertJobsAllowingPartialSuccess', buildInsertSaveResults(new List<Job__c>{ mappedJob }));
            QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
                QueuesJobService.class,
                jobServiceProvider
            );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
                QueuesWorkerService.class,
                workerProvider
            );

            QueuesService service = new QueuesService(
                definitionStub,
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                jobServiceStub,
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                processorStub,
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                workerStub
            );

            // Act
            List<Job__c> jobs = service.enqueue(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'invoiceId' => 'INV-DUP-RACE' },
                        new Queues.JobOptions().jobId('invoice:DUP-RACE')
                    )
                }
            );

            // Assert
            Assert.areEqual(1, jobs.size());
            Assert.areEqual(existingJob.Id, jobs[0].Id);
            Assert.areEqual(2, jobServiceProvider.getCallCount('getJobsByExternalId'));
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobs'));
            Assert.areEqual(0, ((List<Job__c>) jobServiceProvider.getLastArgs('insertJobs')[0]).size());
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobsAllowingPartialSuccess'));
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_return_preexisting_job_without_mapping_when_job_id_already_exists() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c existingJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:PREEXISTING'
            );
            existingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 445);

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'requireProcessors',
                        new Map<String, Processor__mdt>{ processorDefinition.ProcessorClass__c => processorDefinition }
                    )
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobsByExternalId', new Map<String, Job__c>{ 'invoice:preexisting' => existingJob })
                .withReturn('getResolvedJob', existingJob)
                .withReturn('insertJobsAllowingPartialSuccess', new List<Database.SaveResult>());
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

            QueuesService service = new QueuesService(
                definitionStub,
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                processorStub,
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            List<Job__c> jobs = service.enqueue(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'invoiceId' => 'INV-PREEXISTING' },
                        new Queues.JobOptions().jobId('invoice:PREEXISTING')
                    )
                }
            );

            // Assert
            Assert.areEqual(1, jobs.size());
            Assert.areEqual(existingJob.Id, jobs[0].Id);
            Assert.areEqual(0, jobServiceProvider.getCallCount('mapToJob'));
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobs'));
            Assert.areEqual(0, ((List<Job__c>) jobServiceProvider.getLastArgs('insertJobs')[0]).size());
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobsAllowingPartialSuccess'));
            Assert.areEqual(
                0,
                ((List<Job__c>) jobServiceProvider.getLastArgs('insertJobsAllowingPartialSuccess')[0]).size()
            );
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_resolve_duplicate_enqueue_race_case_insensitively() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c existingJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:dup-race'
            );
            Job__c mappedJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:DUP-RACE'
            );
            insert existingJob;

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'requireProcessors',
                        new Map<String, Processor__mdt>{ processorDefinition.ProcessorClass__c => processorDefinition }
                    )
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns(
                    'getJobsByExternalId',
                    new List<Object>{
                        new Map<String, Job__c>(),
                        new Map<String, Job__c>{ 'invoice:dup-race' => existingJob }
                    }
                )
                .withReturn('mapToJob', mappedJob)
                .withReturn('insertJobsAllowingPartialSuccess', buildInsertSaveResults(new List<Job__c>{ mappedJob }));
            QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
                QueuesJobService.class,
                jobServiceProvider
            );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
                QueuesWorkerService.class,
                workerProvider
            );

            QueuesService service = new QueuesService(
                definitionStub,
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                jobServiceStub,
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                processorStub,
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                workerStub
            );

            // Act
            List<Job__c> jobs = service.enqueue(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'invoiceId' => 'INV-DUP-RACE' },
                        new Queues.JobOptions().jobId('invoice:DUP-RACE')
                    )
                }
            );

            // Assert
            Assert.areEqual(1, jobs.size());
            Assert.areEqual(existingJob.Id, jobs[0].Id);
            Assert.areEqual(2, jobServiceProvider.getCallCount('getJobsByExternalId'));
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_preserve_standard_jobs_when_duplicate_idempotent_jobs_are_reconciled() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c standardJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            standardJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 451);
            Job__c duplicateExistingJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:DUP-MIXED'
            );
            insert duplicateExistingJob;
            Job__c duplicateMappedJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:DUP-MIXED'
            );

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'requireProcessors',
                        new Map<String, Processor__mdt>{ processorDefinition.ProcessorClass__c => processorDefinition }
                    )
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns(
                    'getJobsByExternalId',
                    new List<Object>{
                        new Map<String, Job__c>(),
                        new Map<String, Job__c>{ 'invoice:dup-mixed' => duplicateExistingJob }
                    }
                )
                .withReturns('mapToJob', new List<Object>{ standardJob, duplicateMappedJob })
                .withReturn(
                    'insertJobsAllowingPartialSuccess',
                    buildInsertSaveResults(new List<Job__c>{ duplicateMappedJob })
                );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

            QueuesService service = new QueuesService(
                definitionStub,
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                processorStub,
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            List<Job__c> jobs = service.enqueue(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'mode' => 'standard' }
                    ),
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'mode' => 'duplicate' },
                        new Queues.JobOptions().jobId('invoice:DUP-MIXED')
                    )
                }
            );

            // Assert
            Assert.areEqual(2, jobs.size());
            Assert.areEqual(standardJob.Id, jobs[0].Id);
            Assert.areEqual(duplicateExistingJob.Id, jobs[1].Id);
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobs'));
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobsAllowingPartialSuccess'));
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_assign_inserted_id_when_idempotent_job_insert_succeeds() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c mappedJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:IDEMPOTENT-SUCCESS'
            );
            mappedJob.Id = null;

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'requireProcessors',
                        new Map<String, Processor__mdt>{ processorDefinition.ProcessorClass__c => processorDefinition }
                    )
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                .withReturn('mapToJob', mappedJob)
                .withReturn('insertJobsAllowingPartialSuccess', buildInsertSaveResults(new List<Job__c>{ mappedJob }));
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

            QueuesService service = new QueuesService(
                definitionStub,
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                processorStub,
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            List<Job__c> jobs = service.enqueue(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'invoiceId' => 'INV-IDEMPOTENT-SUCCESS' },
                        new Queues.JobOptions().jobId('invoice:IDEMPOTENT-SUCCESS')
                    )
                }
            );

            // Assert
            Assert.areEqual(1, jobs.size());
            Assert.areNotEqual(null, jobs[0].Id);
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobs'));
            Assert.areEqual(0, ((List<Job__c>) jobServiceProvider.getLastArgs('insertJobs')[0]).size());
            Assert.areEqual(1, jobServiceProvider.getCallCount('insertJobsAllowingPartialSuccess'));
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_support_single_enqueue_and_empty_bulk_when_inputs_vary() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c mappedJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            mappedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 402);

            QueuesService service = new QueuesService(
                new QueuesDefinitionService(
                    (QueuesDefinitionRepository) Test.createStub(
                        QueuesDefinitionRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
                    )
                ),
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                new QueuesJobService(),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(
                    (QueuesProcessorRepository) Test.createStub(
                        QueuesProcessorRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
                    )
                ),
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                new QueuesWorkerService()
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                .withReturn('mapToJob', mappedJob)
                .withReturn('insertJobsAllowingPartialSuccess', new List<Database.SaveResult>());
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            service = new QueuesService(
                new QueuesDefinitionService(
                    (QueuesDefinitionRepository) Test.createStub(
                        QueuesDefinitionRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
                    )
                ),
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(
                    (QueuesProcessorRepository) Test.createStub(
                        QueuesProcessorRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
                    )
                ),
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            List<Job__c> emptyJobs = service.enqueue(QueuesTestSupport.DEFAULT_QUEUE_NAME, new List<Queues.JobData>());
            Job__c singleJob = service.enqueue(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                new Queues.JobData(
                    QueuesTestSupport.DEFAULT_JOB_NAME,
                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                    new Map<String, Object>{ 'invoiceId' => 'INV-001' },
                    new Queues.JobOptions()
                )
            );

            // Assert
            Assert.areEqual(0, emptyJobs.size());
            Assert.areEqual(mappedJob.Id, singleJob.Id);
            Assert.areEqual(1, jobServiceProvider.getCallCount('mapToJob'));
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_delegate_admin_stats_and_job_operations_when_services_are_stubbed() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 1);
            Job__c job = QueuesTestSupport.job(Queues.State.WAITING.name());
            job.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 403);
            Queues.QueueStats stats = new Queues.QueueStats(QueuesTestSupport.DEFAULT_QUEUE_NAME)
                .withRuntime(false, 1)
                .withStateCount(Queues.State.WAITING, 2);
            Queues.WorkerStatus workerStatus = new Queues.WorkerStatus(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 404),
                'Holding'
            );

            QueuesTestSupport.RecordingStubProvider queueAdminProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('pause', runtime)
                .withReturn('resume', runtime)
                .withReturn('wake', 1)
                .withReturn('recoverStalled', 2)
                .withReturn('drain', 3)
                .withReturn('cancel', 4)
                .withReturn('getWorkerStatus', workerStatus)
                .withReturn('moveWorkerToFront', true)
                .withReturn('moveWorkerToEnd', true);
            QueuesTestSupport.RecordingStubProvider statsProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getStats', stats);
            QueuesTestSupport.RecordingStubProvider jobAdminProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobState', Queues.State.WAITING)
                .withReturn('retryJob', job)
                .withReturn('pauseJob', job)
                .withReturn('resumeJob', job)
                .withReturn('promoteJob', job)
                .withReturn('discardJob', job)
                .withReturn('cancelJob', job)
                .withReturn('deferJob', job)
                .withReturn('changePriority', job)
                .withReturn('updateJobData', job)
                .withReturn('updateJobProgress', job);

            QueuesService service = new QueuesService(
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                new QueuesJobService(),
                (QueuesJobAdminService) Test.createStub(QueuesJobAdminService.class, jobAdminProvider),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(),
                new QueuesQueueableDispatchService(),
                (QueuesQueueAdminService) Test.createStub(QueuesQueueAdminService.class, queueAdminProvider),
                (QueuesStatsService) Test.createStub(QueuesStatsService.class, statsProvider),
                new QueuesWorkerService()
            );

            // Act
            service.pause(QueuesTestSupport.DEFAULT_QUEUE_NAME);
            service.resume(QueuesTestSupport.DEFAULT_QUEUE_NAME);
            Integer wakeCount = service.wake(QueuesTestSupport.DEFAULT_QUEUE_NAME);
            Integer recoveredCount = service.recoverStalled(QueuesTestSupport.DEFAULT_QUEUE_NAME);
            Integer drainedCount = service.drain(QueuesTestSupport.DEFAULT_QUEUE_NAME);
            Integer canceledCount = service.cancel(QueuesTestSupport.DEFAULT_QUEUE_NAME);
            Queues.WorkerStatus returnedWorkerStatus = service.getWorkerStatus(QueuesTestSupport.DEFAULT_QUEUE_NAME);
            Boolean movedWorkerToFront = service.moveWorkerToFront(QueuesTestSupport.DEFAULT_QUEUE_NAME);
            Boolean movedWorkerToEnd = service.moveWorkerToEnd(QueuesTestSupport.DEFAULT_QUEUE_NAME);
            Queues.QueueStats returnedStats = service.getStats(QueuesTestSupport.DEFAULT_QUEUE_NAME);
            Queues.State jobState = service.getJobState(job.Id);
            service.retryJob(job.Id);
            service.pauseJob(job.Id);
            service.resumeJob(job.Id);
            service.promoteJob(job.Id);
            service.discardJob(job.Id);
            service.cancelJob(job.Id);
            service.deferJob(job.Id, 5);
            service.changePriority(job.Id, 2);
            service.updateJobData(job.Id, new Map<String, Object>{ 'a' => 1 });
            service.updateJobProgress(job.Id, new Map<String, Object>{ 'b' => 2 });
            service.removeJob(job.Id);

            // Assert
            Assert.areEqual(1, wakeCount);
            Assert.areEqual(2, recoveredCount);
            Assert.areEqual(3, drainedCount);
            Assert.areEqual(4, canceledCount);
            Assert.areEqual(workerStatus, returnedWorkerStatus);
            Assert.areEqual(true, movedWorkerToFront);
            Assert.areEqual(true, movedWorkerToEnd);
            Assert.areEqual(stats, returnedStats);
            Assert.areEqual(Queues.State.WAITING, jobState);
            Assert.areEqual(1, queueAdminProvider.getCallCount('pause'));
            Assert.areEqual(1, queueAdminProvider.getCallCount('resume'));
            Assert.areEqual(1, queueAdminProvider.getCallCount('wake'));
            Assert.areEqual(1, queueAdminProvider.getCallCount('recoverStalled'));
            Assert.areEqual(1, queueAdminProvider.getCallCount('drain'));
            Assert.areEqual(1, queueAdminProvider.getCallCount('cancel'));
            Assert.areEqual(1, queueAdminProvider.getCallCount('getWorkerStatus'));
            Assert.areEqual(1, queueAdminProvider.getCallCount('moveWorkerToFront'));
            Assert.areEqual(1, queueAdminProvider.getCallCount('moveWorkerToEnd'));
            Assert.areEqual(1, statsProvider.getCallCount('getStats'));
            Assert.areEqual(1, jobAdminProvider.getCallCount('cancelJob'));
            Assert.areEqual(1, jobAdminProvider.getCallCount('removeJob'));
        }
    }

    @IsTest
    static void should_throw_insert_failure_when_partial_job_insert_returns_nonduplicate_error() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c mappedJob = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'invoice:FAILED-INSERT'
            );
            List<Database.SaveResult> failedSaveResults = buildFailedInsertSaveResults();

            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                .withReturn('mapToJob', mappedJob)
                .withReturn('insertJobsAllowingPartialSuccess', failedSaveResults);
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesService service = new QueuesService(
                new QueuesDefinitionService(
                    (QueuesDefinitionRepository) Test.createStub(
                        QueuesDefinitionRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
                    )
                ),
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(
                    (QueuesProcessorRepository) Test.createStub(
                        QueuesProcessorRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
                    )
                ),
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            String failureMessage;

            // Act
            try {
                service.enqueue(
                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    new List<Queues.JobData>{
                        new Queues.JobData(
                            QueuesTestSupport.DEFAULT_JOB_NAME,
                            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                            new Map<String, Object>{ 'invoiceId' => 'INV-FAILED' },
                            new Queues.JobOptions().jobId('invoice:FAILED-INSERT')
                        )
                    }
                );
                Assert.fail('Expected insert failure to throw.');
            } catch (Queues.QueueException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            Assert.areEqual(true, String.isNotBlank(failureMessage));
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_record_queue_error_after_rollback_when_worker_scheduling_fails_during_enqueue() {
        QueuesTestSupport.RecordingStubProvider workerProvider;
        String failureMessage;

        Test.startTest();
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c mappedJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            mappedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 499);

            QueuesService service = new QueuesService(
                new QueuesDefinitionService(
                    (QueuesDefinitionRepository) Test.createStub(
                        QueuesDefinitionRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
                    )
                ),
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(
                    QueuesJobService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                        .withReturn('mapToJob', mappedJob)
                        .withReturn('insertJobsAllowingPartialSuccess', new List<Database.SaveResult>())
                ),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(
                    (QueuesProcessorRepository) Test.createStub(
                        QueuesProcessorRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
                    )
                ),
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                new QueuesWorkerService()
            );
            workerProvider = new QueuesTestSupport.RecordingStubProvider()
                .withException('scheduleWorkerForJobs', new Queues.QueueException('Worker scheduling exploded.'));
            service = new QueuesService(
                new QueuesDefinitionService(
                    (QueuesDefinitionRepository) Test.createStub(
                        QueuesDefinitionRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
                    )
                ),
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(
                    QueuesJobService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                        .withReturn('mapToJob', mappedJob)
                        .withReturn('insertJobsAllowingPartialSuccess', new List<Database.SaveResult>())
                ),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(
                    (QueuesProcessorRepository) Test.createStub(
                        QueuesProcessorRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
                    )
                ),
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            try {
                service.enqueue(
                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    new List<Queues.JobData>{
                        new Queues.JobData(
                            QueuesTestSupport.DEFAULT_JOB_NAME,
                            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                            new Map<String, Object>{ 'invoiceId' => 'INV-SCHEDULE-FAIL' }
                        )
                    }
                );
                Assert.fail('Expected worker scheduling failure to throw.');
            } catch (Queues.QueueException ex) {
                failureMessage = ex.getMessage();
            }
        }
        Test.getEventBus().deliver();
        Test.stopTest();

        // Assert
        Assert.areEqual('Worker scheduling exploded.', failureMessage);
        Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        QueueError__c error = QueuesTestSupport.queryQueueErrorsAsSystem(QueuesTestSupport.DEFAULT_QUEUE_NAME)[0];
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, error.QueueName__c);
        Assert.areEqual('worker scheduling', error.Component__c);
        Assert.areEqual('Worker scheduling exploded.', error.Message__c);
    }

    @IsTest
    static void should_record_queue_error_after_rollback_when_invocable_enqueue_fails() {
        QueuesTestSupport.RecordingStubProvider jobServiceProvider;
        String failureMessage;

        Test.startTest();
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesErrorService.resetEmittedErrorFingerprints();
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            Job__c mappedJob = QueuesTestSupport.job(Queues.State.WAITING.name());
            mappedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 708);
            mappedJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            mappedJob.AvailableAt__c = System.now().addMinutes(-1);

            jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('getJobsByExternalId', new Map<String, Job__c>())
                .withReturn('mapToJob', mappedJob)
                .withException('insertJobs', new Queues.QueueException('Invocable enqueue exploded.'));
            QueuesService service = new QueuesService(
                new QueuesDefinitionService(
                    (QueuesDefinitionRepository) Test.createStub(
                        QueuesDefinitionRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
                    )
                ),
                new QueuesErrorService(),
                new QueuesFlowProducerService(),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                new QueuesProcessorService(
                    (QueuesProcessorRepository) Test.createStub(
                        QueuesProcessorRepository.class,
                        new QueuesTestSupport.RecordingStubProvider()
                            .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
                    )
                ),
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                new QueuesWorkerService()
            );

            // Act
            try {
                service.enqueue(
                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    new List<Queues.JobData>{
                        new Queues.JobData(
                            QueuesTestSupport.DEFAULT_JOB_NAME,
                            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                            new Map<String, Object>{ 'invoiceId' => 'INV-INVOCABLE-ENQUEUE-FAIL' }
                        )
                    },
                    new Queues.BulkJobOptions().invocable()
                );
                Assert.fail('Expected invocable enqueue failure to throw.');
            } catch (Queues.QueueException ex) {
                failureMessage = ex.getMessage();
            }
        }
        Test.getEventBus().deliver();
        Test.stopTest();

        // Assert
        Assert.areEqual('Invocable enqueue exploded.', failureMessage);
        Assert.areEqual(0, jobServiceProvider.getCallCount('claimInvocableJobs'));
        QueueError__c error = QueuesTestSupport.queryQueueErrorsAsSystem(QueuesTestSupport.DEFAULT_QUEUE_NAME)[0];
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, error.QueueName__c);
        Assert.areEqual('invocable enqueue', error.Component__c);
        Assert.areEqual('Invocable enqueue exploded.', error.Message__c);
    }

    private static List<Database.SaveResult> buildInsertSaveResults(final List<Job__c> jobs) {
        List<Job__c> insertCandidates = new List<Job__c>();
        for (Job__c job : jobs) {
            insertCandidates.add((Job__c) QueuesSupport.clone(job));
        }
        return Database.insert(insertCandidates, false);
    }

    private static List<Database.SaveResult> buildFailedInsertSaveResults() {
        return Database.insert(new List<Job__c>{ new Job__c() }, false);
    }

    private static List<String> toExecutionModeValues(final List<Job__c> jobs) {
        List<String> executionModeValues = new List<String>();
        for (Job__c job : jobs) {
            executionModeValues.add(job.ExecutionMode__c);
        }
        return executionModeValues;
    }

    private class TestableQueueableDispatchService extends QueuesQueueableDispatchService {
        private Integer dispatcherCount = 0;
        private Integer executorCount = 0;
        private Integer futureBridgeCount = 0;
        private Id lastDispatcherDispatchId;
        private Id lastFutureBridgeDispatchId;
        private Boolean lastDispatcherUseInitialStackDepth;
        private String lastDispatcherQueueName;
        private String lastDispatcherDedupeKey;
        private Id lastExecutorJobId;
        private Integer lastExecutorDelayMinutes;
        private Exception executorException;
        private Set<Id> executorExceptionJobIds = new Set<Id>();

        protected override Id enqueueDispatcherJob(final Id dispatchId, final Boolean useInitialStackDepth) {
            lastDispatcherDispatchId = dispatchId;
            lastDispatcherUseInitialStackDepth = useInitialStackDepth;
            lastDispatcherQueueName = null;
            lastDispatcherDedupeKey = null;
            dispatcherCount++;
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 850 + dispatcherCount);
        }

        protected override Id enqueueDispatcherJob(
            final Id dispatchId,
            final Boolean useInitialStackDepth,
            final String queueName,
            final String dedupeKey
        ) {
            lastDispatcherDispatchId = dispatchId;
            lastDispatcherUseInitialStackDepth = useInitialStackDepth;
            lastDispatcherQueueName = queueName;
            lastDispatcherDedupeKey = dedupeKey;
            dispatcherCount++;
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 850 + dispatcherCount);
        }

        protected override Id enqueueExecutor(final Id jobId, final Integer delayMinutes) {
            executorCount++;
            lastExecutorJobId = jobId;
            lastExecutorDelayMinutes = delayMinutes;
            if (
                executorException != null &&
                (executorExceptionJobIds.isEmpty() || executorExceptionJobIds.contains(jobId))
            ) {
                throw executorException;
            }
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 860 + executorCount);
        }

        protected override Id enqueueExecutorViaFutureBridge(final Id jobId, final Integer delayMinutes) {
            return enqueueExecutor(jobId, delayMinutes);
        }

        protected override void enqueueDispatcherViaFutureBridge(final Id dispatchId) {
            futureBridgeCount++;
            lastFutureBridgeDispatchId = dispatchId;
        }
    }

    @SuppressWarnings('PMD.QueueableWithoutFinalizer')
    private class ContextDispatchProbe implements Queueable {
        private final Id dispatchId;

        private ContextDispatchProbe(final Id dispatchId) {
            this.dispatchId = dispatchId;
        }

        public void execute(final QueueableContext ctx) {
            try {
                new QueuesQueueableDispatchService().continueDispatch(dispatchId, true, false);
            } catch (System.AsyncException ex) {
                // Developer Edition stack-depth limits can reject the probe enqueue after the stack context is resolved.
                Assert.areEqual(
                    false,
                    ex.getMessage()?.contains('Cannot reset Maximum Queueable Stack Depth') == true,
                    ex.getMessage()
                );
            }
            QueueableDispatch__c dispatch = [
                SELECT Id, State__c, FinishedAt__c
                FROM QueueableDispatch__c
                WHERE Id = :dispatchId
                FOR UPDATE
            ];
            dispatch.State__c = Queues.QueueableDispatchState.COMPLETED.name();
            dispatch.FinishedAt__c = System.now();
            update dispatch;
        }
    }
}
