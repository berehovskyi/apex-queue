@IsTest
public class QueuesTestSupport {
    public static final String DEFAULT_QUEUE_NAME = 'invoice-sync';
    public static final String DEFAULT_JOB_NAME = 'sync-invoice';
    public static final String DEFAULT_PROCESSOR_CLASS = 'QueuesTestSupport.InvoiceSyncProcessor';

    public static void setupQueueUser() {
        Profile minimumAccessProfile = [SELECT Id FROM Profile WHERE Name = 'Minimum Access - Salesforce' LIMIT 1];
        String uniqueSuffix =
            String.valueOf(System.now().getTime()) + String.valueOf(Crypto.getRandomInteger()).replace('-', '0');
        String uniqueName = 'apex.queue.test.' + uniqueSuffix;
        User testUser = new User(
            Alias = 'aqtstu',
            CommunityNickname = ('aqts' + uniqueSuffix).left(40),
            Email = uniqueName + '@example.com',
            EmailEncodingKey = 'UTF-8',
            LanguageLocaleKey = 'en_US',
            LastName = 'Queues Test User',
            LocaleSidKey = 'en_US',
            ProfileId = minimumAccessProfile.Id,
            TimeZoneSidKey = 'America/Los_Angeles',
            Username = uniqueName + '@example.com'
        );
        insert testUser;
        insert new PermissionSetAssignment(
            AssigneeId = testUser.Id,
            PermissionSetId = [SELECT Id FROM PermissionSet WHERE Name = 'QueuesUser' LIMIT 1]
            .Id
        );
    }

    public static User queueUser() {
        return [SELECT Id FROM User WHERE LastName = 'Queues Test User' ORDER BY CreatedDate DESC LIMIT 1];
    }

    public static List<QueueError__c> queryQueueErrorsAsSystem(final String queueName) {
        return Database.queryWithBinds(
            'SELECT QueueName__c, Component__c, Job__c, AsyncApexJobId__c, Message__c, StackTrace__c, OccurredAt__c ' +
                'FROM QueueError__c ' +
                'WHERE QueueName__c = :queueName ' +
                'ORDER BY CreatedDate DESC',
            new Map<String, Object>{ 'queueName' => queueName },
            System.AccessLevel.SYSTEM_MODE
        );
    }

    public static QueueDefinition__mdt queueDefinition() {
        return queueDefinition(DEFAULT_QUEUE_NAME, true);
    }

    public static QueueDefinition__mdt queueDefinition(final String queueName, final Boolean isActive) {
        return new QueueDefinition__mdt(
            DeveloperName = queueName.replace('-', '_'),
            MasterLabel = queueName,
            QueueName__c = queueName,
            IsActive__c = isActive,
            DefaultAttempts__c = 3,
            DefaultBackoffType__c = Queues.Backoff.NONE.name(),
            DefaultBackoffValue__c = 0,
            DefaultBackoffJitter__c = 0,
            DefaultLeaseMinutes__c = 5,
            BatchClaimSize__c = 1
        );
    }

    public static Processor__mdt processorDefinition() {
        return processorDefinition(DEFAULT_QUEUE_NAME, DEFAULT_JOB_NAME, DEFAULT_PROCESSOR_CLASS, true);
    }

    public static Processor__mdt processorDefinition(
        final String queueName,
        final String name,
        final String processorClass,
        final Boolean isActive
    ) {
        return new Processor__mdt(
            DeveloperName = (queueName + '_' + name).replace('-', '_'),
            MasterLabel = queueName + ' ' + name,
            Name__c = name,
            ProcessorClass__c = processorClass,
            IsActive__c = isActive
        );
    }

    public static Job__c job(final String stateValue) {
        return job(DEFAULT_QUEUE_NAME, DEFAULT_JOB_NAME, stateValue, null);
    }

    public static Job__c job(
        final String queueName,
        final String name,
        final String stateValue,
        final String externalId
    ) {
        return new Job__c(
            QueueName__c = queueName,
            Name__c = name,
            State__c = stateValue,
            ExecutionMode__c = Queues.ExecutionMode.WORKER.name(),
            ExternalId__c = externalId,
            QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, externalId),
            ProcessorClass__c = DEFAULT_PROCESSOR_CLASS,
            AvailableAt__c = System.now(),
            Priority__c = 0,
            AttemptsMade__c = 0,
            MaxAttempts__c = 3,
            PauseRequested__c = false
        );
    }

    public static QueueRuntime__c runtime(
        final String queueName,
        final Boolean isPaused,
        final Integer activeWorkerCount
    ) {
        return new QueueRuntime__c(
            QueueName__c = queueName,
            IsPaused__c = isPaused,
            ActiveWorkerCount__c = activeWorkerCount
        );
    }

    public static JobRun__c jobRun(final Id jobId, final Integer attemptNumber, final String statusValue) {
        return new JobRun__c(
            Job__c = jobId,
            AttemptNumber__c = attemptNumber,
            StartedAt__c = System.now(),
            Status__c = statusValue
        );
    }

    public static JobScheduler__c jobScheduler(final String queueName, final String jobSchedulerId) {
        return new JobScheduler__c(
            Name = jobSchedulerId,
            QueueName__c = queueName,
            ExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, jobSchedulerId),
            IsActive__c = true,
            JobName__c = DEFAULT_JOB_NAME,
            ProcessorClass__c = DEFAULT_PROCESSOR_CLASS,
            Data__c = '{"invoiceId":"INV-SCHED"}',
            ExecutionMode__c = Queues.ExecutionMode.WORKER.name(),
            EveryMinutes__c = 5,
            IterationCount__c = 1,
            NextRunAt__c = System.now().addMinutes(5)
        );
    }

    public static Database.QueryLocator emptyJobLocator() {
        return Database.getQueryLocator('SELECT Id FROM Job__c LIMIT 0');
    }

    public static Id fakeId(final SObjectType sObjectType, final Integer sequence) {
        String keyPrefix = sObjectType.getDescribe(SObjectDescribeOptions.DEFERRED).getKeyPrefix();
        String suffix = sequence.toString().leftPad(12, '0');
        return Id.valueOf(keyPrefix + suffix);
    }

    public static String fakeIdValue(final SObjectType sObjectType, final Integer sequence) {
        return fakeId(sObjectType, sequence);
    }

    public static AsyncApexJob asyncApexJob(final Id jobId, final String status, final String apexClassName) {
        return asyncApexJob(jobId, status, apexClassName, null);
    }

    public static AsyncApexJob asyncApexJob(
        final Id jobId,
        final String status,
        final String apexClassName,
        final Datetime createdAt
    ) {
        return (AsyncApexJob) JSON.deserialize(
            JSON.serialize(
                new Map<String, Object>{
                    'Id' => jobId,
                    'Status' => status,
                    'CreatedDate' => createdAt,
                    'ApexClass' => new Map<String, Object>{ 'Name' => apexClassName }
                }
            ),
            AsyncApexJob.class
        );
    }

    public static BatchApexErrorEvent batchApexErrorEvent(
        final Id asyncApexJobId,
        final String phase,
        final String jobScope,
        final String exceptionType,
        final String message,
        final String stackTrace
    ) {
        return batchApexErrorEvent(asyncApexJobId, phase, jobScope, exceptionType, message, stackTrace, null);
    }

    public static BatchApexErrorEvent batchApexErrorEvent(
        final Id asyncApexJobId,
        final String phase,
        final String jobScope,
        final String exceptionType,
        final String message,
        final String stackTrace,
        final Datetime createdAt
    ) {
        return (BatchApexErrorEvent) JSON.deserialize(
            JSON.serialize(
                new Map<String, Object>{
                    'AsyncApexJobId' => asyncApexJobId,
                    'Phase' => phase,
                    'JobScope' => jobScope,
                    'ExceptionType' => exceptionType,
                    'Message' => message,
                    'StackTrace' => stackTrace,
                    'CreatedDate' => createdAt
                }
            ),
            BatchApexErrorEvent.class
        );
    }

    public static CronTrigger cronTrigger(
        final Id cronTriggerId,
        final String stateValue,
        final String cronExpression,
        final String jobName
    ) {
        return cronTrigger(cronTriggerId, stateValue, cronExpression, jobName, null);
    }

    public static CronTrigger cronTrigger(
        final Id cronTriggerId,
        final String stateValue,
        final String cronExpression,
        final String jobName,
        final Datetime nextFireTime
    ) {
        return (CronTrigger) JSON.deserialize(
            JSON.serialize(
                new Map<String, Object>{
                    'Id' => cronTriggerId,
                    'State' => stateValue,
                    'CronExpression' => cronExpression,
                    'NextFireTime' => nextFireTime,
                    'CronJobDetail' => new Map<String, Object>{ 'Name' => jobName }
                }
            ),
            CronTrigger.class
        );
    }

    public static QueuesService newFacadeServiceWithQueueMetadata(final RecordingStubProvider workerProvider) {
        return newFacadeServiceWithQueueMetadata(DEFAULT_QUEUE_NAME, workerProvider);
    }

    public static QueuesService newFacadeServiceWithQueueMetadata(
        final String queueName,
        final RecordingStubProvider workerProvider
    ) {
        QueueDefinition__mdt queueDefinition = queueDefinition(queueName, true);
        Processor__mdt processorDefinition = processorDefinition(
            queueName,
            DEFAULT_JOB_NAME,
            DEFAULT_PROCESSOR_CLASS,
            true
        );

        QueuesDefinitionService definitionService = new QueuesDefinitionService(
            (QueuesDefinitionRepository) Test.createStub(
                QueuesDefinitionRepository.class,
                new RecordingStubProvider()
                    .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
                    .withReturn('queryQueueDefinitionsByQueueNames', new List<QueueDefinition__mdt>{ queueDefinition })
            )
        );
        QueuesProcessorService processorService = new QueuesProcessorService(
            (QueuesProcessorRepository) Test.createStub(
                QueuesProcessorRepository.class,
                new RecordingStubProvider()
                    .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
            )
        );
        QueuesStatsService statsService = new QueuesStatsService(
            definitionService,
            new QueuesRuntimeService(),
            new QueuesJobService()
        );

        QueuesWorkerService workerService = workerProvider != null
            ? (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            : new QueuesWorkerService();
        QueuesJobService jobService = new QueuesJobService();
        QueuesRuntimeService runtimeService = new QueuesRuntimeService();
        QueuesQueueableExecutionService queueableExecutionService = new QueuesQueueableExecutionService();
        QueuesQueueableDispatchService queueableDispatchService = new QueuesQueueableDispatchService();
        QueuesFlowProducerService flowProducerService = new QueuesFlowProducerService(
            definitionService,
            jobService,
            processorService,
            queueableDispatchService,
            runtimeService,
            workerService
        );
        QueuesJobSchedulerService jobSchedulerService = new QueuesJobSchedulerService(
            definitionService,
            new QueuesErrorService(),
            new QueuesJobSchedulerRepository(),
            jobService,
            processorService,
            queueableDispatchService,
            new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
            new QueuesJobSchedulerSchedule(),
            flowProducerService,
            workerService
        );
        QueuesQueueAdminService queueAdminService = new QueuesQueueAdminService(
            definitionService,
            runtimeService,
            jobService,
            new QueuesJobRunService(),
            queueableDispatchService,
            queueableExecutionService,
            new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
            new QueuesStalledJobRecoveryService(),
            flowProducerService,
            workerService
        );
        QueuesJobAdminService jobAdminService = new QueuesJobAdminService(
            jobService,
            runtimeService,
            queueableExecutionService,
            flowProducerService,
            workerService
        );

        return new QueuesService(
            definitionService,
            new QueuesErrorService(),
            flowProducerService,
            jobService,
            jobAdminService,
            jobSchedulerService,
            new QueuesMaintenanceService(),
            new QueuesMaintenanceSchedule(),
            processorService,
            queueableDispatchService,
            queueAdminService,
            statsService,
            workerService
        );
    }

    public static void resetFacadeService() {
        Queues.queueService = new QueuesService();
    }

    public class RecordingStubProvider implements System.StubProvider {
        private final Map<String, List<Object>> returnsByMethodName = new Map<String, List<Object>>();
        private final Map<String, Exception> exceptionsByMethodName = new Map<String, Exception>();
        private final Map<String, Integer> callCountsByMethodName = new Map<String, Integer>();
        private final Map<String, List<List<Object>>> callsByMethodName = new Map<String, List<List<Object>>>();

        public RecordingStubProvider withReturn(final String methodName, final Object returnValue) {
            returnsByMethodName.put(methodName, new List<Object>{ returnValue });
            return this;
        }

        public RecordingStubProvider withReturns(final String methodName, final List<Object> returnValues) {
            List<Object> copiedReturnValues = new List<Object>();
            for (Object returnValue : returnValues ?? new List<Object>()) {
                copiedReturnValues.add(returnValue);
            }
            returnsByMethodName.put(methodName, copiedReturnValues);
            return this;
        }

        public RecordingStubProvider withException(final String methodName, final Exception ex) {
            exceptionsByMethodName.put(methodName, ex);
            return this;
        }

        public Integer getCallCount(final String methodName) {
            return callCountsByMethodName.containsKey(methodName) ? callCountsByMethodName.get(methodName) : 0;
        }

        public List<Object> getLastArgs(final String methodName) {
            List<List<Object>> calls = callsByMethodName.get(methodName);
            return calls == null || calls.isEmpty() ? null : calls[calls.size() - 1];
        }

        public List<List<Object>> getCalls(final String methodName) {
            return callsByMethodName.containsKey(methodName)
                ? callsByMethodName.get(methodName)
                : new List<List<Object>>();
        }

        public Object handleMethodCall(
            final Object stubbedObject,
            final String stubbedMethodName,
            final Type returnType,
            final List<Type> listOfParamTypes,
            final List<String> listOfParamNames,
            final List<Object> listOfArgs
        ) {
            callCountsByMethodName.put(stubbedMethodName, getCallCount(stubbedMethodName) + 1);
            String fallbackMethodName = resolveFallbackMethodName(stubbedMethodName);

            List<Object> copiedArgs = new List<Object>();
            for (Object arg : listOfArgs ?? new List<Object>()) {
                copiedArgs.add(arg);
            }
            recordCall(stubbedMethodName, copiedArgs);
            if (fallbackMethodName != null && !hasConfiguredBehavior(stubbedMethodName)) {
                callCountsByMethodName.put(fallbackMethodName, getCallCount(fallbackMethodName) + 1);
                recordCall(fallbackMethodName, copiedArgs);
            }

            String behaviorMethodName = hasConfiguredBehavior(stubbedMethodName) || fallbackMethodName == null
                ? stubbedMethodName
                : fallbackMethodName;
            if (exceptionsByMethodName.containsKey(behaviorMethodName)) {
                throw exceptionsByMethodName.get(behaviorMethodName);
            }

            if (!returnsByMethodName.containsKey(behaviorMethodName)) {
                return null;
            }

            List<Object> returnValues = returnsByMethodName.get(behaviorMethodName);
            if (returnValues.isEmpty()) {
                return null;
            }
            if (returnValues.size() == 1) {
                return returnValues[0];
            }

            Object returnValue = returnValues.remove(0);
            returnsByMethodName.put(behaviorMethodName, returnValues);
            return returnValue;
        }

        private Boolean hasConfiguredBehavior(final String methodName) {
            return returnsByMethodName.containsKey(methodName) || exceptionsByMethodName.containsKey(methodName);
        }

        private String resolveFallbackMethodName(final String methodName) {
            String suffix = 'AsSystem';
            return methodName?.endsWith(suffix) == true
                ? methodName.substring(0, methodName.length() - suffix.length())
                : null;
        }

        private void recordCall(final String methodName, final List<Object> copiedArgs) {
            List<List<Object>> calls = callsByMethodName.containsKey(methodName)
                ? callsByMethodName.get(methodName)
                : new List<List<Object>>();
            calls.add(copiedArgs);
            callsByMethodName.put(methodName, calls);
        }
    }

    public class InvoiceSyncProcessor implements Queues.JobProcessor {
        public Queues.ProcessResult process(final Queues.JobContext ctx) {
            return ctx.complete(ctx.data());
        }
    }
}
