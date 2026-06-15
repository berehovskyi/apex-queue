@IsTest(IsParallel=true)
private class QueuesProcessorServiceTest {
    @IsTest
    static void should_require_active_processors_when_job_processors_are_requested() {
        // Arrange
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            QueuesTestSupport.DEFAULT_JOB_NAME,
            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            true
        );
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition });
        QueuesProcessorRepository repositoryStub = (QueuesProcessorRepository) Test.createStub(
            QueuesProcessorRepository.class,
            repositoryProvider
        );

        QueuesProcessorService service = new QueuesProcessorService(repositoryStub);

        // Act
        Map<String, Processor__mdt> processors = service.requireProcessors(
            new List<Queues.JobData>{
                new Queues.JobData(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, null)
            }
        );
        // Assert
        Assert.areEqual(1, processors.size());
        Assert.areEqual(1, repositoryProvider.getCallCount('queryProcessorsByClassNames'));
    }

    @IsTest
    static void should_return_empty_processor_map_when_no_job_names_are_requested() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>());
        QueuesProcessorRepository repositoryStub = (QueuesProcessorRepository) Test.createStub(
            QueuesProcessorRepository.class,
            repositoryProvider
        );

        QueuesProcessorService service = new QueuesProcessorService(repositoryStub);

        // Act
        Map<String, Processor__mdt> processors = service.requireProcessors(new List<Queues.JobData>());

        // Assert
        Assert.areEqual(0, processors.size());
        Assert.areEqual(1, repositoryProvider.getCallCount('queryProcessorsByClassNames'));
    }

    @IsTest
    static void should_return_processors_by_class_and_prefer_active_definition_when_duplicates_exist() {
        // Arrange
        Processor__mdt inactiveProcessorDefinition = QueuesTestSupport.processorDefinition(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'sync-invoice-inactive',
            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            false
        );
        Processor__mdt activeProcessorDefinition = QueuesTestSupport.processorDefinition();
        QueuesProcessorRepository repositoryStub = (QueuesProcessorRepository) Test.createStub(
            QueuesProcessorRepository.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryProcessorsByClassNames',
                    new List<Processor__mdt>{ inactiveProcessorDefinition, activeProcessorDefinition }
                )
        );

        QueuesProcessorService service = new QueuesProcessorService(repositoryStub);

        // Act
        Map<String, Processor__mdt> processorsByClass = service.getProcessorsByClassAsSystem(
            new Set<String>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS }
        );

        // Assert
        Assert.areEqual(1, processorsByClass.size());
        Assert.areEqual(true, processorsByClass.containsKey(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS));
        Assert.areEqual(true, QueuesSupport.isActive(processorsByClass.get(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)));
        Assert.areEqual(
            activeProcessorDefinition.Name__c,
            processorsByClass.get(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS).Name__c
        );
    }

    @IsTest
    static void should_ignore_processor_definitions_without_processor_class_names() {
        // Arrange
        Processor__mdt blankProcessorDefinition = QueuesTestSupport.processorDefinition();
        blankProcessorDefinition.ProcessorClass__c = null;
        Processor__mdt activeProcessorDefinition = QueuesTestSupport.processorDefinition();
        QueuesProcessorRepository repositoryStub = (QueuesProcessorRepository) Test.createStub(
            QueuesProcessorRepository.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryProcessorsByClassNames',
                    new List<Processor__mdt>{ blankProcessorDefinition, activeProcessorDefinition }
                )
        );

        QueuesProcessorService service = new QueuesProcessorService(repositoryStub);

        // Act
        Map<String, Processor__mdt> processorsByClass = service.getProcessorsByClassAsSystem(
            new Set<String>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS }
        );

        // Assert
        Assert.areEqual(1, processorsByClass.size());
        Assert.areEqual(true, processorsByClass.containsKey(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS));
    }

    @IsTest
    static void should_resolve_processors_case_insensitively_when_request_casing_differs_from_metadata() {
        // Arrange
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
        String requestedProcessorClass = processorDefinition.ProcessorClass__c.toLowerCase();
        QueuesProcessorRepository repositoryStub = (QueuesProcessorRepository) Test.createStub(
            QueuesProcessorRepository.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
        );

        QueuesProcessorService service = new QueuesProcessorService(repositoryStub);

        // Act
        Map<String, Processor__mdt> processorsByClass = service.getProcessorsByClassAsSystem(
            new Set<String>{ requestedProcessorClass }
        );

        // Assert
        Assert.areEqual(1, processorsByClass.size());
        Assert.areEqual(true, processorsByClass.containsKey(requestedProcessorClass));
        Assert.areEqual(processorDefinition.Name__c, processorsByClass.get(requestedProcessorClass).Name__c);
        Assert.areEqual(
            processorDefinition.ProcessorClass__c,
            processorsByClass.get(requestedProcessorClass).ProcessorClass__c
        );
    }

    @IsTest
    static void should_require_processors_and_execute_processor_instance_when_processor_metadata_is_valid() {
        // Arrange
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
        QueuesProcessorRepository repositoryStub = (QueuesProcessorRepository) Test.createStub(
            QueuesProcessorRepository.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryProcessorsByClassNames',
                    new List<Processor__mdt>{ processorDefinition, processorDefinition }
                )
        );
        Job__c job = QueuesTestSupport.job(Queues.State.WAITING.name());
        job.ProcessorClass__c = QueuesTestSupport.DEFAULT_PROCESSOR_CLASS;
        job.Data__c = QueuesSupport.serialize(new Map<String, Object>{ 'status' => 'done' });

        QueuesProcessorService service = new QueuesProcessorService(repositoryStub);

        // Act
        Map<String, Processor__mdt> requiredProcessors = service.requireProcessors(
            new List<Queues.JobData>{
                new Queues.JobData(QueuesTestSupport.DEFAULT_JOB_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, null)
            }
        );
        Map<String, Processor__mdt> processorsByClass = service.getProcessorsByClassAsSystem(
            new Set<String>{ job.ProcessorClass__c }
        );
        Object processor = service.instantiate(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS);
        Queues.ProcessResult result = service.execute(processor, job);

        // Assert
        Assert.areEqual(1, requiredProcessors.size());
        Assert.areEqual(true, requiredProcessors.containsKey(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS));
        Assert.areEqual(
            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            requiredProcessors.get(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS).ProcessorClass__c
        );
        Assert.areEqual(1, processorsByClass.size());
        Assert.areEqual(true, processorsByClass.containsKey(job.ProcessorClass__c));
        Assert.areEqual(Queues.Outcome.COMPLETED, result.getOutcome());
        Assert.areEqual('{"status":"done"}', result.getReturnValueJson());
    }

    @IsTest
    static void should_throw_configuration_exception_when_processor_configuration_is_invalid() {
        // Arrange
        QueuesProcessorService service = new QueuesProcessorService();
        service = new QueuesProcessorService(
            (QueuesProcessorRepository) Test.createStub(
                QueuesProcessorRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>())
            )
        );

        String missingProcessorMessage;
        String missingTypeMessage;
        String invalidProcessorMessage;

        // Act
        try {
            service.requireProcessors(
                new List<Queues.JobData>{
                    new Queues.JobData(
                        QueuesTestSupport.DEFAULT_JOB_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        null
                    )
                }
            );
            Assert.fail('Expected missing processor lookup to throw.');
        } catch (Queues.ConfigurationException ex) {
            missingProcessorMessage = ex.getMessage();
        }

        try {
            service.instantiate('MissingQueueProcessor');
            Assert.fail('Expected missing processor type to throw.');
        } catch (Queues.ConfigurationException ex) {
            missingTypeMessage = ex.getMessage();
        }

        try {
            service.execute('not-a-processor', new Job__c(ProcessorClass__c = 'not-a-processor'));
            Assert.fail('Expected invalid processor instance to throw.');
        } catch (Queues.ConfigurationException ex) {
            invalidProcessorMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual(true, missingProcessorMessage.contains('does not have an active processor metadata mapping'));
        Assert.areEqual(true, missingTypeMessage.contains('could not be resolved'));
        Assert.areEqual(true, invalidProcessorMessage.contains('does not implement'));
    }
}
