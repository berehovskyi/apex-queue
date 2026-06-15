/**
 * @description Resolves, instantiates, and executes processor classes.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesProcessorService {
    private static final String MISSING_PROCESSOR_MESSAGE = 'Processor "{0}" does not have an active processor metadata mapping.';

    private QueuesProcessorRepository repository = new QueuesProcessorRepository(System.AccessLevel.SYSTEM_MODE);

    public QueuesProcessorService() {
    }

    @TestVisible
    private QueuesProcessorService(final QueuesProcessorRepository repository) {
        this.repository = repository;
    }

    // <editor-fold desc="Processors">
    public Map<String, Processor__mdt> requireProcessors(final List<Queues.JobData> jobs) {
        Map<String, Processor__mdt> processorByClass = getProcessorsByClassAsSystem(toProcessorClassNames(jobs));
        for (Queues.JobData job : jobs) {
            Processor__mdt definition = processorByClass.get(job.getProcessor());
            QueuesSupport.isTrue(
                QueuesSupport.isActive(definition),
                new Queues.ConfigurationException(
                    String.format(MISSING_PROCESSOR_MESSAGE, new List<String>{ job.getProcessor() })
                )
            );
        }
        return processorByClass;
    }

    public Map<String, Processor__mdt> getProcessorsByClassAsSystem(final Set<String> processorClassNames) {
        Map<String, Processor__mdt> processorByNormalizedClass = new Map<String, Processor__mdt>();
        for (Processor__mdt definition : repository.queryProcessorsByClassNames(processorClassNames)) {
            String processorClassName = definition.ProcessorClass__c;
            String normalizedProcessorClassName = normalizeProcessorClassName(processorClassName);
            if (String.isBlank(normalizedProcessorClassName)) {
                continue;
            }

            Processor__mdt currentDefinition = processorByNormalizedClass.get(normalizedProcessorClassName);
            if (
                currentDefinition == null ||
                (!QueuesSupport.isActive(currentDefinition) && QueuesSupport.isActive(definition))
            ) {
                processorByNormalizedClass.put(normalizedProcessorClassName, definition);
            }
        }

        Map<String, Processor__mdt> processorByClass = new Map<String, Processor__mdt>();
        for (String processorClassName : processorClassNames) {
            Processor__mdt definition = processorByNormalizedClass.get(normalizeProcessorClassName(processorClassName));
            if (definition != null) {
                processorByClass.put(processorClassName, definition);
            }
        }
        return processorByClass;
    }

    public Object instantiate(final String processorClassName) {
        QueuesSupport.notBlank(processorClassName, 'processorClassName');

        Type processorType = Type.forName(processorClassName);
        QueuesSupport.isTrue(
            processorType != null,
            new Queues.ConfigurationException(
                'Processor "' +
                    processorClassName +
                    '" could not be resolved. Please ensure this class exists, is accessible, and has a public no-argument constructor.'
            )
        );
        return processorType.newInstance();
    }

    public Queues.ProcessResult execute(final Object processor, final Job__c job) {
        Queues.JobContext ctx = new Queues.JobContext(job);
        if (processor instanceof Queues.JobProcessor) {
            return ((Queues.JobProcessor) processor).process(ctx);
        }
        throw new Queues.ConfigurationException(
            'Processor "' + job.ProcessorClass__c + '" does not implement a queue processor interface.'
        );
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private static Set<String> toProcessorClassNames(final List<Queues.JobData> jobs) {
        Set<String> processorClassNames = new Set<String>();
        for (Queues.JobData job : jobs) {
            if (String.isNotBlank(job?.getProcessor())) {
                processorClassNames.add(job.getProcessor());
            }
        }
        return processorClassNames;
    }

    private static String normalizeProcessorClassName(final String processorClassName) {
        return processorClassName?.trim()?.toLowerCase();
    }
    // </editor-fold>
}
