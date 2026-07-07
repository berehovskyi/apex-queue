/**
 * @description Inserts dependency graphs and resolves parent dependencies.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesFlowProducerService {
    private static final String CHILD_FAILED_REASON = 'Child job failed.';
    private static final String CHILD_CANCELED_REASON = 'Child job was canceled.';
    private static final String DEFAULT_DEPENDENCY_POLICY = 'DEFAULT';
    private static final String DEPENDENCY_RESOLUTION_COMPONENT = 'dependency resolution';
    private static final String EXISTING_FLOW_EXTENSION_MESSAGE = 'Existing flow jobs can only participate in an idempotent replay; adding new children is not supported.';
    private static final String EXISTING_FLOW_DEPENDENCIES_MESSAGE = 'Existing flow job dependencies do not match the requested flow.';
    private static final String EXISTING_FLOW_EXECUTION_MODE_MESSAGE = 'Existing flow job execution mode does not match flow execution mode.';
    private static final String EXISTING_FLOW_PARENT_MESSAGE = 'Existing flow job parent does not match the requested flow.';
    private static final String FLOW_CYCLE_MESSAGE = 'Flow jobs cannot form cycles.';
    private static final String FLOW_INSTANCE_REUSE_MESSAGE = 'Flow job instances cannot be reused within one graph.';
    private static final String FLOW_JOB_ID_UNIQUE_MESSAGE = 'Flow jobId values must be unique within each queue.';
    private static final String FLOW_INSERT_RESULTS_MISMATCH_MESSAGE = 'Flow job insert results did not match the requested job count.';
    private static final String FLOW_INSERT_FAILED_MESSAGE = 'Failed to insert flow job.';
    private static final String INVALID_PARENT_OPTION_MESSAGE = 'Flow jobs do not support opts.parent(...). Define dependencies with child(...).';
    private static final String MISSING_QUEUE_MESSAGE = 'Queue "{0}" is not configured.';
    private static final String INACTIVE_QUEUE_MESSAGE = 'Queue "{0}" is inactive.';
    private static final String DUPLICATE_FLOW_JOB_RESOLUTION_MESSAGE = 'Failed to resolve duplicate flow job "{0}" after insert race.';
    private static final Integer DEPENDENCY_CLEANUP_BATCH_SIZE = 200;
    private static final Integer DEPENDENCY_REPAIR_BATCH_SIZE = 50;
    private static final Integer DEPENDENCY_RESOLUTION_BATCH_COUNT_LIMIT = 10;
    private static final Integer DEPENDENCY_RESOLUTION_PARENT_BATCH_SIZE = 10;
    private static final Integer DEPENDENCY_RESOLUTION_DML_ROW_HEADROOM = 100;
    private static final Integer DEPENDENCY_RESOLUTION_DML_STATEMENT_HEADROOM = 10;
    private static final Integer DEPENDENCY_RESOLUTION_QUERY_HEADROOM = 15;
    private static final Integer DEPENDENCY_RESOLUTION_CPU_MS_HEADROOM = 2000;
    private static final Set<Queues.State> RUNNABLE_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED
    };
    private static final Set<Queues.State> TERMINAL_JOB_STATES = new Set<Queues.State>{
        Queues.State.COMPLETED,
        Queues.State.FAILED,
        Queues.State.CANCELED
    };
    @TestVisible
    private static Integer forceDependencyResolutionBatchCountLimit;

    private QueuesDefinitionService definitionService = new QueuesDefinitionService();
    private QueuesJobService jobService = new QueuesJobService();
    private QueuesProcessorService processorService = new QueuesProcessorService();
    private QueuesQueueableDispatchService queueableDispatchService = new QueuesQueueableDispatchService();
    private QueuesRuntimeService runtimeService = new QueuesRuntimeService();
    private QueuesWorkerService workerService = new QueuesWorkerService();
    private QueuesQueueEventService queueEventService = new QueuesQueueEventService();

    public QueuesFlowProducerService() {
    }

    @TestVisible
    private QueuesFlowProducerService(
        final QueuesDefinitionService definitionService,
        final QueuesJobService jobService,
        final QueuesProcessorService processorService,
        final QueuesQueueableDispatchService queueableDispatchService,
        final QueuesRuntimeService runtimeService,
        final QueuesWorkerService workerService,
        final QueuesQueueEventService queueEventService
    ) {
        this.definitionService = definitionService;
        this.jobService = jobService;
        this.processorService = processorService;
        this.queueableDispatchService = queueableDispatchService;
        this.runtimeService = runtimeService;
        this.workerService = workerService;
        this.queueEventService = queueEventService;
    }

    // <editor-fold desc="Flows">
    public Queues.JobNode add(final Queues.FlowJob flow) {
        return add(flow, new Queues.FlowOpts());
    }

    public Queues.JobNode add(final Queues.FlowJob flow, final Queues.FlowOpts opts) {
        return addBulk(new List<Queues.FlowJob>{ flow }, opts)[0];
    }

    public List<Queues.JobNode> addBulk(final List<Queues.FlowJob> flows) {
        return addBulk(flows, new Queues.FlowOpts());
    }

    public List<Queues.JobNode> addBulk(final List<Queues.FlowJob> flows, final Queues.FlowOpts opts) {
        QueuesSupport.notNull(flows, 'flows');
        validateFlowJobs(flows);
        if (flows.isEmpty()) {
            return new List<Queues.JobNode>();
        }

        Queues.FlowOpts resolvedOpts = opts ?? new Queues.FlowOpts();
        Queues.ExecutionMode executionMode = resolvedOpts.getExecutionMode();
        Datetime now = QueuesSupport.now();
        List<FlowNodeState> rootStates = toRootStates(flows);
        List<FlowNodeState> states = flatten(rootStates);
        validateUniqueExternalIds(states);
        validateFlow(states, executionMode, now);

        Map<String, QueueDefinition__mdt> queueDefinitionsByName = requireQueueDefinitions(states);
        Map<String, Processor__mdt> processorByClass = processorService.requireProcessors(toJobData(states));
        Map<String, Map<String, Job__c>> existingJobsByQueueAndExternalId = queryExistingJobs(states);

        Savepoint savepoint = Database.setSavepoint();
        queueEventService.beginScope();
        try {
            insertStatesByLevel(
                rootStates,
                queueDefinitionsByName,
                processorByClass,
                existingJobsByQueueAndExternalId,
                executionMode,
                now
            );
            validateExistingDependencyStates(states);
            resolveParentDependenciesAsSystem(toJobs(states));
            dispatchRunnableJobs(resolveInsertedRunnableJobs(states), now, false);
            queueEventService.commitScope();
            return toJobNodes(rootStates);
        } catch (Exception ex) {
            Database.rollback(savepoint);
            queueEventService.abandonScope();
            throw ex;
        }
    }

    public Queues.JobNode getFlow(final Id rootJobId) {
        Job__c root = jobService.requireJob(rootJobId);
        return toJobNode(root, queryFlowChildrenByParentId(new Set<Id>{ root.Id }));
    }
    // </editor-fold>

    // <editor-fold desc="Dependencies">
    public void resolveParentDependenciesAsSystem(final List<Job__c> jobs) {
        resolveParentDependenciesAsSystem(jobs, new Set<String>());
    }

    public void resolveParentDependenciesAsSystem(
        final List<Job__c> jobs,
        final Set<String> suppressedWorkerQueueNames
    ) {
        resolveParentDependenciesAsSystem(jobs, suppressedWorkerQueueNames, new Set<String>());
    }

    public void resolveParentDependenciesAsSystem(
        final List<Job__c> jobs,
        final Set<String> suppressedWorkerQueueNames,
        final Set<String> lockedRuntimeQueueNames
    ) {
        resolveParentDependenciesAsSystem(jobs, suppressedWorkerQueueNames, lockedRuntimeQueueNames, false);
    }

    public void deferParentDependenciesAsSystem(final List<Job__c> jobs) {
        resolveParentDependenciesAsSystem(jobs, new Set<String>(), new Set<String>(), true);
    }

    public void resolveRemovedParentDependenciesAsSystem(final List<Job__c> removedJobs) {
        resolveRemovedParentDependenciesAsSystem(removedJobs, new Set<String>());
    }

    public void resolveRemovedParentDependenciesAsSystem(
        final List<Job__c> removedJobs,
        final Set<String> suppressedWorkerQueueNames
    ) {
        resolveRemovedParentDependenciesAsSystem(removedJobs, suppressedWorkerQueueNames, new Set<String>());
    }

    public void resolveRemovedParentDependenciesAsSystem(
        final List<Job__c> removedJobs,
        final Set<String> suppressedWorkerQueueNames,
        final Set<String> lockedRuntimeQueueNames
    ) {
        resolveParentIdsAsSystem(
            QueuesSupport.toIdSet(removedJobs, 'Parent__c'),
            suppressedWorkerQueueNames,
            new Set<Id>(),
            lockedRuntimeQueueNames
        );
    }

    public void continueParentDependencyResolutionAsSystem(
        final Set<Id> parentIds,
        final Set<Id> cleanupParentIds,
        final Set<String> suppressedWorkerQueueNames
    ) {
        resolveParentIdsAsSystem(parentIds, suppressedWorkerQueueNames, cleanupParentIds, new Set<String>());
    }

    public Integer repairStaleParentDependenciesAsSystem(final String queueName, final Datetime staleBefore) {
        List<Job__c> parents = jobService.queryStaleWaitingChildrenJobsAsSystem(
            queueName,
            staleBefore,
            DEPENDENCY_REPAIR_BATCH_SIZE
        );
        jobService.updateJobsAsSystem(toDependencyRepairCheckedJobs(parents, QueuesSupport.now()));
        return resolveParentIdsAsSystem(QueuesSupport.toIdSet(parents), new Set<String>());
    }

    public Integer repairStaleRemovedFailureDependenciesAsSystem(final String queueName, final Datetime staleBefore) {
        return detachDependenciesAsSystem(
            jobService.queryStaleRemovedDependenciesForUpdateAsSystem(
                queueName,
                staleBefore,
                DEPENDENCY_CLEANUP_BATCH_SIZE
            )
        );
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private static void validateFlowJobs(final List<Queues.FlowJob> flows) {
        for (Queues.FlowJob flow : flows) {
            QueuesSupport.notNull(flow, 'flow');
        }
    }

    private static List<FlowNodeState> toRootStates(final List<Queues.FlowJob> flows) {
        List<FlowNodeState> rootStates = new List<FlowNodeState>();
        Set<Queues.FlowJob> visitedFlows = new Set<Queues.FlowJob>();
        for (Queues.FlowJob flow : flows) {
            rootStates.add(toNodeState(flow, null, visitedFlows, new Set<Queues.FlowJob>()));
        }
        return rootStates;
    }

    private static FlowNodeState toNodeState(
        final Queues.FlowJob flow,
        final FlowNodeState parent,
        final Set<Queues.FlowJob> visitedFlows,
        final Set<Queues.FlowJob> ancestorFlows
    ) {
        QueuesSupport.notBlank(flow.getName(), 'name');
        QueuesSupport.notBlank(flow.getQueueName(), 'queueName');
        QueuesSupport.notBlank(flow.getProcessor(), 'processor');
        QueuesSupport.isTrue(
            flow.getOpts().getParentJobId() == null,
            new Queues.QueueException(INVALID_PARENT_OPTION_MESSAGE)
        );
        QueuesSupport.isTrue(!ancestorFlows.contains(flow), new Queues.QueueException(FLOW_CYCLE_MESSAGE));
        QueuesSupport.isTrue(!visitedFlows.contains(flow), new Queues.QueueException(FLOW_INSTANCE_REUSE_MESSAGE));
        visitedFlows.add(flow);
        ancestorFlows.add(flow);

        FlowNodeState state = new FlowNodeState(flow, parent);
        for (Queues.FlowJob child : flow.getChildren()) {
            QueuesSupport.notNull(child, 'child');
            state.children.add(toNodeState(child, state, visitedFlows, ancestorFlows));
        }
        ancestorFlows.remove(flow);
        return state;
    }

    private static List<FlowNodeState> flatten(final List<FlowNodeState> rootStates) {
        List<FlowNodeState> states = new List<FlowNodeState>();
        for (FlowNodeState rootState : rootStates) {
            addFlattenedState(states, rootState);
        }
        return states;
    }

    private static void addFlattenedState(final List<FlowNodeState> states, final FlowNodeState state) {
        states.add(state);
        for (FlowNodeState child : state.children) {
            addFlattenedState(states, child);
        }
    }

    private static List<Queues.JobData> toJobData(final List<FlowNodeState> states) {
        List<Queues.JobData> jobs = new List<Queues.JobData>();
        for (FlowNodeState state : states) {
            jobs.add(toJobData(state));
        }
        return jobs;
    }

    private static Queues.JobData toJobData(final FlowNodeState state) {
        Queues.FlowJob flowJob = state.flowJob;
        return new Queues.JobData(flowJob.getName(), flowJob.getProcessor(), flowJob.getData(), flowJob.getOpts());
    }

    private static Set<String> toQueueNames(final List<FlowNodeState> states) {
        Set<String> queueNames = new Set<String>();
        for (FlowNodeState state : states) {
            queueNames.add(state.flowJob.getQueueName());
        }
        return queueNames;
    }

    private static Map<String, Set<String>> toExternalIdsByQueue(final List<FlowNodeState> states) {
        Map<String, Set<String>> externalIdsByQueue = new Map<String, Set<String>>();
        for (FlowNodeState state : states) {
            String externalId = state.flowJob.getOpts().getJobId();
            if (String.isBlank(externalId)) {
                continue;
            }

            String queueName = state.flowJob.getQueueName();
            Set<String> externalIds = externalIdsByQueue.get(queueName) ?? new Set<String>();
            externalIds.add(externalId);
            externalIdsByQueue.put(queueName, externalIds);
        }
        return externalIdsByQueue;
    }

    private static void validateUniqueExternalIds(final List<FlowNodeState> states) {
        Set<String> scopedExternalIds = new Set<String>();
        for (FlowNodeState state : states) {
            String externalId = state.flowJob.getOpts().getJobId();
            if (String.isBlank(externalId)) {
                continue;
            }

            String scopedExternalId = QueuesSupport.normalizeCaseInsensitiveKey(
                String.format('{0}:{1}', new List<String>{ state.flowJob.getQueueName(), externalId })
            );
            QueuesSupport.isTrue(
                !scopedExternalIds.contains(scopedExternalId),
                new Queues.QueueException(FLOW_JOB_ID_UNIQUE_MESSAGE)
            );
            scopedExternalIds.add(scopedExternalId);
        }
    }

    private static void validateFlow(
        final List<FlowNodeState> states,
        final Queues.ExecutionMode executionMode,
        final Datetime now
    ) {
        QueuesSupport.notNull(executionMode, 'executionMode');
        if (!QueuesSupport.isQueueableExecutionMode(executionMode)) {
            return;
        }

        for (FlowNodeState state : states) {
            Integer delayMinutes = resolveQueueableDelayMinutes(state.flowJob.getOpts(), now);
            QueuesQueueableTransportService.requireValidNativeQueueableDelayMinutes(delayMinutes);
        }
    }

    private static Integer resolveQueueableDelayMinutes(final Queues.JobOptions options, final Datetime now) {
        if (options.getAvailableAt() != null) {
            return QueuesQueueableTransportService.resolveNativeQueueableDelayMinutes(options.getAvailableAt(), now);
        }
        return options.getDelay() ?? 0;
    }

    private static Boolean isDuplicateInsertStatusCode(final System.StatusCode statusCode) {
        return statusCode == System.StatusCode.DUPLICATE_VALUE || statusCode == System.StatusCode.DUPLICATES_DETECTED;
    }

    private static Boolean isDuplicateJobInsert(final Database.SaveResult saveResult) {
        for (Database.Error error : saveResult.getErrors()) {
            if (!isDuplicateInsertStatusCode(error.getStatusCode())) {
                return false;
            }
        }
        return true;
    }

    private static Exception buildInsertFailure(final Database.SaveResult saveResult) {
        List<Database.Error> errors = saveResult.getErrors();
        String message = errors.isEmpty() ? FLOW_INSERT_FAILED_MESSAGE : errors[0].getMessage();
        return new Queues.QueueException(message);
    }

    private static void requireMatchingExecutionMode(final Job__c job, final Queues.ExecutionMode executionMode) {
        QueuesSupport.isTrue(
            QueuesSupport.toExecutionMode(job.ExecutionMode__c) == executionMode,
            new Queues.QueueException(EXISTING_FLOW_EXECUTION_MODE_MESSAGE)
        );
    }

    private static void requireMatchingParent(final Job__c job, final FlowNodeState state) {
        Id expectedParentId = state.parent?.job?.Id;
        QueuesSupport.isTrue(
            job.Parent__c == expectedParentId,
            new Queues.QueueException(EXISTING_FLOW_PARENT_MESSAGE)
        );
    }

    private static void applyWaitingChildrenState(final Job__c job, final FlowNodeState state) {
        if (state.children.isEmpty()) {
            return;
        }

        job.State__c = Queues.State.WAITING_CHILDREN.name();
    }

    private static Boolean isRunnableJob(final Job__c job) {
        return RUNNABLE_JOB_STATES.contains(QueuesSupport.toState(job?.State__c));
    }

    private static Boolean isTerminalJob(final Job__c job) {
        return TERMINAL_JOB_STATES.contains(QueuesSupport.toState(job?.State__c));
    }

    private static Map<Queues.ExecutionMode, Map<String, List<Job__c>>> groupJobsByExecutionModeAndQueue(
        final List<Job__c> jobs
    ) {
        Map<Queues.ExecutionMode, Map<String, List<Job__c>>> jobsByModeAndQueue = new Map<Queues.ExecutionMode, Map<String, List<Job__c>>>();
        for (Job__c job : jobs) {
            if (!isRunnableJob(job)) {
                continue;
            }

            Queues.ExecutionMode executionMode = QueuesSupport.toExecutionMode(job.ExecutionMode__c);
            Map<String, List<Job__c>> jobsByQueue = jobsByModeAndQueue.get(executionMode) ??
                new Map<String, List<Job__c>>();
            List<Job__c> queueJobs = jobsByQueue.get(job.QueueName__c) ?? new List<Job__c>();
            queueJobs.add(job);
            jobsByQueue.put(job.QueueName__c, queueJobs);
            jobsByModeAndQueue.put(executionMode, jobsByQueue);
        }
        return jobsByModeAndQueue;
    }

    private static List<Job__c> resolveInsertedRunnableJobs(final List<FlowNodeState> states) {
        List<Job__c> jobs = new List<Job__c>();
        for (FlowNodeState state : states) {
            if (!state.wasInserted || !state.children.isEmpty() || !isRunnableJob(state.job)) {
                continue;
            }

            jobs.add(state.job);
        }
        return jobs;
    }

    private static List<Job__c> toJobs(final List<FlowNodeState> states) {
        List<Job__c> jobs = new List<Job__c>();
        for (FlowNodeState state : states) {
            if (state.job != null) {
                jobs.add(state.job);
            }
        }
        return jobs;
    }

    private static Set<Id> toTerminalParentIds(final List<Job__c> jobs) {
        Set<Id> parentIds = new Set<Id>();
        for (Job__c job : jobs) {
            if (job.Parent__c != null && isTerminalJob(job)) {
                parentIds.add(job.Parent__c);
            }
        }
        return parentIds;
    }

    private static List<Queues.JobNode> toJobNodes(final List<FlowNodeState> states) {
        List<Queues.JobNode> nodes = new List<Queues.JobNode>();
        for (FlowNodeState state : states) {
            nodes.add(toJobNode(state));
        }
        return nodes;
    }

    private static Queues.JobNode toJobNode(final FlowNodeState state) {
        return new Queues.JobNode(new Queues.Job(state.job.Id), toJobNodes(state.children));
    }

    private static Queues.JobNode toJobNode(final Job__c job, final Map<Id, List<Job__c>> childrenByParentId) {
        return new Queues.JobNode(
            new Queues.Job(job.Id),
            toJobNodes(childrenByParentId.get(job.Id), childrenByParentId)
        );
    }

    private static List<Queues.JobNode> toJobNodes(
        final List<Job__c> jobs,
        final Map<Id, List<Job__c>> childrenByParentId
    ) {
        List<Queues.JobNode> nodes = new List<Queues.JobNode>();
        if (jobs == null) {
            return nodes;
        }

        for (Job__c job : jobs) {
            nodes.add(toJobNode(job, childrenByParentId));
        }
        return nodes;
    }

    private static void deferParentDependencyResolution(
        final Set<Id> parentIds,
        final Set<String> suppressedWorkerQueueNames
    ) {
        if (parentIds == null || parentIds.isEmpty()) {
            return;
        }

        QueuesFlowProducerDependencyResolver.tryEnqueue(
            parentIds,
            new Set<Id>(),
            suppressedWorkerQueueNames ?? new Set<String>()
        );
    }

    private Map<Id, List<Job__c>> queryFlowChildrenByParentId(final Set<Id> rootJobIds) {
        Map<Id, List<Job__c>> childrenByParentId = new Map<Id, List<Job__c>>();
        Set<Id> currentParentIds = new Set<Id>();
        currentParentIds.addAll(rootJobIds);
        Set<Id> queriedParentIds = new Set<Id>();

        while (!currentParentIds.isEmpty()) {
            currentParentIds.removeAll(queriedParentIds);
            queriedParentIds.addAll(currentParentIds);
            Set<Id> queryParentIds = currentParentIds;

            currentParentIds = new Set<Id>();
            for (Job__c child : jobService.queryJobsByParentIds(queryParentIds)) {
                List<Job__c> children = childrenByParentId.get(child.Parent__c) ?? new List<Job__c>();
                children.add(child);
                childrenByParentId.put(child.Parent__c, children);
                currentParentIds.add(child.Id);
            }
        }
        return childrenByParentId;
    }

    private static Queues.DependencyFailurePolicy toDependencyFailurePolicy(final String policyValue) {
        String resolvedPolicyValue = policyValue ?? DEFAULT_DEPENDENCY_POLICY;
        return resolvedPolicyValue == DEFAULT_DEPENDENCY_POLICY
            ? null
            : Queues.DependencyFailurePolicy.valueOf(resolvedPolicyValue);
    }

    private static Queues.DependencyCanceledPolicy toDependencyCanceledPolicy(final String policyValue) {
        String resolvedPolicyValue = policyValue ?? DEFAULT_DEPENDENCY_POLICY;
        return resolvedPolicyValue == DEFAULT_DEPENDENCY_POLICY
            ? null
            : Queues.DependencyCanceledPolicy.valueOf(resolvedPolicyValue);
    }

    private static Set<Id> takeParentBatch(final Set<Id> parentIds) {
        Set<Id> parentBatchIds = new Set<Id>();
        for (Id parentId : parentIds) {
            parentBatchIds.add(parentId);
            if (parentBatchIds.size() >= DEPENDENCY_RESOLUTION_PARENT_BATCH_SIZE) {
                break;
            }
        }
        return parentBatchIds;
    }

    private static List<Job__c> toDependencyRepairCheckedJobs(final List<Job__c> parents, final Datetime checkedAt) {
        List<Job__c> checkedParents = new List<Job__c>();
        for (Job__c parent : parents) {
            checkedParents.add(new Job__c(Id = parent.Id, DependencyRepairCheckedAt__c = checkedAt));
        }
        return checkedParents;
    }

    private static Boolean hasDependencyResolutionHeadroom() {
        QueuesSupport.GovernorHeadroom headroom = QueuesSupport.currentGovernorHeadroom();
        return hasDependencyResolutionHeadroom(
            headroom.dmlRows,
            headroom.dmlStatements,
            headroom.queries,
            headroom.cpuMs
        );
    }

    @TestVisible
    private static Boolean hasDependencyResolutionHeadroom(
        final Integer remainingDmlRows,
        final Integer remainingDmlStatements,
        final Integer remainingQueries,
        final Integer remainingCpuMs
    ) {
        return remainingDmlRows >= DEPENDENCY_RESOLUTION_DML_ROW_HEADROOM &&
            remainingDmlStatements >= DEPENDENCY_RESOLUTION_DML_STATEMENT_HEADROOM &&
            remainingQueries >= DEPENDENCY_RESOLUTION_QUERY_HEADROOM &&
            remainingCpuMs >= DEPENDENCY_RESOLUTION_CPU_MS_HEADROOM;
    }

    private static List<FlowNodeState> nextLevel(final List<FlowNodeState> currentLevel) {
        List<FlowNodeState> nextLevel = new List<FlowNodeState>();
        for (FlowNodeState state : currentLevel) {
            nextLevel.addAll(state.children);
        }
        return nextLevel;
    }

    private static Job__c findExistingJob(
        final FlowNodeState state,
        final Map<String, Map<String, Job__c>> existingJobsByQueueAndExternalId
    ) {
        String externalId = state.flowJob.getOpts().getJobId();
        if (String.isBlank(externalId)) {
            return null;
        }

        Map<String, Job__c> existingJobsByExternalId = existingJobsByQueueAndExternalId.get(
            state.flowJob.getQueueName()
        );
        return existingJobsByExternalId.get(QueuesSupport.normalizeCaseInsensitiveKey(externalId));
    }

    private static void requireExistingJob(
        final Job__c existingJob,
        final FlowNodeState state,
        final Queues.ExecutionMode executionMode
    ) {
        requireMatchingExecutionMode(existingJob, executionMode);
        requireMatchingParent(existingJob, state);
    }

    private static void validateExistingDependencyStates(final List<FlowNodeState> states) {
        for (FlowNodeState state : states) {
            if (state.wasInserted || state.children.isEmpty()) {
                continue;
            }

            for (FlowNodeState child : state.children) {
                QueuesSupport.isTrue(!child.wasInserted, new Queues.QueueException(EXISTING_FLOW_EXTENSION_MESSAGE));
                if (QueuesSupport.toState(state.job.State__c) == Queues.State.WAITING_CHILDREN) {
                    continue;
                }
                QueuesSupport.isTrue(
                    isTerminalJob(child.job),
                    new Queues.QueueException(EXISTING_FLOW_DEPENDENCIES_MESSAGE)
                );
            }
        }
    }

    private void resolveParentDependenciesAsSystem(
        final List<Job__c> jobs,
        final Set<String> suppressedWorkerQueueNames,
        final Set<String> lockedRuntimeQueueNames,
        final Boolean deferResolution
    ) {
        Set<Id> parentIds = toTerminalParentIds(jobs);
        detachRemovedFailureDependenciesAsSystem(jobs);
        if (deferResolution == true) {
            deferParentDependencyResolution(parentIds, suppressedWorkerQueueNames);
            return;
        }
        resolveParentIdsAsSystem(parentIds, suppressedWorkerQueueNames, new Set<Id>(), lockedRuntimeQueueNames);
    }

    private Integer resolveParentIdsAsSystem(final Set<Id> parentIds, final Set<String> suppressedWorkerQueueNames) {
        return resolveParentIdsAsSystem(parentIds, suppressedWorkerQueueNames, new Set<Id>(), new Set<String>());
    }

    private Integer resolveParentIdsAsSystem(
        final Set<Id> parentIds,
        final Set<String> suppressedWorkerQueueNames,
        final Set<Id> cleanupParentIds,
        final Set<String> lockedRuntimeQueueNames
    ) {
        Set<Id> currentParentIds = new Set<Id>(parentIds ?? new Set<Id>());
        Set<String> suppressedQueueNames = suppressedWorkerQueueNames ?? new Set<String>();
        Set<Id> currentCleanupParentIds = new Set<Id>(cleanupParentIds ?? new Set<Id>());
        Set<String> currentLockedRuntimeQueueNames = new Set<String>(lockedRuntimeQueueNames ?? new Set<String>());
        Integer resolvedParentCount = 0;
        Integer remainingBatchCount = forceDependencyResolutionBatchCountLimit ??
            DEPENDENCY_RESOLUTION_BATCH_COUNT_LIMIT;
        while (!currentParentIds.isEmpty() && remainingBatchCount > 0 && hasDependencyResolutionHeadroom()) {
            Set<Id> parentBatchIds = takeParentBatch(currentParentIds);
            currentParentIds.removeAll(parentBatchIds);
            if (!lockWorkerParentRuntimes(parentBatchIds, suppressedQueueNames, currentLockedRuntimeQueueNames)) {
                currentParentIds.addAll(parentBatchIds);
                break;
            }
            List<Job__c> updatedParents = resolveParentDependencyLevel(parentBatchIds, currentCleanupParentIds);
            resolvedParentCount += updatedParents.size();
            dispatchRunnableJobs(updatedParents, QueuesSupport.now(), true, suppressedQueueNames);
            currentParentIds.addAll(toTerminalParentIds(updatedParents));
            remainingBatchCount--;
        }
        currentCleanupParentIds = cleanupRemovedFailureDependenciesAsSystem(currentCleanupParentIds);
        if (!currentParentIds.isEmpty() || !currentCleanupParentIds.isEmpty()) {
            QueuesFlowProducerDependencyResolver.tryEnqueue(
                currentParentIds,
                currentCleanupParentIds,
                suppressedQueueNames
            );
        }
        return resolvedParentCount;
    }

    private Boolean lockWorkerParentRuntimes(
        final Set<Id> parentIds,
        final Set<String> suppressedWorkerQueueNames,
        final Set<String> lockedRuntimeQueueNames
    ) {
        Set<String> queueNameSet = new Set<String>();
        for (Job__c parent : jobService.queryJobsAsSystem(parentIds)) {
            if (
                QueuesSupport.toExecutionMode(parent.ExecutionMode__c) == Queues.ExecutionMode.WORKER &&
                !suppressedWorkerQueueNames.contains(parent.QueueName__c) &&
                !lockedRuntimeQueueNames.contains(parent.QueueName__c)
            ) {
                queueNameSet.add(parent.QueueName__c);
            }
        }

        List<String> queueNames = new List<String>(queueNameSet);
        queueNames.sort();
        if (!lockedRuntimeQueueNames.isEmpty() && !queueNames.isEmpty()) {
            return false;
        }

        for (String queueName : queueNames) {
            runtimeService.ensureRuntimeAsSystem(queueName);
            runtimeService.lockRuntimeAsSystem(queueName);
            lockedRuntimeQueueNames.add(queueName);
        }
        return true;
    }

    private Map<String, QueueDefinition__mdt> requireQueueDefinitions(final List<FlowNodeState> states) {
        Set<String> queueNames = toQueueNames(states);
        Map<String, QueueDefinition__mdt> queueDefinitionsByName = definitionService.findQueueDefinitionsByQueueNamesAsSystem(
            queueNames
        );
        for (String queueName : queueNames) {
            QueueDefinition__mdt definition = queueDefinitionsByName.get(queueName);
            QueuesSupport.isTrue(
                definition != null,
                new Queues.ConfigurationException(String.format(MISSING_QUEUE_MESSAGE, new List<String>{ queueName }))
            );
            QueuesSupport.isTrue(
                QueuesSupport.isActive(definition),
                new Queues.ConfigurationException(String.format(INACTIVE_QUEUE_MESSAGE, new List<String>{ queueName }))
            );
        }
        return queueDefinitionsByName;
    }

    private Map<String, Map<String, Job__c>> queryExistingJobs(final List<FlowNodeState> states) {
        Map<String, Map<String, Job__c>> jobsByQueueAndExternalId = new Map<String, Map<String, Job__c>>();
        Map<String, Set<String>> externalIdsByQueue = toExternalIdsByQueue(states);
        for (String queueName : externalIdsByQueue.keySet()) {
            jobsByQueueAndExternalId.put(
                queueName,
                jobService.getJobsByExternalId(queueName, externalIdsByQueue.get(queueName))
            );
        }
        return jobsByQueueAndExternalId;
    }

    private void insertStatesByLevel(
        final List<FlowNodeState> rootStates,
        final Map<String, QueueDefinition__mdt> queueDefinitionsByName,
        final Map<String, Processor__mdt> processorByClass,
        final Map<String, Map<String, Job__c>> existingJobsByQueueAndExternalId,
        final Queues.ExecutionMode executionMode,
        final Datetime now
    ) {
        List<FlowNodeState> currentLevel = rootStates;
        while (!currentLevel.isEmpty()) {
            insertLevel(
                currentLevel,
                queueDefinitionsByName,
                processorByClass,
                existingJobsByQueueAndExternalId,
                executionMode,
                now
            );
            currentLevel = nextLevel(currentLevel);
        }
    }

    private void insertLevel(
        final List<FlowNodeState> states,
        final Map<String, QueueDefinition__mdt> queueDefinitionsByName,
        final Map<String, Processor__mdt> processorByClass,
        final Map<String, Map<String, Job__c>> existingJobsByQueueAndExternalId,
        final Queues.ExecutionMode executionMode,
        final Datetime now
    ) {
        List<Job__c> jobsToInsert = new List<Job__c>();
        List<FlowNodeState> statesToInsert = new List<FlowNodeState>();
        List<Job__c> idempotentJobsToInsert = new List<Job__c>();
        List<FlowNodeState> idempotentStatesToInsert = new List<FlowNodeState>();

        for (FlowNodeState state : states) {
            Job__c existingJob = findExistingJob(state, existingJobsByQueueAndExternalId);
            if (existingJob != null) {
                requireExistingJob(existingJob, state, executionMode);
                state.job = existingJob;
                continue;
            }

            Job__c job = mapToJob(state, queueDefinitionsByName, processorByClass, executionMode, now);
            if (String.isBlank(job.ExternalId__c)) {
                jobsToInsert.add(job);
                statesToInsert.add(state);
            } else {
                idempotentJobsToInsert.add(job);
                idempotentStatesToInsert.add(state);
            }
        }

        jobService.insertJobs(jobsToInsert);
        for (Integer index = 0; index < jobsToInsert.size(); index++) {
            statesToInsert[index].job = jobsToInsert[index];
            statesToInsert[index].wasInserted = true;
        }
        reconcileIdempotentInserts(
            idempotentJobsToInsert,
            idempotentStatesToInsert,
            jobService.insertJobsAllowingPartialSuccess(idempotentJobsToInsert)
        );
    }

    private Job__c mapToJob(
        final FlowNodeState state,
        final Map<String, QueueDefinition__mdt> queueDefinitionsByName,
        final Map<String, Processor__mdt> processorByClass,
        final Queues.ExecutionMode executionMode,
        final Datetime now
    ) {
        Queues.JobData jobData = toJobData(state);
        Job__c job = jobService.mapToJob(
            state.flowJob.getQueueName(),
            jobData,
            queueDefinitionsByName.get(state.flowJob.getQueueName()),
            processorByClass.get(jobData.getProcessor()),
            now,
            executionMode
        );
        job.Parent__c = state.parent?.job?.Id;
        applyWaitingChildrenState(job, state);
        return job;
    }

    private void reconcileIdempotentInserts(
        final List<Job__c> jobs,
        final List<FlowNodeState> states,
        final List<Database.SaveResult> saveResults
    ) {
        QueuesSupport.isTrue(saveResults.size() == jobs.size(), FLOW_INSERT_RESULTS_MISMATCH_MESSAGE);

        for (Integer index = 0; index < jobs.size(); index++) {
            Job__c job = jobs[index];
            Database.SaveResult saveResult = saveResults[index];
            FlowNodeState state = states[index];
            if (saveResult.isSuccess()) {
                job.Id = saveResult.getId();
                state.job = job;
                state.wasInserted = true;
                continue;
            }

            if (isDuplicateJobInsert(saveResult)) {
                state.job = resolveDuplicateJob(job, state);
                continue;
            }

            throw buildInsertFailure(saveResult);
        }
    }

    private Job__c resolveDuplicateJob(final Job__c duplicateJob, final FlowNodeState state) {
        Map<String, Job__c> jobsByExternalId = jobService.getJobsByExternalId(
            duplicateJob.QueueName__c,
            new Set<String>{ duplicateJob.ExternalId__c }
        );
        Job__c resolvedJob = jobsByExternalId.get(
            QueuesSupport.normalizeCaseInsensitiveKey(duplicateJob.ExternalId__c)
        );
        QueuesSupport.isTrue(
            resolvedJob != null,
            String.format(DUPLICATE_FLOW_JOB_RESOLUTION_MESSAGE, new List<String>{ duplicateJob.ExternalId__c })
        );
        requireExistingJob(resolvedJob, state, QueuesSupport.toExecutionMode(duplicateJob.ExecutionMode__c));
        return resolvedJob;
    }

    private List<Job__c> resolveParentDependencyLevel(
        final Set<Id> parentIds,
        final Set<Id> removedDependencyParentIds
    ) {
        Map<Id, Job__c> parentsById = new Map<Id, Job__c>(
            jobService.queryJobsForUpdateAsSystem(parentIds, Queues.State.WAITING_CHILDREN)
        );
        Set<Id> waitingParentIds = parentsById.keySet();
        Map<Id, ChildStateCounts> countsByParentId = queryChildStateCounts(waitingParentIds);
        List<Job__c> parentsToUpdate = new List<Job__c>();
        Datetime now = QueuesSupport.now();
        for (Id parentId : waitingParentIds) {
            Job__c parent = parentsById.get(parentId);
            ChildStateCounts counts = countsByParentId.get(parentId) ?? new ChildStateCounts();
            if (counts.hasRemovedDependency == true) {
                removedDependencyParentIds.add(parentId);
            }
            if (!counts.isResolved()) {
                continue;
            }

            applyResolvedDependencyState(parent, counts, now);
            parentsToUpdate.add(parent);
        }

        jobService.updateJobsAsSystem(parentsToUpdate);
        queueEventService.publishTerminalJobEvents(parentsToUpdate, null, DEPENDENCY_RESOLUTION_COMPONENT);
        return parentsToUpdate;
    }

    private Set<Id> cleanupRemovedFailureDependenciesAsSystem(final Set<Id> parentIds) {
        Set<Id> currentParentIds = new Set<Id>(parentIds ?? new Set<Id>());
        Integer availableCleanupDmlRows =
            Limits.getLimitDmlRows() -
            Limits.getDmlRows() -
            DEPENDENCY_RESOLUTION_DML_ROW_HEADROOM;
        if (currentParentIds.isEmpty() || !hasDependencyResolutionHeadroom() || availableCleanupDmlRows <= 0) {
            return currentParentIds;
        }

        Integer cleanupBatchSize = Math.min(DEPENDENCY_CLEANUP_BATCH_SIZE, availableCleanupDmlRows);
        List<Job__c> dependencies = jobService.queryFailedRemovedDependenciesForUpdateAsSystem(
            currentParentIds,
            cleanupBatchSize
        );
        detachDependenciesAsSystem(dependencies);
        return dependencies.size() >= cleanupBatchSize ? currentParentIds : new Set<Id>();
    }

    private void detachRemovedFailureDependenciesAsSystem(final List<Job__c> jobs) {
        List<Job__c> dependencies = new List<Job__c>();
        for (Job__c job : jobs) {
            if (
                job.Parent__c != null &&
                QueuesSupport.toState(job.State__c) == Queues.State.FAILED &&
                job.DependencyFailurePolicy__c == Queues.DependencyFailurePolicy.REMOVE_DEPENDENCY.name()
            ) {
                dependencies.add(job);
            }
        }
        detachDependenciesAsSystem(dependencies);
    }

    private Integer detachDependenciesAsSystem(final List<Job__c> dependencies) {
        List<Job__c> sparseDependencies = new List<Job__c>();
        for (Job__c dependency : dependencies) {
            dependency.Parent__c = null;
            sparseDependencies.add(new Job__c(Id = dependency.Id, Parent__c = null));
        }
        jobService.updateJobsAsSystem(sparseDependencies);
        return sparseDependencies.size();
    }

    private Map<Id, ChildStateCounts> queryChildStateCounts(final Set<Id> parentIds) {
        Map<Id, ChildStateCounts> countsByParentId = new Map<Id, ChildStateCounts>();
        for (AggregateResult row : jobService.queryChildStateCountsAsSystem(parentIds)) {
            Id parentId = (Id) row.get('parentId');
            ChildStateCounts counts = countsByParentId.get(parentId) ?? new ChildStateCounts();
            counts.add(
                (String) row.get('stateValue'),
                (String) row.get('failurePolicyValue'),
                (String) row.get('canceledPolicyValue'),
                QueuesSupport.toInteger(row.get('rowCount'), 0)
            );
            countsByParentId.put(parentId, counts);
        }
        return countsByParentId;
    }

    private void applyResolvedDependencyState(final Job__c parent, final ChildStateCounts counts, final Datetime now) {
        if (counts.failParent == true) {
            jobService.applyFailure(parent, counts.parentFailureReason, now, true);
            return;
        }
        if (counts.cancelParent == true) {
            QueuesJobService.applyCanceled(parent, CHILD_CANCELED_REASON, now);
            return;
        }

        parent.State__c = parent.AvailableAt__c > now ? Queues.State.DELAYED.name() : Queues.State.WAITING.name();
        parent.FailedReason__c = null;
        parent.PauseRequested__c = false;
        jobService.clearLease(parent);
    }

    private void dispatchRunnableJobs(final List<Job__c> jobs, final Datetime now, final Boolean asSystem) {
        dispatchRunnableJobs(jobs, now, asSystem, new Set<String>());
    }

    private void dispatchRunnableJobs(
        final List<Job__c> jobs,
        final Datetime now,
        final Boolean asSystem,
        final Set<String> suppressedWorkerQueueNames
    ) {
        Map<Queues.ExecutionMode, Map<String, List<Job__c>>> jobsByModeAndQueue = groupJobsByExecutionModeAndQueue(
            jobs
        );
        dispatchWorkerJobs(
            jobsByModeAndQueue.get(Queues.ExecutionMode.WORKER) ?? new Map<String, List<Job__c>>(),
            now,
            suppressedWorkerQueueNames
        );
        dispatchQueueableJobs(
            jobsByModeAndQueue.get(Queues.ExecutionMode.QUEUEABLE_SERIAL) ?? new Map<String, List<Job__c>>(),
            Queues.ExecutionMode.QUEUEABLE_SERIAL,
            asSystem
        );
        dispatchQueueableJobs(
            jobsByModeAndQueue.get(Queues.ExecutionMode.QUEUEABLE_CONCURRENT) ?? new Map<String, List<Job__c>>(),
            Queues.ExecutionMode.QUEUEABLE_CONCURRENT,
            asSystem
        );
    }

    private void dispatchWorkerJobs(
        final Map<String, List<Job__c>> jobsByQueue,
        final Datetime now,
        final Set<String> suppressedWorkerQueueNames
    ) {
        for (String queueName : jobsByQueue.keySet()) {
            if (suppressedWorkerQueueNames.contains(queueName)) {
                continue;
            }
            workerService.scheduleWorkerForJobs(queueName, jobsByQueue.get(queueName), now);
        }
    }

    private void dispatchQueueableJobs(
        final Map<String, List<Job__c>> jobsByQueue,
        final Queues.ExecutionMode executionMode,
        final Boolean asSystem
    ) {
        for (String queueName : jobsByQueue.keySet()) {
            List<Job__c> jobs = jobsByQueue.get(queueName);
            QueueableDispatch__c dispatch = queueableDispatchService.createWaitingDispatch(
                queueName,
                null,
                executionMode,
                jobs,
                asSystem
            );
            jobService.updateJobs(jobs, asSystem);
            queueableDispatchService.enqueueDispatcher(dispatch, asSystem);
        }
    }

    private class FlowNodeState {
        private final Queues.FlowJob flowJob;
        private final FlowNodeState parent;
        private final List<FlowNodeState> children = new List<FlowNodeState>();
        private Job__c job;
        private Boolean wasInserted = false;

        private FlowNodeState(final Queues.FlowJob flowJob, final FlowNodeState parent) {
            this.flowJob = flowJob;
            this.parent = parent;
        }
    }

    private class ChildStateCounts {
        private Integer totalCount = 0;
        private Integer resolvedCount = 0;
        private Boolean failParent = false;
        private Boolean cancelParent = false;
        private Boolean continueParent = false;
        private Boolean hasRemovedDependency = false;
        private String parentFailureReason;

        private void add(
            final String stateValue,
            final String failurePolicyValue,
            final String canceledPolicyValue,
            final Integer count
        ) {
            Integer safeCount = count ?? 0;
            totalCount += safeCount;
            Queues.State state = QueuesSupport.toState(stateValue);
            if (state == Queues.State.COMPLETED) {
                resolvedCount += safeCount;
                return;
            }
            if (state == Queues.State.FAILED) {
                applyFailurePolicy(toDependencyFailurePolicy(failurePolicyValue), safeCount);
                return;
            }
            if (state == Queues.State.CANCELED) {
                applyCanceledPolicy(toDependencyCanceledPolicy(canceledPolicyValue), safeCount);
            }
        }

        private Boolean isResolved() {
            return failParent == true || cancelParent == true || continueParent == true || totalCount == resolvedCount;
        }

        private void applyFailurePolicy(final Queues.DependencyFailurePolicy policy, final Integer count) {
            switch on policy {
                when FAIL_PARENT {
                    applyFailParent(CHILD_FAILED_REASON);
                }
                when CONTINUE_PARENT {
                    continueParent = true;
                }
                when IGNORE_DEPENDENCY {
                    resolvedCount += count;
                }
                when REMOVE_DEPENDENCY {
                    resolvedCount += count;
                    hasRemovedDependency = true;
                }
            }
        }

        private void applyCanceledPolicy(final Queues.DependencyCanceledPolicy policy, final Integer count) {
            switch on policy {
                when CANCEL_PARENT {
                    cancelParent = true;
                }
                when FAIL_PARENT {
                    applyFailParent(CHILD_CANCELED_REASON);
                }
                when IGNORE_DEPENDENCY {
                    resolvedCount += count;
                }
            }
        }

        private void applyFailParent(final String reason) {
            failParent = true;
            parentFailureReason = parentFailureReason ?? reason;
        }
    }
    // </editor-fold>
}
