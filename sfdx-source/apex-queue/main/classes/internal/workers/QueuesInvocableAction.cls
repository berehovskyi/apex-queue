/**
 * @description Invocable action entry points used by the queue Flows.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesInvocableAction {
    // <editor-fold desc="Invocable">
    @InvocableMethod(Label='Queues Invocable Job Handoff' Callout=true)
    public static void run(final List<FlowInput> inputs) {
        Set<Id> claimJobIds = new Set<Id>();
        List<FlowInput> executeInputs = new List<FlowInput>();
        for (FlowInput input : inputs ?? new List<FlowInput>()) {
            switch on Phase.valueOf(input.phase) {
                when CLAIM {
                    claimJobIds.add(input.jobId);
                }
                when EXECUTE {
                    executeInputs.add(input);
                }
            }
        }

        QueuesInvocableExecutionService service = new QueuesInvocableExecutionService();
        if (!executeInputs.isEmpty()) {
            QueuesSupport.isTrue(
                claimJobIds.isEmpty() && executeInputs.size() == 1,
                new Queues.ConfigurationException('Invocable execute phase expects exactly one input.')
            );
            executeSafely(service, executeInputs[0].jobId);
            return;
        }

        service.claimDueJobsAsSystem(claimJobIds);
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private static void executeSafely(final QueuesInvocableExecutionService service, final Id jobId) {
        try {
            service.executeJobAsSystem(jobId);
        } catch (Queues.JobNotFoundException ex) {
            // A stale scheduled path can resume after an admin/user removed the job.
            return;
        }
    }
    // </editor-fold>

    public class FlowInput {
        @InvocableVariable(Label='Job Id' Required=true)
        public Id jobId;
        @InvocableVariable(Label='Phase' Required=true)
        public String phase;
    }

    private enum Phase {
        CLAIM,
        EXECUTE
    }
}
