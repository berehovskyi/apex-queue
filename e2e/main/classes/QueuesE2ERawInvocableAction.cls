@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesE2ERawInvocableAction {
    @InvocableMethod(Label='Queues E2E Raw Invocable Capture' Callout=true)
    public static void run(final List<FlowInput> inputs) {
        Set<Id> runIds = new Set<Id>();
        for (FlowInput input : inputs ?? new List<FlowInput>()) {
            if (input?.runId != null) {
                runIds.add(input.runId);
            }
        }
        if (runIds.isEmpty()) {
            return;
        }

        Map<Id, RawPerformanceRun__c> runsById = new Map<Id, RawPerformanceRun__c>(
            [
                SELECT Id, RequestedAtMs__c, Status__c
                FROM RawPerformanceRun__c
                WHERE Id IN :runIds
                FOR UPDATE
            ]
        );

        for (FlowInput input : inputs ?? new List<FlowInput>()) {
            RawPerformanceRun__c run = runsById.get(input?.runId);
            if (run == null || run.Status__c != QueuesE2ERawPerformanceProbe.STATUS_WAITING) {
                continue;
            }

            try {
                QueuesE2ERawPerformanceProbe.capture(
                    run.Id,
                    run.RequestedAtMs__c,
                    QueuesE2ERawPerformanceProbe.DEFAULT_DELAY_MS,
                    null
                );
            } catch (Exception ex) {
                QueuesE2ERawPerformanceProbe.captureFailure(run.Id, null, ex);
            }
        }
    }

    public class FlowInput {
        @InvocableVariable(Label='Raw Performance Run Id' Required=true)
        public Id runId;
    }
}
