@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesE2ECatastrophicProcessor implements Queues.JobProcessor {
    public Queues.ProcessResult process(final Queues.JobContext ctx) {
        // Force an uncatchable processor failure so e2e probes can exercise
        // catastrophic worker or queueable recovery paths.
        while (true) {
            List<Job__c> jobs = [SELECT Id FROM Job__c LIMIT 0];
        }

        return ctx.complete(new Map<String, Object>{ 'status' => 'unreachable' });
    }
}
