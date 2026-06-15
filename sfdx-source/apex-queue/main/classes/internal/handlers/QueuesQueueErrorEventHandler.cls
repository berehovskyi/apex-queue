/**
 * @description Persists QueueErrorEvent platform events into durable QueueError records.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesQueueErrorEventHandler {
    private QueuesErrorService errorService = new QueuesErrorService();

    public QueuesQueueErrorEventHandler() {
    }

    @TestVisible
    private QueuesQueueErrorEventHandler(final QueuesErrorService errorService) {
        this.errorService = errorService;
    }

    // <editor-fold desc="Commands">
    public void handle(final List<QueueErrorEvent__e> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        List<QueueError__c> errors = new List<QueueError__c>();
        for (QueueErrorEvent__e event : events) {
            QueueError__c error = mapToQueueError(event);
            if (error != null) {
                errors.add(error);
            }
        }

        errorService.insertQueueErrorsAsSystem(errors);
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    @TestVisible
    private static QueueError__c mapToQueueError(final QueueErrorEvent__e eventRecord) {
        if (String.isBlank(eventRecord?.QueueName__c) || String.isBlank(eventRecord?.Component__c)) {
            return null;
        }

        return new QueueError__c(
            QueueName__c = eventRecord.QueueName__c,
            Component__c = eventRecord.Component__c,
            Job__c = QueuesSupport.toId(eventRecord.JobId__c),
            AsyncApexJobId__c = eventRecord.AsyncApexJobId__c,
            Message__c = eventRecord.Message__c,
            StackTrace__c = eventRecord.StackTrace__c,
            OccurredAt__c = eventRecord.OccurredAt__c ?? QueuesSupport.now()
        );
    }
    // </editor-fold>
}
