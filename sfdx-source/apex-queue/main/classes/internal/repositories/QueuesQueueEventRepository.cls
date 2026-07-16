/**
 * @description Publishes queue lifecycle platform events.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public virtual inherited sharing class QueuesQueueEventRepository {
    private static final String QUEUE_EVENTS_COMPONENT = 'queue events';
    private static final String ASYNC_PUBLISH_FAILURE_MESSAGE = 'Queue lifecycle event publication failed asynchronously.';

    private final AccessLevel accessLevel;

    public QueuesQueueEventRepository() {
        this(System.AccessLevel.SYSTEM_MODE);
    }

    public QueuesQueueEventRepository(final AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    public virtual void publishQueueEvents(final List<QueueEvent__e> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        PublishFailureCallback callback = new PublishFailureCallback(
            events,
            new QueuesErrorRepository(System.AccessLevel.SYSTEM_MODE)
        );
        List<Database.SaveResult> publishResults = publishEvents(events, callback);
        throwOnPublishFailure(publishResults);
    }

    @TestVisible
    protected virtual List<Database.SaveResult> publishEvents(final List<SObject> events, final Object callback) {
        return EventBus.publishWithAccessLevel(events, callback, accessLevel);
    }

    private static void throwOnPublishFailure(final List<Database.SaveResult> publishResults) {
        List<String> failureMessages = new List<String>();
        for (Database.SaveResult publishResult : publishResults ?? new List<Database.SaveResult>()) {
            if (publishResult == null || publishResult.isSuccess()) {
                continue;
            }
            for (Database.Error error : publishResult.getErrors()) {
                failureMessages.add(error.getMessage());
            }
        }
        if (!failureMessages.isEmpty()) {
            throw new Queues.QueueException(String.join(failureMessages, '; '));
        }
    }

    public class PublishFailureCallback implements EventBus.EventPublishFailureCallback {
        private final Map<String, String> queueNamesByEventUuid = new Map<String, String>();
        private final QueuesErrorRepository errorRepository;

        public PublishFailureCallback(final List<QueueEvent__e> events, final QueuesErrorRepository errorRepository) {
            this.errorRepository = errorRepository;
            for (QueueEvent__e eventRecord : events ?? new List<QueueEvent__e>()) {
                if (String.isNotBlank(eventRecord?.EventUuid) && String.isNotBlank(eventRecord?.QueueName__c)) {
                    queueNamesByEventUuid.put(eventRecord.EventUuid, eventRecord.QueueName__c);
                }
            }
        }

        public void onFailure(final EventBus.FailureResult result) {
            Set<String> failedQueueNames = new Set<String>();
            for (String eventUuid : result?.getEventUuids() ?? new List<String>()) {
                String queueName = queueNamesByEventUuid.get(eventUuid);
                if (String.isNotBlank(queueName)) {
                    failedQueueNames.add(queueName);
                }
            }
            if (failedQueueNames.isEmpty()) {
                return;
            }

            List<QueueError__c> errors = new List<QueueError__c>();
            Datetime occurredAt = QueuesSupport.now();
            for (String queueName : failedQueueNames) {
                errors.add(
                    new QueueError__c(
                        QueueName__c = queueName,
                        Component__c = QUEUE_EVENTS_COMPONENT,
                        Message__c = ASYNC_PUBLISH_FAILURE_MESSAGE,
                        OccurredAt__c = occurredAt
                    )
                );
            }

            try {
                errorRepository.insertQueueErrors(errors);
            } catch (Exception ex) {
                QueuesSupport.debugError(
                    String.format(
                        'Failed to record asynchronous queue event publication errors: {0}\n{1}',
                        new List<String>{ ex.getMessage(), ex.getStackTraceString() }
                    )
                );
            }
        }
    }
}
