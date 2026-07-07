/**
 * @description Publishes queue error platform events.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public virtual inherited sharing class QueuesErrorEventRepository {
    private final AccessLevel accessLevel;
    private QueuesErrorRepository errorRepository = new QueuesErrorRepository(System.AccessLevel.SYSTEM_MODE);

    public QueuesErrorEventRepository() {
        this(System.AccessLevel.USER_MODE);
    }

    public QueuesErrorEventRepository(final AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    @TestVisible
    private QueuesErrorEventRepository(final QueuesErrorRepository errorRepository) {
        this(System.AccessLevel.USER_MODE);
        this.errorRepository = errorRepository;
    }

    public void publishQueueError(final QueueErrorEvent__e errorEvent) {
        if (errorEvent == null) {
            return;
        }

        PublishFailureCallback callback = new PublishFailureCallback(
            new List<QueueErrorEvent__e>{ errorEvent },
            errorRepository
        );

        try {
            publishEvents(new List<SObject>{ errorEvent }, callback);
        } catch (Exception ex) {
            callback.handleFailure(new List<String>{ errorEvent.EventUuid });
        }
    }

    @TestVisible
    protected virtual void publishEvents(final List<SObject> events, final Object callback) {
        EventBus.publishWithAccessLevel(events, callback, accessLevel);
    }

    public class PublishFailureCallback implements EventBus.EventPublishFailureCallback {
        private final Map<String, QueueErrorInfo> errorInfoByEventUuid = new Map<String, QueueErrorInfo>();
        private final QueuesErrorRepository errorRepository;

        public PublishFailureCallback(
            final List<QueueErrorEvent__e> events,
            final QueuesErrorRepository errorRepository
        ) {
            this.errorRepository = errorRepository;
            for (QueueErrorEvent__e eventRecord : events ?? new List<QueueErrorEvent__e>()) {
                QueueErrorInfo errorInfo = new QueueErrorInfo().fromEvent(eventRecord);
                if (errorInfo != null) {
                    errorInfoByEventUuid.put(errorInfo.eventUuid, errorInfo);
                }
            }
        }

        public void onFailure(final EventBus.FailureResult result) {
            handleFailure(result?.getEventUuids());
        }

        @TestVisible
        private void handleFailure(final List<String> failedEventUuids) {
            List<QueueError__c> errors = new List<QueueError__c>();
            for (String eventUuid : failedEventUuids ?? new List<String>()) {
                QueueErrorInfo errorInfo = errorInfoByEventUuid.get(eventUuid);
                if (errorInfo != null) {
                    errors.add(errorInfo.toQueueError());
                }
            }

            if (errors.isEmpty()) {
                return;
            }

            try {
                errorRepository.insertQueueErrors(errors);
            } catch (Exception ex) {
                logPartialFailure(errors, ex);
            }
        }

        // Retry one row at a time so a single bad record does not erase the entire callback fallback.
        private void logPartialFailure(final List<QueueError__c> errors, final Exception bulkInsertException) {
            RetrySummary retrySummary = retryIndividually(errors);
            if (retrySummary.insertedCount == errors.size()) {
                return;
            }

            String singleInsertFailureMessage = String.format(
                '\nLast single-insert failure: {0}\n{1}',
                new List<String>{
                    retrySummary.lastException.getMessage(),
                    retrySummary.lastException.getStackTraceString()
                }
            );
            QueuesSupport.debugError(
                String.format(
                    'Queues error callback capture failed. Inserted {0} of {1} rows.\nBulk failure: {2}\n{3}{4}',
                    new List<Object>{
                        retrySummary.insertedCount,
                        errors.size(),
                        bulkInsertException.getMessage(),
                        bulkInsertException.getStackTraceString(),
                        singleInsertFailureMessage
                    }
                )
            );
        }

        private RetrySummary retryIndividually(final List<QueueError__c> errors) {
            Integer insertedCount = 0;
            Exception lastException;
            for (QueueError__c error : errors) {
                try {
                    errorRepository.insertQueueError(error);
                    insertedCount++;
                } catch (Exception singleInsertEx) {
                    lastException = singleInsertEx;
                }
            }
            return new RetrySummary(insertedCount, lastException);
        }
    }

    private class RetrySummary {
        private final Integer insertedCount;
        private final Exception lastException;

        private RetrySummary(final Integer insertedCount, final Exception lastException) {
            this.insertedCount = insertedCount ?? 0;
            this.lastException = lastException;
        }
    }

    private class QueueErrorInfo {
        private String eventUuid;
        private String queueName;
        private String component;
        private String jobIdValue;
        private String asyncApexJobId;
        private String message;
        private String stackTrace;
        private Datetime occurredAt;

        private QueueErrorInfo fromEvent(final QueueErrorEvent__e eventRecord) {
            if (
                String.isBlank(eventRecord?.EventUuid) ||
                String.isBlank(eventRecord?.QueueName__c) ||
                String.isBlank(eventRecord?.Component__c)
            ) {
                return null;
            }

            QueueErrorInfo errorInfo = new QueueErrorInfo();
            errorInfo.eventUuid = eventRecord.EventUuid;
            errorInfo.queueName = eventRecord.QueueName__c;
            errorInfo.component = eventRecord.Component__c;
            errorInfo.jobIdValue = eventRecord.JobId__c;
            errorInfo.asyncApexJobId = eventRecord.AsyncApexJobId__c;
            errorInfo.message = eventRecord.Message__c;
            errorInfo.stackTrace = eventRecord.StackTrace__c;
            errorInfo.occurredAt = eventRecord.OccurredAt__c;
            return errorInfo;
        }

        private QueueError__c toQueueError() {
            return new QueueError__c(
                QueueName__c = queueName,
                Component__c = component,
                Job__c = QueuesSupport.toId(jobIdValue),
                AsyncApexJobId__c = asyncApexJobId,
                Message__c = message,
                StackTrace__c = stackTrace,
                OccurredAt__c = occurredAt ?? QueuesSupport.now()
            );
        }
    }
}
