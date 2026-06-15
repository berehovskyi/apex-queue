/**
 * @description Records queue errors through events and persistence.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesErrorService {
    private static final Set<String> emittedErrorFingerprints = new Set<String>();

    private QueuesErrorRepository userModeRepository = new QueuesErrorRepository();
    private QueuesErrorRepository systemModeRepository = new QueuesErrorRepository(System.AccessLevel.SYSTEM_MODE);
    private QueuesErrorEventRepository eventRepository = new QueuesErrorEventRepository(System.AccessLevel.SYSTEM_MODE);

    public QueuesErrorService() {
    }

    @TestVisible
    private QueuesErrorService(
        final QueuesErrorRepository repository,
        final QueuesErrorEventRepository eventRepository
    ) {
        this.userModeRepository = repository;
        this.systemModeRepository = repository;
        this.eventRepository = eventRepository;
    }

    // <editor-fold desc="Errors">
    public QueueError__c insertQueueErrorAsSystem(final QueueError__c error) {
        return systemModeRepository.insertQueueError(error);
    }

    public List<QueueError__c> insertQueueErrors(final List<QueueError__c> errors) {
        return userModeRepository.insertQueueErrors(errors);
    }

    public List<QueueError__c> insertQueueErrorsAsSystem(final List<QueueError__c> errors) {
        return systemModeRepository.insertQueueErrors(errors);
    }

    public void recordQueueError(final String queueName, final String component, final Exception ex) {
        recordQueueError(queueName, component, null, null, ex);
    }

    public void recordQueueError(
        final String queueName,
        final String component,
        final Id jobId,
        final String asyncApexJobId,
        final Exception exc
    ) {
        QueuesSupport.notBlank(queueName, 'queueName');
        QueuesSupport.notBlank(component, 'component');
        if (exc == null) {
            return;
        }

        String fingerprint = buildErrorFingerprint(queueName, exc);
        if (emittedErrorFingerprints.contains(fingerprint)) {
            return;
        }

        QueueErrorEvent__e errorEvent = mapToErrorEvent(queueName, component, jobId, asyncApexJobId, exc);
        eventRepository.publishQueueError(errorEvent);
        emittedErrorFingerprints.add(fingerprint);
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    @TestVisible
    private static void resetEmittedErrorFingerprints() {
        emittedErrorFingerprints.clear();
    }

    private static String buildErrorFingerprint(final String queueName, final Exception exc) {
        return String.join(
            new List<String>{ queueName, QueuesSupport.resolveExceptionReason(exc), exc.getStackTraceString() },
            '||'
        );
    }

    @TestVisible
    private static QueueErrorEvent__e mapToErrorEvent(
        final String queueName,
        final String component,
        final Id jobId,
        final String asyncApexJobId,
        final Exception exc
    ) {
        // Preallocate EventUuid so the publish-failure callback can correlate this event if EventBus.publish throws or later fails asynchronously.
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) QueueErrorEvent__e.SObjectType.newSObject(null, true);
        errorEvent.QueueName__c = queueName;
        errorEvent.Component__c = component;
        errorEvent.JobId__c = jobId;
        errorEvent.AsyncApexJobId__c = asyncApexJobId;
        errorEvent.Message__c = QueuesSupport.resolveExceptionReason(exc);
        errorEvent.StackTrace__c = exc.getStackTraceString();
        errorEvent.OccurredAt__c = QueuesSupport.now();
        return errorEvent;
    }
    // </editor-fold>
}
