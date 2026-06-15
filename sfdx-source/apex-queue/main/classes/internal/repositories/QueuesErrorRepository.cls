/**
 * @description Data access for QueueError records.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesErrorRepository {
    private final AccessLevel accessLevel;

    public QueuesErrorRepository() {
        this(System.AccessLevel.USER_MODE);
    }

    public QueuesErrorRepository(final AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    // <editor-fold desc="DMLs">
    @SuppressWarnings('PMD.ApexCRUDViolation')
    public QueueError__c insertQueueError(final QueueError__c error) {
        if (error == null) {
            return null;
        }

        Database.insert(error, accessLevel);
        return error;
    }

    @SuppressWarnings('PMD.ApexCRUDViolation')
    public List<QueueError__c> insertQueueErrors(final List<QueueError__c> errors) {
        if (errors == null || errors.isEmpty()) {
            return new List<QueueError__c>();
        }

        Database.insert(errors, accessLevel);
        return errors;
    }
    // </editor-fold>
}
