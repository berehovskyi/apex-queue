/**
 * @description Data access and Flex Queue commands for AsyncApexJob records.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesAsyncApexJobRepository {
    private final AccessLevel accessLevel;

    public QueuesAsyncApexJobRepository() {
        this(System.AccessLevel.USER_MODE);
    }

    public QueuesAsyncApexJobRepository(final AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    // <editor-fold desc="Queries">
    public List<AsyncApexJob> queryAsyncJobs(final String asyncJobId) {
        if (String.isBlank(asyncJobId)) {
            return new List<AsyncApexJob>();
        }

        return Database.queryWithBinds(
            'SELECT Id, Status FROM AsyncApexJob WHERE Id = :asyncJobId',
            new Map<String, Object>{ 'asyncJobId' => asyncJobId },
            accessLevel
        );
    }

    public List<AsyncApexJob> queryAsyncJobsByIds(final Set<Id> asyncJobIds) {
        if (asyncJobIds == null || asyncJobIds.isEmpty()) {
            return new List<AsyncApexJob>();
        }

        return Database.queryWithBinds(
            'SELECT Id, ApexClass.Name, ParentJobId, Status FROM AsyncApexJob WHERE Id IN :asyncJobIds',
            new Map<String, Object>{ 'asyncJobIds' => asyncJobIds },
            accessLevel
        );
    }

    public List<AsyncApexJob> queryActiveAsyncJobsByApexClass(
        final String apexClassName,
        final Set<String> activeStatuses
    ) {
        if (String.isBlank(apexClassName) || activeStatuses == null || activeStatuses.isEmpty()) {
            return new List<AsyncApexJob>();
        }

        return Database.queryWithBinds(
            'SELECT Id, ApexClass.Name, Status, CreatedDate ' +
                'FROM AsyncApexJob ' +
                'WHERE ApexClass.Name = :apexClassName ' +
                'AND Status IN :activeStatuses',
            new Map<String, Object>{ 'apexClassName' => apexClassName, 'activeStatuses' => activeStatuses },
            accessLevel
        );
    }
    // </editor-fold>

    // <editor-fold desc="Commands">
    public Boolean moveJobToFront(final Id asyncApexJobId) {
        QueuesSupport.notNull(asyncApexJobId, 'asyncApexJobId');
        return System.FlexQueue.moveJobToFront(asyncApexJobId);
    }

    public Boolean moveJobToEnd(final Id asyncApexJobId) {
        QueuesSupport.notNull(asyncApexJobId, 'asyncApexJobId');
        return System.FlexQueue.moveJobToEnd(asyncApexJobId);
    }
    // </editor-fold>
}
