/**
 * @description Publishes queue lifecycle platform events.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public virtual inherited sharing class QueuesQueueEventRepository {
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

        publishEvents(events);
    }

    @TestVisible
    protected virtual void publishEvents(final List<SObject> events) {
        EventBus.publishWithAccessLevel(events, accessLevel);
    }
}
