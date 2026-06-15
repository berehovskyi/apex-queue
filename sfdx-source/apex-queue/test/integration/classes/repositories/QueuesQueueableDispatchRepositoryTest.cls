@IsTest(IsParallel=false)
private class QueuesQueueableDispatchRepositoryTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_insert_and_update_dispatch_as_system() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesQueueableDispatchRepository repository = new QueuesQueueableDispatchRepository();
            QueueableDispatch__c dispatch = new QueueableDispatch__c(
                QueueName__c = 'queueable-dispatch-repository-system',
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                Mode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );

            // Act
            QueueableDispatch__c insertedDispatch = repository.insertDispatch(dispatch);
            insertedDispatch.State__c = Queues.QueueableDispatchState.ACTIVE.name();
            QueueableDispatch__c updatedDispatch = repository.updateDispatch(insertedDispatch);

            // Assert
            Assert.areNotEqual(null, insertedDispatch.Id);
            Assert.areEqual(insertedDispatch.Id, updatedDispatch.Id);
            Assert.areEqual(
                Queues.QueueableDispatchState.ACTIVE.name(),
                [
                    SELECT State__c
                    FROM QueueableDispatch__c
                    WHERE Id = :insertedDispatch.Id
                ]
                .State__c
            );
        }
    }

    @IsTest
    static void should_query_stale_dispatches_for_recovery() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queueable-dispatch-repository-stale';
            Datetime currentNow = System.now();
            QueuesQueueableDispatchRepository repository = new QueuesQueueableDispatchRepository();
            QueueableDispatch__c staleWaitingDispatch = queueableDispatch(
                queueName,
                Queues.QueueableDispatchState.WAITING,
                10
            );
            QueueableDispatch__c staleActiveDispatch = queueableDispatch(
                queueName,
                Queues.QueueableDispatchState.ACTIVE,
                11
            );
            staleActiveDispatch.StartedAt__c = currentNow.addMinutes(-10);
            QueueableDispatch__c freshWaitingDispatch = queueableDispatch(
                queueName,
                Queues.QueueableDispatchState.WAITING,
                12
            );
            QueueableDispatch__c staleUnsubmittedWaitingDispatch = queueableDispatch(
                queueName,
                Queues.QueueableDispatchState.WAITING,
                14
            );
            staleUnsubmittedWaitingDispatch.DispatcherAsyncApexJobId__c = null;
            QueueableDispatch__c freshUnsubmittedWaitingDispatch = queueableDispatch(
                queueName,
                Queues.QueueableDispatchState.WAITING,
                15
            );
            freshUnsubmittedWaitingDispatch.DispatcherAsyncApexJobId__c = null;
            QueueableDispatch__c terminalDispatch = queueableDispatch(
                queueName,
                Queues.QueueableDispatchState.COMPLETED,
                13
            );
            insert new List<QueueableDispatch__c>{
                staleWaitingDispatch,
                staleActiveDispatch,
                freshWaitingDispatch,
                staleUnsubmittedWaitingDispatch,
                freshUnsubmittedWaitingDispatch,
                terminalDispatch
            };
            Test.setCreatedDate(staleWaitingDispatch.Id, currentNow.addMinutes(-10));
            Test.setCreatedDate(freshWaitingDispatch.Id, currentNow.addMinutes(-1));
            Test.setCreatedDate(staleUnsubmittedWaitingDispatch.Id, currentNow.addMinutes(-10));
            Test.setCreatedDate(freshUnsubmittedWaitingDispatch.Id, currentNow.addMinutes(-1));
            Set<String> stateValues = QueuesSupport.toQueueableDispatchStateValues(
                new Set<Queues.QueueableDispatchState>{
                    Queues.QueueableDispatchState.WAITING,
                    Queues.QueueableDispatchState.ACTIVE
                }
            );

            // Act
            List<AggregateResult> maintenanceQueueNames = repository.queryStaleDispatchMaintenanceQueueNames(
                stateValues
            );
            List<AggregateResult> unsubmittedMaintenanceQueueNames = repository.queryUnsubmittedWaitingDispatchMaintenanceQueueNames();
            Boolean hasStaleDispatch = repository.hasStaleDispatch(queueName, stateValues, currentNow.addMinutes(-5));
            Boolean hasNoOldEnoughDispatch = repository.hasStaleDispatch(
                queueName,
                stateValues,
                currentNow.addMinutes(-20)
            );
            List<QueueableDispatch__c> staleDispatches = repository.queryStaleDispatchesForUpdate(
                queueName,
                stateValues,
                currentNow.addMinutes(-5),
                10
            );
            List<QueueableDispatch__c> limitedStaleDispatches = repository.queryStaleDispatchesForUpdate(
                queueName,
                stateValues,
                currentNow.addMinutes(-5),
                1
            );

            // Assert
            Assert.areEqual(1, maintenanceQueueNames.size());
            Assert.areEqual(queueName, (String) maintenanceQueueNames[0].get('queueName'));
            Assert.areEqual(1, unsubmittedMaintenanceQueueNames.size());
            Assert.areEqual(queueName, (String) unsubmittedMaintenanceQueueNames[0].get('queueName'));
            Assert.areEqual(true, hasStaleDispatch);
            Assert.areEqual(false, hasNoOldEnoughDispatch);
            Assert.areEqual(3, staleDispatches.size());
            Set<Id> staleDispatchIds = new Map<Id, QueueableDispatch__c>(staleDispatches).keySet();
            Assert.areEqual(true, staleDispatchIds.contains(staleWaitingDispatch.Id));
            Assert.areEqual(true, staleDispatchIds.contains(staleActiveDispatch.Id));
            Assert.areEqual(true, staleDispatchIds.contains(staleUnsubmittedWaitingDispatch.Id));
            Assert.areEqual(1, limitedStaleDispatches.size());
            Assert.areEqual(true, staleDispatchIds.contains(limitedStaleDispatches[0].Id));
        }
    }

    @IsTest
    static void should_ignore_empty_dispatch_inputs() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesQueueableDispatchRepository repository = new QueuesQueueableDispatchRepository();

            // Act
            List<AggregateResult> emptyMaintenanceQueueNames = repository.queryStaleDispatchMaintenanceQueueNames(
                new Set<String>()
            );
            List<AggregateResult> unsubmittedMaintenanceQueueNames = repository.queryUnsubmittedWaitingDispatchMaintenanceQueueNames();
            Boolean hasStaleDispatch = repository.hasStaleDispatch(
                null,
                new Set<String>{ Queues.QueueableDispatchState.WAITING.name() },
                System.now()
            );
            Boolean hasNoUnsubmittedActiveDispatch = repository.hasStaleDispatch(
                'queueable-dispatch-repository-empty',
                new Set<String>{ Queues.QueueableDispatchState.ACTIVE.name() },
                System.now()
            );
            List<QueueableDispatch__c> emptyStaleDispatches = repository.queryStaleDispatchesForUpdate(
                'queueable-dispatch-repository-empty',
                new Set<String>{ Queues.QueueableDispatchState.WAITING.name() },
                System.now(),
                0
            );
            List<QueueableDispatch__c> emptyActiveStaleDispatches = repository.queryStaleDispatchesForUpdate(
                'queueable-dispatch-repository-empty',
                new Set<String>{ Queues.QueueableDispatchState.ACTIVE.name() },
                System.now(),
                1
            );
            QueueableDispatch__c queriedDispatch = repository.queryDispatchByIdForUpdate(null);
            QueueableDispatch__c updatedDispatch = repository.updateDispatch(null);

            // Assert
            Assert.areEqual(0, emptyMaintenanceQueueNames.size());
            Assert.areEqual(0, unsubmittedMaintenanceQueueNames.size());
            Assert.areEqual(false, hasStaleDispatch);
            Assert.areEqual(false, hasNoUnsubmittedActiveDispatch);
            Assert.areEqual(0, emptyStaleDispatches.size());
            Assert.areEqual(0, emptyActiveStaleDispatches.size());
            Assert.areEqual(null, queriedDispatch);
            Assert.areEqual(null, updatedDispatch);
        }
    }

    private static QueueableDispatch__c queueableDispatch(
        final String queueName,
        final Queues.QueueableDispatchState state,
        final Integer seed
    ) {
        return new QueueableDispatch__c(
            QueueName__c = queueName,
            State__c = state.name(),
            Mode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
            RequestedCount__c = 1,
            DispatchedCount__c = state == Queues.QueueableDispatchState.ACTIVE ? 0 : 1,
            DispatchDepth__c = 0,
            DispatcherAsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, seed)
        );
    }
}
