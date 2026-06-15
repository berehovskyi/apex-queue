@IsTest(IsParallel=true)
private class QueuesUtilsTest {
    @IsTest
    static void should_validate_required_values_when_guards_are_used() {
        // Arrange
        String isTrueMessage;
        String notNullMessage;
        String notBlankMessage;
        String noNullElementsMessage;
        List<Object> safeValues = new List<Object>{ 'value', 2 };

        // Act
        QueuesSupport.isTrue(true, 'should not throw');
        Iterable<Object> validatedValues = QueuesSupport.noNullElements(safeValues, 'item');

        try {
            QueuesSupport.isTrue(false, 'boom');
            Assert.fail('Expected QueueException.');
        } catch (Queues.QueueException ex) {
            isTrueMessage = ex.getMessage();
        }

        try {
            QueuesSupport.notNull(null, 'payload');
            Assert.fail('Expected notNull to throw.');
        } catch (Queues.QueueException ex) {
            notNullMessage = ex.getMessage();
        }

        try {
            QueuesSupport.notBlank('', 'queueName');
            Assert.fail('Expected notBlank to throw.');
        } catch (Queues.QueueException ex) {
            notBlankMessage = ex.getMessage();
        }

        try {
            QueuesSupport.noNullElements(new List<Object>{ 'value', null }, 'item');
            Assert.fail('Expected noNullElements to throw.');
        } catch (Queues.QueueException ex) {
            noNullElementsMessage = ex.getMessage();
        }

        // Assert
        Assert.areNotEqual(null, validatedValues);
        Assert.areEqual('boom', isTrueMessage);
        Assert.areEqual('payload cannot be null', notNullMessage);
        Assert.areEqual('queueName cannot be blank', notBlankMessage);
        Assert.areEqual('item cannot be null', noNullElementsMessage);
    }

    @IsTest
    static void should_convert_numbers_enums_and_flags_when_values_are_provided() {
        // Arrange
        Id firstJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 21);
        Id secondJobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 22);
        List<SObject> jobs = new List<SObject>{
            new Job__c(Id = firstJobId),
            new Job__c(),
            new Job__c(Id = secondJobId)
        };

        // Act
        Integer decimalValue = QueuesSupport.toInteger(Decimal.valueOf('7'), 0);
        Integer stringValue = QueuesSupport.toInteger('9', 0);
        Integer defaultValue = QueuesSupport.toInteger(null, 5);
        List<Id> orderedJobIds = QueuesSupport.toIds(jobs);
        Set<Id> jobIdSet = QueuesSupport.toIdSet(jobs);
        Integer positiveInteger = QueuesSupport.toPositiveInteger(-1, 4);
        Queues.State state = QueuesSupport.toState(Queues.State.WAITING_CHILDREN.name());
        Queues.State unknownState = QueuesSupport.toState('unknown');
        Set<String> stateValues = QueuesSupport.toStateValues(
            new Set<Queues.State>{ Queues.State.WAITING, Queues.State.WAITING_CHILDREN }
        );
        Queues.Backoff backoff = QueuesSupport.toBackoff(Queues.Backoff.EXPONENTIAL.name());
        Queues.Backoff unknownBackoff = QueuesSupport.toBackoff('???');
        Queues.ExecutionMode executionMode = QueuesSupport.toExecutionMode(
            Queues.ExecutionMode.QUEUEABLE_SERIAL.name()
        );
        Queues.ExecutionMode invocableExecutionMode = QueuesSupport.toExecutionMode(
            Queues.ExecutionMode.INVOCABLE.name()
        );
        Queues.ExecutionMode unknownExecutionMode = QueuesSupport.toExecutionMode('???');
        Boolean queueableExecutionMode = QueuesSupport.isQueueableExecutionMode(
            Queues.ExecutionMode.QUEUEABLE_CONCURRENT
        );
        Boolean workerIsQueueableExecutionMode = QueuesSupport.isQueueableExecutionMode(Queues.ExecutionMode.WORKER);
        Boolean invocableIsQueueableExecutionMode = QueuesSupport.isQueueableExecutionMode(
            Queues.ExecutionMode.INVOCABLE
        );
        Boolean workerExecutionMode = QueuesSupport.isWorkerExecutionMode(Queues.ExecutionMode.WORKER);
        Boolean invocableIsWorkerExecutionMode = QueuesSupport.isWorkerExecutionMode(Queues.ExecutionMode.INVOCABLE);
        Boolean invocableExecutionModeFlag = QueuesSupport.isInvocableExecutionMode(Queues.ExecutionMode.INVOCABLE);
        Boolean workerIsInvocableExecutionMode = QueuesSupport.isInvocableExecutionMode(Queues.ExecutionMode.WORKER);
        Queues.QueueableDispatchState queueableDispatchState = QueuesSupport.toQueueableDispatchState(
            Queues.QueueableDispatchState.WAITING.name()
        );
        Queues.QueueableDispatchState unknownQueueableDispatchState = QueuesSupport.toQueueableDispatchState('???');
        Set<String> queueableDispatchStateValues = QueuesSupport.toQueueableDispatchStateValues(
            new Set<Queues.QueueableDispatchState>{
                Queues.QueueableDispatchState.ACTIVE,
                Queues.QueueableDispatchState.WAITING
            }
        );
        String normalizedEnumValue = QueuesSupport.normalizeEnumValue(' waiting children ');
        String queueScopedExternalId = QueuesSupport.buildQueueScopedExternalId('invoice-sync', 'invoice:INV-001');
        String sameQueueScopedExternalId = QueuesSupport.buildQueueScopedExternalId('invoice-sync', 'invoice:INV-001');
        String differentQueueScopedExternalId = QueuesSupport.buildQueueScopedExternalId(
            'billing-sync',
            'invoice:INV-001'
        );
        Boolean activeDefinition = QueuesSupport.isActive(new QueueDefinition__mdt(IsActive__c = true));
        Boolean pausedRuntime = QueuesSupport.isPaused(new QueueRuntime__c(IsPaused__c = true));

        // Assert
        Assert.areEqual(7, decimalValue);
        Assert.areEqual(9, stringValue);
        Assert.areEqual(5, defaultValue);
        Assert.areEqual(new List<Id>{ firstJobId, secondJobId }, orderedJobIds);
        Assert.areEqual(new Set<Id>{ firstJobId, secondJobId }, jobIdSet);
        Assert.areEqual(4, positiveInteger);
        Assert.areEqual(Queues.State.WAITING_CHILDREN, state);
        Assert.areEqual(null, unknownState);
        Assert.areEqual(new Set<String>{ 'WAITING', 'WAITING_CHILDREN' }, stateValues);
        Assert.areEqual(Queues.Backoff.EXPONENTIAL, backoff);
        Assert.areEqual(null, unknownBackoff);
        Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL, executionMode);
        Assert.areEqual(Queues.ExecutionMode.INVOCABLE, invocableExecutionMode);
        Assert.areEqual(null, unknownExecutionMode);
        Assert.areEqual(true, queueableExecutionMode);
        Assert.areEqual(false, workerIsQueueableExecutionMode);
        Assert.areEqual(false, invocableIsQueueableExecutionMode);
        Assert.areEqual(true, workerExecutionMode);
        Assert.areEqual(false, invocableIsWorkerExecutionMode);
        Assert.areEqual(true, invocableExecutionModeFlag);
        Assert.areEqual(false, workerIsInvocableExecutionMode);
        Assert.areEqual(Queues.QueueableDispatchState.WAITING, queueableDispatchState);
        Assert.areEqual(null, unknownQueueableDispatchState);
        Assert.areEqual(new Set<String>{ 'ACTIVE', 'WAITING' }, queueableDispatchStateValues);
        Assert.areEqual('WAITING_CHILDREN', normalizedEnumValue);
        Assert.areEqual('invoice-sync:invoice:INV-001', queueScopedExternalId);
        Assert.areEqual(queueScopedExternalId, sameQueueScopedExternalId);
        Assert.areEqual(true, String.isNotBlank(queueScopedExternalId));
        Assert.areEqual(true, queueScopedExternalId != differentQueueScopedExternalId);
        Assert.areEqual(true, activeDefinition);
        Assert.areEqual(true, pausedRuntime);
    }

    @IsTest
    static void should_resolve_failure_reasons_when_normalizing_errors() {
        // Arrange
        Queues.QueueException explicitMessage = new Queues.QueueException('Processor exploded.');
        Queues.QueueException genericMessage = new Queues.QueueException('Script-thrown exception');
        Queues.QueueException blankMessage = new Queues.QueueException('   ');

        // Act
        String normalizedFailureReason = QueuesSupport.normalizeExceptionReason('  exploded  ');
        String defaultFailureReason = QueuesSupport.normalizeExceptionReason('  ');
        String explicitFailureReason = QueuesSupport.resolveExceptionReason(explicitMessage);
        String genericFailureReason = QueuesSupport.resolveExceptionReason(genericMessage);
        String blankFailureReason = QueuesSupport.resolveExceptionReason(blankMessage);
        String nullFailureReason = QueuesSupport.resolveExceptionReason(null);
        QueuesSupport.debugError('Processor exploded.');

        // Assert
        Assert.areEqual('exploded', normalizedFailureReason);
        Assert.areEqual(QueuesSupport.DEFAULT_EXCEPTION_REASON, defaultFailureReason);
        Assert.areEqual('Processor exploded.', explicitFailureReason);
        Assert.areEqual('Queues.QueueException', genericFailureReason);
        Assert.areEqual('Queues.QueueException', blankFailureReason);
        Assert.areEqual(QueuesSupport.DEFAULT_EXCEPTION_REASON, nullFailureReason);
    }

    @IsTest
    static void should_serialize_clone_and_normalize_times_when_using_utilities() {
        // Arrange
        Job__c originalJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        originalJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 101);
        originalJob.Data__c = '{"invoiceId":"INV-101"}';

        // Act
        Job__c clonedJob = (Job__c) QueuesSupport.clone(originalJob);
        Map<String, Object> payload = (Map<String, Object>) QueuesSupport.deserialize(
            QueuesSupport.serialize(new Map<String, Object>{ 'invoiceId' => 'INV-102' })
        );
        Datetime before = QueuesSupport.now();
        Datetime normalizedPast = QueuesSupport.normalizeWakeAt(before.addMinutes(-1));
        Datetime after = QueuesSupport.now();
        Datetime futureWakeAt = before.addMinutes(3);

        // Assert
        Assert.areEqual(originalJob.Id, clonedJob.Id);
        Assert.areEqual('{"invoiceId":"INV-102"}', QueuesSupport.serialize(payload));
        Assert.areEqual(true, String.isNotBlank(QueuesSupport.generateRandomToken()));
        Assert.areEqual(true, normalizedPast >= before && normalizedPast <= after);
        Assert.areEqual(futureWakeAt, QueuesSupport.normalizeWakeAt(futureWakeAt));
        Assert.areEqual(null, QueuesSupport.normalizeWakeAt(null));
    }

    @IsTest
    static void should_handle_null_enum_sets_hash_inputs_and_queue_scoped_ids_when_values_are_missing() {
        // Arrange

        // Act
        Id invalidId = QueuesSupport.toId('not-an-id');
        Queues.State nullState = QueuesSupport.toState(null);
        Set<String> stateValues = QueuesSupport.toStateValues(new Set<Queues.State>{ null, Queues.State.WAITING });
        Set<String> emptyStateValues = QueuesSupport.toStateValues(null);
        Set<String> emptyQueueableDispatchStateValues = QueuesSupport.toQueueableDispatchStateValues(null);
        Queues.Backoff nullBackoff = QueuesSupport.toBackoff(null);
        String nullQueueScopedExternalId = QueuesSupport.buildQueueScopedExternalId(null, 'job-a');
        String nullHash = QueuesSupport.sha256Hex(null);
        Object blankDeserializedValue = QueuesSupport.deserialize('');

        // Assert
        Assert.areEqual(null, invalidId);
        Assert.areEqual(null, nullState);
        Assert.areEqual(new Set<String>{ 'WAITING' }, stateValues);
        Assert.areEqual(0, emptyStateValues.size());
        Assert.areEqual(0, emptyQueueableDispatchStateValues.size());
        Assert.areEqual(null, nullBackoff);
        Assert.areEqual(null, nullQueueScopedExternalId);
        Assert.areEqual(null, nullHash);
        Assert.areEqual(null, blankDeserializedValue);
    }
}
