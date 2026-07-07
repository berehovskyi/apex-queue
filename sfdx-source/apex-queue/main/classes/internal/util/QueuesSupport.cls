/**
 * @description Shared internal utilities: coercion, validation, and serialization.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesSupport {
    @TestVisible
    private static final String DEFAULT_EXCEPTION_REASON = 'Unknown error.';
    private static final String NULL_ARGUMENT_MESSAGE = '{0} cannot be null';
    private static final String BLANK_ARGUMENT_MESSAGE = '{0} cannot be blank';
    private static final String INVALID_QUEUE_SCOPED_EXTERNAL_ID_LENGTH_MESSAGE = 'queueScopedExternalId must be 255 characters or fewer';
    private static final Set<String> GENERIC_EXCEPTION_REASONS = new Set<String>{ 'Script-thrown exception' };
    @TestVisible
    private static Decimal forcedRandom;
    @TestVisible
    private static Integer forcedRemainingQueueableSubmissionSlots;

    public static void isTrue(final Boolean condition, final Exception ex) {
        if (condition ?? false) {
            return;
        }
        throw ex;
    }

    public static void isTrue(final Boolean condition, final String message) {
        isTrue(condition, new Queues.QueueException(message));
    }

    public static void notNull(final Object value, final String argName) {
        isTrue(
            value != null,
            new Queues.QueueException(String.format(NULL_ARGUMENT_MESSAGE, new List<String>{ argName }))
        );
    }

    public static void notBlank(final String value, final String argName) {
        isTrue(
            value != null,
            new Queues.QueueException(String.format(NULL_ARGUMENT_MESSAGE, new List<String>{ argName }))
        );
        isTrue(
            String.isNotBlank(value),
            new Queues.QueueException(String.format(BLANK_ARGUMENT_MESSAGE, new List<String>{ argName }))
        );
    }

    public static Integer toInteger(final Object value, final Integer fallbackValue) {
        if (value == null) {
            return fallbackValue;
        }
        if (value instanceof Decimal) {
            return ((Decimal) value).intValue();
        }
        return Integer.valueOf(String.valueOf(value));
    }

    public static Decimal toDecimal(final Object value, final Decimal fallbackValue) {
        if (value == null) {
            return fallbackValue;
        }
        if (value instanceof Decimal) {
            return (Decimal) value;
        }
        return Decimal.valueOf(String.valueOf(value));
    }

    public static Iterable<Object> noNullElements(final Iterable<Object> it, String argName) {
        for (Object o : it) {
            notNull(o, argName);
        }
        return it;
    }

    public static Integer toPositiveInteger(final Object value, final Integer fallbackValue) {
        Integer resolvedValue = toInteger(value, fallbackValue);
        return resolvedValue > 0 ? resolvedValue : fallbackValue;
    }

    public static String truncate(final String value, final Integer maxLength) {
        if (value == null || maxLength == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, Math.max(0, maxLength));
    }

    public static Id toId(final String value) {
        if (String.isBlank(value)) {
            return null;
        }

        try {
            return Id.valueOf(value);
        } catch (Exception ex) {
            return null;
        }
    }

    public static Set<Id> toIdSet(final List<SObject> records) {
        return toIdSet(records, 'Id');
    }

    public static List<Id> toIds(final List<SObject> records) {
        return toIds(records, 'Id');
    }

    public static List<Id> toIds(final List<SObject> records, String field) {
        List<Id> result = new List<Id>();
        for (SObject record : records) {
            Id value = (Id) record?.get(field);
            if (String.isNotBlank(value)) {
                result.add(value);
            }
        }
        return result;
    }

    public static Set<Id> toIdSet(final List<SObject> records, String field) {
        Set<Id> result = new Set<Id>();
        for (SObject record : records) {
            Id value = (Id) record?.get(field);
            if (String.isNotBlank(value)) {
                result.add(value);
            }
        }
        return result;
    }

    public static Set<String> toStringSet(final List<SObject> records, String field) {
        Set<String> result = new Set<String>();
        for (SObject record : records) {
            String value = (String) record?.get(field);
            if (String.isNotBlank(value)) {
                result.add(value);
            }
        }
        return result;
    }

    public static Queues.State toState(final String stateValue) {
        try {
            return Queues.State.valueOf(stateValue);
        } catch (Exception ex) {
            return null;
        }
    }

    public static Set<String> toStateValues(final Set<Queues.State> states) {
        return toValues(states, new QueueStateValueResolver());
    }

    public static Set<String> toQueueableDispatchStateValues(final Set<Queues.QueueableDispatchState> states) {
        return toValues(states, new QueueableDispatchStateValueResolver());
    }

    public static Queues.Backoff toBackoff(final String backoffValue) {
        try {
            return Queues.Backoff.valueOf(backoffValue);
        } catch (Exception ex) {
            return null;
        }
    }

    public static Queues.ExecutionMode toExecutionMode(final String executionModeValue) {
        try {
            return Queues.ExecutionMode.valueOf(executionModeValue);
        } catch (Exception ex) {
            return null;
        }
    }

    public static Boolean isQueueableExecutionMode(final Queues.ExecutionMode executionMode) {
        return executionMode == Queues.ExecutionMode.QUEUEABLE_SERIAL ||
            executionMode == Queues.ExecutionMode.QUEUEABLE_CONCURRENT;
    }

    public static Boolean isWorkerExecutionMode(final Queues.ExecutionMode executionMode) {
        return executionMode == Queues.ExecutionMode.WORKER;
    }

    public static Boolean isInvocableExecutionMode(final Queues.ExecutionMode executionMode) {
        return executionMode == Queues.ExecutionMode.INVOCABLE;
    }

    public static Queues.QueueableDispatchState toQueueableDispatchState(final String queueableDispatchStateValue) {
        try {
            return Queues.QueueableDispatchState.valueOf(queueableDispatchStateValue);
        } catch (Exception ex) {
            return null;
        }
    }

    public static SObject clone(final SObject record) {
        return record?.clone(true, false, false, false);
    }

    public static String serialize(final Object payload) {
        return payload == null ? null : JSON.serialize(payload);
    }

    public static Object deserialize(final String value) {
        return String.isBlank(value) ? null : JSON.deserializeUntyped(value);
    }

    public static String normalizeEnumValue(final String value) {
        return value?.trim()?.replace('-', '_')?.replace(' ', '_')?.toUpperCase();
    }

    public static String normalizeCaseInsensitiveKey(final String value) {
        return value?.toLowerCase();
    }

    public static String normalizeExceptionReason(final String exceptionReason) {
        String normalizedExceptionReason = exceptionReason?.trim();
        return String.isNotBlank(normalizedExceptionReason) ? normalizedExceptionReason : DEFAULT_EXCEPTION_REASON;
    }

    public static String resolveExceptionReason(final Exception ex) {
        String exceptionReason = ex?.getMessage()?.trim();
        if (String.isNotBlank(exceptionReason) && !GENERIC_EXCEPTION_REASONS.contains(exceptionReason)) {
            return exceptionReason;
        }

        String exceptionType = ex?.getTypeName()?.trim();
        return normalizeExceptionReason(exceptionType);
    }

    @SuppressWarnings('PMD.AvoidDebugStatements')
    public static void debugError(final String message) {
        System.debug(LoggingLevel.ERROR, message);
    }

    public static String buildQueueScopedExternalId(final String queueName, final String externalId) {
        if (String.isBlank(queueName) || String.isBlank(externalId)) {
            return null;
        }

        String queueScopedExternalId = String.format('{0}:{1}', new List<String>{ queueName, externalId });
        QueuesSupport.isTrue(queueScopedExternalId.length() <= 255, INVALID_QUEUE_SCOPED_EXTERNAL_ID_LENGTH_MESSAGE);
        return queueScopedExternalId;
    }

    public static String sha256Hex(final String value) {
        if (value == null) {
            return null;
        }

        return EncodingUtil.convertToHex(Crypto.generateDigest('SHA-256', Blob.valueOf(value)));
    }

    public static String generateRandomToken() {
        return sha256Hex('' + Crypto.getRandomLong());
    }

    public static Boolean isActive(final SObject record) {
        return record?.get('IsActive__c') == true;
    }

    public static Boolean isPaused(final SObject record) {
        return record?.get('IsPaused__c') == true;
    }

    public static Datetime now() {
        return System.now();
    }

    public static Decimal random() {
        return forcedRandom ?? Decimal.valueOf(String.valueOf(Math.random()));
    }

    public static Integer remainingQueueableSubmissionSlots() {
        return forcedRemainingQueueableSubmissionSlots ??
            Math.max(Limits.getLimitQueueableJobs() - Limits.getQueueableJobs(), 0);
    }

    public static Boolean hasQueueableSubmissionSlot() {
        return remainingQueueableSubmissionSlots() > 0;
    }

    public static Datetime normalizeWakeAt(final Datetime desiredWakeAt) {
        if (desiredWakeAt == null) {
            return null;
        }

        Datetime currentNow = now();
        return desiredWakeAt <= currentNow ? currentNow : desiredWakeAt;
    }

    private static Set<String> toValues(final Iterable<Object> values, final ValueResolver resolver) {
        Set<String> resolvedValues = new Set<String>();
        for (Object value : values ?? new List<Object>()) {
            String resolvedValue = resolver.resolve(value);
            if (String.isNotBlank(resolvedValue)) {
                resolvedValues.add(resolvedValue);
            }
        }
        return resolvedValues;
    }

    private interface ValueResolver {
        String resolve(Object value);
    }

    private inherited sharing class QueueStateValueResolver implements ValueResolver {
        public String resolve(final Object value) {
            return ((Queues.State) value)?.name();
        }
    }

    private inherited sharing class QueueableDispatchStateValueResolver implements ValueResolver {
        public String resolve(final Object value) {
            return ((Queues.QueueableDispatchState) value)?.name();
        }
    }

    public static GovernorHeadroom currentGovernorHeadroom() {
        return new GovernorHeadroom();
    }

    public class GovernorHeadroom {
        public final Integer dmlRows;
        public final Integer dmlStatements;
        public final Integer queries;
        public final Integer cpuMs;

        private GovernorHeadroom() {
            this.dmlRows = Limits.getLimitDmlRows() - Limits.getDmlRows();
            this.dmlStatements = Limits.getLimitDmlStatements() - Limits.getDmlStatements();
            this.queries = Limits.getLimitQueries() - Limits.getQueries();
            this.cpuMs = Limits.getLimitCpuTime() - Limits.getCpuTime();
        }
    }
}
