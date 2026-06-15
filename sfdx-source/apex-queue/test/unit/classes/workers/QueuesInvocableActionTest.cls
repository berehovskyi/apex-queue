@IsTest(IsParallel=true)
private class QueuesInvocableActionTest {
    @IsTest
    static void should_tolerate_stale_claim_and_execute_inputs() {
        // Arrange
        Integer dmlStatementsBefore = Limits.getDmlStatements();

        // Act
        QueuesInvocableAction.run(new List<QueuesInvocableAction.FlowInput>{ input('CLAIM', 801) });
        QueuesInvocableAction.run(new List<QueuesInvocableAction.FlowInput>{ input('EXECUTE', 802) });

        // Assert
        Assert.areEqual(dmlStatementsBefore, Limits.getDmlStatements());
    }

    @IsTest
    static void should_bulk_claim_full_flow_batch_without_per_job_queries() {
        // Arrange
        Integer dmlStatementsBefore = Limits.getDmlStatements();
        Integer queriesBefore = Limits.getQueries();
        List<QueuesInvocableAction.FlowInput> inputs = new List<QueuesInvocableAction.FlowInput>();
        for (Integer sequence = 1; sequence <= 100; sequence++) {
            inputs.add(input('CLAIM', sequence));
        }

        // Act
        QueuesInvocableAction.run(inputs);

        // Assert
        Assert.areEqual(dmlStatementsBefore, Limits.getDmlStatements());
        Assert.areEqual(queriesBefore + 1, Limits.getQueries());
    }

    @IsTest
    static void should_reject_multiple_execute_flow_inputs() {
        // Act + Assert
        try {
            QueuesInvocableAction.run(
                new List<QueuesInvocableAction.FlowInput>{ input('EXECUTE', 805), input('EXECUTE', 806) }
            );
            Assert.fail('Expected multi-execute input to throw.');
        } catch (Queues.ConfigurationException ex) {
            Assert.areEqual('Invocable execute phase expects exactly one input.', ex.getMessage());
        }
    }

    private static QueuesInvocableAction.FlowInput input(final String phase, final Integer sequence) {
        QueuesInvocableAction.FlowInput input = new QueuesInvocableAction.FlowInput();
        input.jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, sequence);
        input.phase = phase;
        return input;
    }
}
