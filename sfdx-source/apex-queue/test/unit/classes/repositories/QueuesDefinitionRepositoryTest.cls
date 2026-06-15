@IsTest(IsParallel=true)
private class QueuesDefinitionRepositoryTest {
    @IsTest
    static void should_return_no_rows_when_queue_definition_is_missing() {
        // Arrange
        QueuesDefinitionRepository repository = new QueuesDefinitionRepository();

        // Act
        List<QueueDefinition__mdt> definitions = repository.queryQueueDefinitions('missing-queue');
        List<QueueDefinition__mdt> definitionsByQueueNames = repository.queryQueueDefinitionsByQueueNames(
            new Set<String>{ 'missing-queue' }
        );
        List<QueueDefinition__mdt> emptyDefinitions = repository.queryQueueDefinitionsByQueueNames(new Set<String>());

        // Assert
        Assert.areEqual(0, definitions.size());
        Assert.areEqual(0, definitionsByQueueNames.size());
        Assert.areEqual(0, emptyDefinitions.size());
    }
}
