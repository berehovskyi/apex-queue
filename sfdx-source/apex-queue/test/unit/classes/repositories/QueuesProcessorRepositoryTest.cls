@IsTest(IsParallel=true)
private class QueuesProcessorRepositoryTest {
    @IsTest
    static void should_return_no_rows_when_processor_definition_is_missing() {
        // Arrange
        QueuesProcessorRepository repository = new QueuesProcessorRepository();

        // Act
        List<Processor__mdt> emptyProcessorsByClass = repository.queryProcessorsByClassNames(new Set<String>());
        List<Processor__mdt> processorsByClass = repository.queryProcessorsByClassNames(
            new Set<String>{ 'MissingProcessorClass' }
        );

        // Assert
        Assert.areEqual(0, emptyProcessorsByClass.size());
        Assert.areEqual(0, processorsByClass.size());
    }
}
