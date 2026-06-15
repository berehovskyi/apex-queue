@IsTest(IsParallel=true)
private class QueuesStatsServiceTest {
    @IsTest
    static void should_aggregate_runtime_and_job_counts_when_building_stats() {
        // Arrange
        String queueName = 'stats-unit';
        Map<String, Integer> stateCountsByValue = new Map<String, Integer>{
            Queues.State.WAITING.name() => 1,
            Queues.State.FAILED.name() => 1
        };

        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, true))
        );
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('findVisibleRuntime', QueuesTestSupport.runtime(queueName, true, 2))
        );
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('queryQueueStateCounts', stateCountsByValue)
        );

        QueuesStatsService service = new QueuesStatsService(definitionStub, runtimeStub, jobServiceStub);

        // Act
        Queues.QueueStats stats = service.getStats(queueName);

        // Assert
        Assert.areEqual(queueName, stats.queueName);
        Assert.areEqual(true, stats.isPaused);
        Assert.areEqual(2, stats.activeWorkerCount);
        Assert.areEqual(2, stats.total);
        Assert.areEqual(1, stats.waiting);
        Assert.areEqual(1, stats.failed);
    }

    @IsTest
    static void should_map_aggregate_state_counts_from_exact_aggregate_results() {
        // Arrange
        Map<String, Integer> stateCountsByValue = new Map<String, Integer>{
            Queues.State.WAITING.name() => 2,
            Queues.State.ACTIVE.name() => 1
        };

        // Act
        Queues.QueueStats stats = QueuesStatsService.mapToStats(
            'stats-truncated',
            QueuesTestSupport.runtime('stats-truncated', false, 0),
            stateCountsByValue
        );

        // Assert
        Assert.areEqual(2, stats.waiting);
        Assert.areEqual(1, stats.active);
        Assert.areEqual(3, stats.total);
    }
}
