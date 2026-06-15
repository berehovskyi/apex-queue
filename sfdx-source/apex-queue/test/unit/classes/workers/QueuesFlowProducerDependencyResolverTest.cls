@IsTest(IsParallel=true)
private class QueuesFlowProducerDependencyResolverTest {
    @IsTest
    static void should_enqueue_resolver_directly_outside_queueable_stack() {
        // Act
        Test.startTest();
        Boolean enqueued = QueuesFlowProducerDependencyResolver.tryEnqueue(
            new Set<Id>(),
            new Set<Id>(),
            new Set<String>()
        );
        Test.stopTest();

        // Assert
        Assert.areEqual(true, enqueued);
        Assert.areEqual(1, queryResolverCount());
    }

    @IsTest
    static void should_bridge_resolver_submission_near_queueable_stack_limit() {
        // Act
        Test.startTest();
        QueuesQueueableStackSupport.forceQueueableStackContext = true;
        QueuesQueueableStackSupport.forceCurrentQueueableStackDepth = 4;
        QueuesQueueableStackSupport.forceMaximumQueueableStackDepth = 5;
        Boolean enqueued;
        try {
            enqueued = QueuesFlowProducerDependencyResolver.tryEnqueue(new Set<Id>(), new Set<Id>(), new Set<String>());
        } finally {
            QueuesQueueableStackSupport.forceQueueableStackContext = null;
            QueuesQueueableStackSupport.forceCurrentQueueableStackDepth = null;
            QueuesQueueableStackSupport.forceMaximumQueueableStackDepth = null;
        }
        Test.stopTest();

        // Assert
        Assert.areEqual(true, enqueued);
        Assert.areEqual(1, queryResolverCount());
    }

    @IsTest
    static void should_report_submission_slot_availability() {
        // Act + Assert
        Assert.areEqual(true, QueuesFlowProducerDependencyResolver.hasSubmissionSlot(0, 1));
        Assert.areEqual(false, QueuesFlowProducerDependencyResolver.hasSubmissionSlot(1, 1));
        QueuesSupport.forcedRemainingQueueableSubmissionSlots = 0;
        try {
            Assert.areEqual(
                false,
                QueuesFlowProducerDependencyResolver.tryEnqueue(new Set<Id>(), new Set<Id>(), new Set<String>())
            );
        } finally {
            QueuesSupport.forcedRemainingQueueableSubmissionSlots = null;
        }
    }

    @IsTest
    static void should_reject_future_bridge_when_submission_slot_is_unavailable() {
        // Arrange
        QueuesQueueableStackSupport.forceQueueableStackContext = true;
        QueuesQueueableStackSupport.forceCurrentQueueableStackDepth = 4;
        QueuesQueueableStackSupport.forceMaximumQueueableStackDepth = 5;
        QueuesFlowProducerDependencyResolver.forceSubmissionSlotAvailable = false;

        // Act
        Boolean enqueued;
        try {
            enqueued = QueuesFlowProducerDependencyResolver.tryEnqueue(null, null, null);
        } finally {
            QueuesQueueableStackSupport.forceQueueableStackContext = null;
            QueuesQueueableStackSupport.forceCurrentQueueableStackDepth = null;
            QueuesQueueableStackSupport.forceMaximumQueueableStackDepth = null;
            QueuesFlowProducerDependencyResolver.forceSubmissionSlotAvailable = null;
        }

        // Assert
        Assert.areEqual(false, enqueued);
    }

    @IsTest
    static void should_reject_future_bridge_when_native_submission_fails() {
        // Arrange
        QueuesQueueableStackSupport.forceQueueableStackContext = true;
        QueuesQueueableStackSupport.forceCurrentQueueableStackDepth = 4;
        QueuesQueueableStackSupport.forceMaximumQueueableStackDepth = 5;
        QueuesFlowProducerDependencyResolver.forceSubmissionSlotAvailable = true;
        QueuesFlowProducerDependencyResolver.forceSubmissionException = new System.AsyncException(
            'native future submission rejected'
        );

        // Act
        Boolean enqueued;
        try {
            enqueued = QueuesFlowProducerDependencyResolver.tryEnqueue(new Set<Id>(), new Set<Id>(), new Set<String>());
        } finally {
            QueuesQueueableStackSupport.forceQueueableStackContext = null;
            QueuesQueueableStackSupport.forceCurrentQueueableStackDepth = null;
            QueuesQueueableStackSupport.forceMaximumQueueableStackDepth = null;
            QueuesFlowProducerDependencyResolver.forceSubmissionSlotAvailable = null;
            QueuesFlowProducerDependencyResolver.forceSubmissionException = null;
        }

        // Assert
        Assert.areEqual(false, enqueued);
    }

    @IsTest
    static void should_reject_direct_submission_when_native_submission_fails() {
        // Arrange
        QueuesFlowProducerDependencyResolver.forceSubmissionSlotAvailable = true;
        QueuesFlowProducerDependencyResolver.forceSubmissionException = new System.AsyncException(
            'native queueable submission rejected'
        );

        // Act
        Boolean enqueued;
        try {
            enqueued = QueuesFlowProducerDependencyResolver.tryEnqueue(new Set<Id>(), new Set<Id>(), new Set<String>());
        } finally {
            QueuesFlowProducerDependencyResolver.forceSubmissionSlotAvailable = null;
            QueuesFlowProducerDependencyResolver.forceSubmissionException = null;
        }

        // Assert
        Assert.areEqual(false, enqueued);
    }

    private static Integer queryResolverCount() {
        return [
            SELECT COUNT()
            FROM AsyncApexJob
            WHERE ApexClass.Name = 'QueuesFlowProducerDependencyResolver'
        ];
    }
}
