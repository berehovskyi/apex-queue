@IsTest(IsParallel=false)
private class QueuesErrorRepositoryTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_insert_single_queue_error() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesErrorRepository repository = new QueuesErrorRepository();
            QueueError__c error = new QueueError__c(
                QueueName__c = 'queue-error-repository-single',
                Component__c = 'worker scheduling',
                Message__c = 'Single insert.',
                OccurredAt__c = System.now()
            );

            // Act
            QueueError__c insertedError = repository.insertQueueError(error);

            // Assert
            Assert.areEqual(true, insertedError.Id != null);
            Assert.areEqual(1, [SELECT COUNT() FROM QueueError__c WHERE Id = :insertedError.Id]);
        }
    }

    @IsTest
    static void should_insert_multiple_queue_errors() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesErrorRepository repository = new QueuesErrorRepository();
            List<QueueError__c> errors = new List<QueueError__c>{
                new QueueError__c(
                    QueueName__c = 'queue-error-repository-bulk-1',
                    Component__c = 'runtime maintenance',
                    Message__c = 'Bulk insert 1.',
                    OccurredAt__c = System.now()
                ),
                new QueueError__c(
                    QueueName__c = 'queue-error-repository-bulk-2',
                    Component__c = 'runtime maintenance',
                    Message__c = 'Bulk insert 2.',
                    OccurredAt__c = System.now()
                )
            };

            // Act
            List<QueueError__c> insertedErrors = repository.insertQueueErrors(errors);
            List<QueueError__c> insertedSystemErrors = repository.insertQueueErrors(
                new List<QueueError__c>{
                    new QueueError__c(
                        QueueName__c = 'queue-error-repository-system-bulk',
                        Component__c = 'runtime maintenance',
                        Message__c = 'System bulk insert.',
                        OccurredAt__c = System.now()
                    )
                }
            );

            // Assert
            Assert.areEqual(2, insertedErrors.size());
            Assert.areEqual(1, insertedSystemErrors.size());
            Assert.areEqual(true, insertedErrors[0].Id != null);
            Assert.areEqual(true, insertedErrors[1].Id != null);
            Assert.areEqual(true, insertedSystemErrors[0].Id != null);
            Assert.areEqual(
                3,
                [
                    SELECT COUNT()
                    FROM QueueError__c
                    WHERE
                        QueueName__c IN (
                            'queue-error-repository-bulk-1',
                            'queue-error-repository-bulk-2',
                            'queue-error-repository-system-bulk'
                        )
                ]
            );
        }
    }

    @IsTest
    static void should_return_empty_results_when_queue_error_inputs_are_empty() {
        // Arrange
        QueuesErrorRepository repository = new QueuesErrorRepository();

        // Act
        QueueError__c insertedError = repository.insertQueueError(null);
        List<QueueError__c> insertedErrors = repository.insertQueueErrors(new List<QueueError__c>());

        // Assert
        Assert.areEqual(null, insertedError);
        Assert.areEqual(0, insertedErrors.size());
    }
}
