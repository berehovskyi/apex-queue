@IsTest(IsParallel=false)
private class QueuesFlowProducerServiceTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_add_flow_parent_and_children() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesFlowProducerService service = newService(workerProvider);

            Queues.FlowJob flow = new Queues.FlowJob(
                    'root',
                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                )
                .data(new Map<String, Object>{ 'root' => true })
                .child(
                    new Queues.FlowJob(
                            'child',
                            QueuesTestSupport.DEFAULT_QUEUE_NAME,
                            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                        )
                        .data(new Map<String, Object>{ 'child' => true })
                );

            // Act
            Queues.JobNode node = service.add(flow);

            // Assert
            Assert.areEqual(true, node.hasChildren());
            List<Job__c> jobs = [
                SELECT Id, Parent__c, State__c
                FROM Job__c
                ORDER BY Parent__c ASC NULLS FIRST, CreatedDate ASC
            ];
            Assert.areEqual(2, jobs.size());
            Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), jobs[0].State__c);
            Assert.areEqual(Queues.State.WAITING.name(), jobs[1].State__c);
            Assert.areEqual(jobs[0].Id, jobs[1].Parent__c);
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_get_complete_flow_tree() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Queues.JobNode createdRoot = service.add(
                flowJob('root', 'root-id')
                    .child(flowJob('child-a', 'child-a-id').child(flowJob('grandchild', 'grandchild-id')))
                    .child(flowJob('child-b', 'child-b-id'))
            );

            // Act
            Queues.JobNode fetchedRoot = service.getFlow(createdRoot.getJob().getId());

            // Assert
            Assert.areEqual(createdRoot.getJob().getId(), fetchedRoot.getJob().getId());
            Assert.areEqual(2, fetchedRoot.getChildren().size());

            Integer grandchildCount = 0;
            for (Queues.JobNode child : fetchedRoot.getChildren()) {
                grandchildCount += child.getChildren().size();
            }
            Assert.areEqual(1, grandchildCount);
        }
    }

    @IsTest
    static void should_release_parent_when_all_children_complete() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesFlowProducerService service = newService(workerProvider);
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c completedChild = insertJob(Queues.State.COMPLETED, parent.Id);
            Job__c waitingChild = insertJob(Queues.State.WAITING, parent.Id);

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ completedChild });
            parent = [
                SELECT State__c
                FROM Job__c
                WHERE Id = :parent.Id
            ];
            waitingChild.State__c = Queues.State.COMPLETED.name();
            update waitingChild;
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ waitingChild });

            // Assert
            Job__c releasedParent = [
                SELECT State__c
                FROM Job__c
                WHERE Id = :parent.Id
            ];
            Assert.areEqual(Queues.State.WAITING.name(), releasedParent.State__c);
            Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), parent.State__c);
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_defer_parent_resolution_in_restricted_context() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesFlowProducerService service = newService(workerProvider);
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c completedChild = insertJob(Queues.State.COMPLETED, parent.Id);

            // Act
            Test.startTest();
            service.deferParentDependenciesAsSystem(new List<Job__c>{ completedChild });
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Integer submittedResolverCount = Limits.getQueueableJobs();
            Test.stopTest();

            // Assert
            Job__c releasedParent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), parent.State__c);
            Assert.areEqual(1, submittedResolverCount);
            Assert.areEqual(Queues.State.WAITING.name(), releasedParent.State__c);
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_leave_parent_waiting_when_deferred_resolver_has_no_submission_slot() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c completedChild = insertJob(Queues.State.COMPLETED, parent.Id);

            // Act
            Test.startTest();
            QueuesFlowProducerDependencyResolver.forceSubmissionSlotAvailable = false;
            try {
                service.deferParentDependenciesAsSystem(new List<Job__c>{ completedChild });
            } finally {
                QueuesFlowProducerDependencyResolver.forceSubmissionSlotAvailable = null;
            }
            Integer submittedResolverCount = Limits.getQueueableJobs();
            Test.stopTest();

            // Assert
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Assert.areEqual(0, submittedResolverCount);
            Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), parent.State__c);
        }
    }

    @IsTest
    static void should_release_parent_when_remaining_child_is_removed() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesFlowProducerService service = newService(workerProvider);
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c waitingChild = insertJob(Queues.State.WAITING, parent.Id);

            // Act
            delete waitingChild;
            service.resolveRemovedParentDependenciesAsSystem(new List<Job__c>{ waitingChild });

            // Assert
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Assert.areEqual(Queues.State.WAITING.name(), parent.State__c);
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_fail_parent_when_child_fails() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c failedChild = insertJob(Queues.State.FAILED, parent.Id, 'FAIL_PARENT', null);

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ failedChild });

            // Assert
            Job__c failedParent = [
                SELECT FailedReason__c, State__c
                FROM Job__c
                WHERE Id = :parent.Id
            ];
            Assert.areEqual(Queues.State.FAILED.name(), failedParent.State__c);
            Assert.areEqual('Child job failed.', failedParent.FailedReason__c);
        }
    }

    @IsTest
    static void should_not_dispatch_existing_idempotent_leaves_again() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesFlowProducerService service = newService(workerProvider);
            Queues.FlowJob flow = flowJob('root', 'root-id').child(flowJob('child', 'child-id'));

            // Act
            Queues.JobNode inserted = service.add(flow);
            Queues.JobNode replayed = service.add(flow);

            // Assert
            Assert.areEqual(2, [SELECT COUNT() FROM Job__c]);
            Assert.areEqual(inserted.getJob().getId(), replayed.getJob().getId());
            Assert.areEqual(inserted.getChildren()[0].getJob().getId(), replayed.getChildren()[0].getJob().getId());
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_allow_fresh_multi_level_graph() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());

            // Act
            Queues.JobNode root = service.add(
                flowJob('root', 'fresh-root-id')
                    .child(flowJob('child', 'fresh-child-id').child(flowJob('grandchild', 'fresh-grandchild-id')))
            );

            // Assert
            Assert.areEqual(3, [SELECT COUNT() FROM Job__c]);
            Assert.areEqual(1, root.getChildren().size());
            Assert.areEqual(1, root.getChildren()[0].getChildren().size());
        }
    }

    @IsTest
    static void should_reject_new_children_for_existing_waiting_parent() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING_CHILDREN.name(),
                'root-id'
            );
            insert parent;

            // Act
            try {
                service.add(
                    flowJob('root', 'root-id')
                        .child(
                            new Queues.FlowJob(
                                'new-child',
                                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                            )
                        )
                );
                Assert.fail('Expected existing flow extension to be rejected.');
            } catch (Queues.QueueException ex) {
                // Assert
                Assert.areEqual(
                    'Existing flow jobs can only participate in an idempotent replay; adding new children is not supported.',
                    ex.getMessage()
                );
            }
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c]);
        }
    }

    @IsTest
    static void should_reject_mixed_existing_and_new_children_for_existing_parent() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING_CHILDREN.name(),
                'root-id'
            );
            insert parent;
            Job__c existingChild = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'existing-child-id'
            );
            existingChild.Parent__c = parent.Id;
            insert existingChild;

            // Act
            try {
                service.add(
                    flowJob('root', 'root-id')
                        .child(flowJob('existing-child', 'existing-child-id'))
                        .child(
                            new Queues.FlowJob(
                                'new-child',
                                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                            )
                        )
                );
                Assert.fail('Expected mixed existing/new dependency replay to be rejected.');
            } catch (Queues.QueueException ex) {
                // Assert
                Assert.areEqual(
                    'Existing flow jobs can only participate in an idempotent replay; adding new children is not supported.',
                    ex.getMessage()
                );
            }
            Assert.areEqual(2, [SELECT COUNT() FROM Job__c]);
        }
    }

    @IsTest
    static void should_roll_back_complete_bulk_when_existing_flow_extension_is_rejected() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c existingParent = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING_CHILDREN.name(),
                'existing-root-id'
            );
            insert existingParent;

            // Act
            try {
                service.addBulk(
                    new List<Queues.FlowJob>{
                        flowJob('fresh-root', 'fresh-root-id').child(flowJob('fresh-child', 'fresh-child-id')),
                        flowJob('existing-root', 'existing-root-id')
                            .child(
                                new Queues.FlowJob(
                                    'new-child',
                                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                                )
                            )
                    }
                );
                Assert.fail('Expected bulk flow extension to be rejected.');
            } catch (Queues.QueueException ex) {
                // Assert
                Assert.areEqual(
                    'Existing flow jobs can only participate in an idempotent replay; adding new children is not supported.',
                    ex.getMessage()
                );
            }
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c]);
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE ExternalId__c = 'fresh-root-id']);
        }
    }

    @IsTest
    static void should_resolve_duplicate_idempotent_insert_race() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            DuplicateInsertRaceRepositoryStubProvider repositoryProvider = new DuplicateInsertRaceRepositoryStubProvider();
            QueuesJobRepository repository = (QueuesJobRepository) Test.createStub(
                QueuesJobRepository.class,
                repositoryProvider
            );
            QueuesFlowProducerService service = newService(
                workerProvider,
                new QueuesQueueableDispatchService(),
                new QueuesJobService(repository, new QueuesDefinitionService())
            );

            // Act
            Queues.JobNode node = service.add(flowJob('root', 'root-id'));

            // Assert
            Assert.areEqual(repositoryProvider.winner.Id, node.getJob().getId());
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c]);
            Assert.areEqual(2, repositoryProvider.queryJobsByExternalIdsCount);
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_reject_new_children_for_existing_released_parent() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'root-id'
            );
            insert parent;
            Queues.FlowJob flow = flowJob('root', 'root-id')
                .child(
                    new Queues.FlowJob(
                        'child',
                        QueuesTestSupport.DEFAULT_QUEUE_NAME,
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                    )
                );

            // Act
            try {
                service.add(flow);
                Assert.fail('Expected flow dependency mismatch.');
            } catch (Queues.QueueException ex) {
                // Assert
                Assert.areEqual(
                    'Existing flow jobs can only participate in an idempotent replay; adding new children is not supported.',
                    ex.getMessage()
                );
            }
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c]);
        }
    }

    @IsTest
    static void should_reject_nonterminal_existing_children_for_existing_released_parent() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'root-id'
            );
            insert parent;
            Job__c child = QueuesTestSupport.job(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'child-id'
            );
            child.Parent__c = parent.Id;
            insert child;

            // Act
            try {
                service.add(flowJob('root', 'root-id').child(flowJob('child', 'child-id')));
                Assert.fail('Expected flow dependency mismatch.');
            } catch (Queues.QueueException ex) {
                // Assert
                Assert.areEqual('Existing flow job dependencies do not match the requested flow.', ex.getMessage());
            }
            Assert.areEqual(2, [SELECT COUNT() FROM Job__c]);
        }
    }

    @IsTest
    static void should_dispatch_queueable_flow_leaves() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            RecordingQueueableDispatchService queueableDispatchService = new RecordingQueueableDispatchService();
            QueuesFlowProducerService service = newService(workerProvider, queueableDispatchService);

            // Act
            service.add(
                new Queues.FlowJob(
                    'queueable',
                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                ),
                new Queues.FlowOpts().queueable()
            );

            // Assert
            Job__c job = [
                SELECT ExecutionMode__c
                FROM Job__c
                LIMIT 1
            ];
            Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), job.ExecutionMode__c);
            Assert.areEqual(1, queueableDispatchService.createWaitingDispatchCount);
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_create_invocable_flow_leaves_without_dispatch() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            RecordingQueueableDispatchService queueableDispatchService = new RecordingQueueableDispatchService();
            QueuesFlowProducerService service = newService(workerProvider, queueableDispatchService);

            // Act
            service.add(
                new Queues.FlowJob(
                    'invocable',
                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                ),
                new Queues.FlowOpts().invocable()
            );

            // Assert
            Job__c job = [
                SELECT ExecutionMode__c
                FROM Job__c
                LIMIT 1
            ];
            Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), job.ExecutionMode__c);
            Assert.areEqual(0, queueableDispatchService.createWaitingDispatchCount);
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_return_empty_nodes_when_bulk_flow_is_empty() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesFlowProducerService service = newService(workerProvider);

            // Act
            List<Queues.JobNode> nodes = service.addBulk(new List<Queues.FlowJob>());

            // Assert
            Assert.areEqual(0, nodes.size());
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c]);
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_resolve_queueable_delay_from_available_at() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            RecordingQueueableDispatchService queueableDispatchService = new RecordingQueueableDispatchService();
            QueuesFlowProducerService service = newService(
                new QueuesTestSupport.RecordingStubProvider(),
                queueableDispatchService
            );

            // Act
            service.add(
                flowJob('queueable-available-at', 'queueable-available-at-id')
                    .opts(
                        new Queues.JobOptions()
                            .jobId('queueable-available-at-id')
                            .availableAt(System.now().addMinutes(1))
                    ),
                new Queues.FlowOpts().queueable()
            );

            // Assert
            Assert.areEqual(1, queueableDispatchService.createWaitingDispatchCount);
        }
    }

    @IsTest
    static void should_reject_duplicate_queue_scoped_job_ids_in_same_graph() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesFlowProducerService service = newService(workerProvider);

            // Act
            try {
                service.addBulk(
                    new List<Queues.FlowJob>{
                        flowJob('duplicate-a', 'duplicate-id'),
                        flowJob('duplicate-b', 'DUPLICATE-ID')
                    }
                );
                Assert.fail('Expected duplicate flow jobId values to be rejected.');
            } catch (Queues.QueueException ex) {
                // Assert
                Assert.areEqual('Flow jobId values must be unique within each queue.', ex.getMessage());
            }

            // Assert
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c]);
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_reject_parent_option_in_flow_job() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Queues.FlowJob flow = flowJob('parent-option', 'parent-option-id')
                .opts(
                    new Queues.JobOptions()
                        .jobId('parent-option-id')
                        .parent(QueuesTestSupport.fakeId(Job__c.SObjectType, 901))
                );

            // Act
            try {
                service.add(flow);
                Assert.fail('Expected FlowJob opts.parent(...) to be rejected.');
            } catch (Queues.QueueException ex) {
                // Assert
                Assert.areEqual(
                    'Flow jobs do not support opts.parent(...). Define dependencies with child(...).',
                    ex.getMessage()
                );
            }
        }
    }

    @IsTest
    static void should_reject_structural_cycles() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Queues.FlowJob flow = flowJob('cycle', 'cycle-id');
            flow.child(flow);

            // Act
            try {
                service.add(flow);
                Assert.fail('Expected cycle to be rejected.');
            } catch (Queues.QueueException ex) {
                // Assert
                Assert.areEqual('Flow jobs cannot form cycles.', ex.getMessage());
            }
        }
    }

    @IsTest
    static void should_reject_reused_flow_job_instances() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Queues.FlowJob reusedChild = flowJob('reused-child', 'reused-child-id');
            Queues.FlowJob flow = flowJob('reused-root', 'reused-root-id').child(reusedChild).child(reusedChild);

            // Act
            try {
                service.add(flow);
                Assert.fail('Expected reused flow job instance to be rejected.');
            } catch (Queues.QueueException ex) {
                // Assert
                Assert.areEqual('Flow job instances cannot be reused within one graph.', ex.getMessage());
            }
        }
    }

    @IsTest
    static void should_raise_insert_failure_for_invalid_idempotent_job() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Queues.FlowJob invalidFlow = new Queues.FlowJob(
                    repeat('x', 121),
                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS
                )
                .opts(new Queues.JobOptions().jobId('invalid-idempotent-job'));

            // Act
            try {
                service.add(invalidFlow);
                Assert.fail('Expected invalid flow job insert to fail.');
            } catch (Queues.QueueException ex) {
                // Assert
                Assert.areNotEqual(null, ex.getMessage());
            }
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c]);
        }
    }

    @IsTest
    static void should_skip_dependency_resolution_for_parent_not_waiting_on_children() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesFlowProducerService service = newService(workerProvider);
            Job__c parent = insertJob(Queues.State.WAITING, null);
            Job__c completedChild = insertJob(Queues.State.COMPLETED, parent.Id);

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ completedChild });

            // Assert
            parent = [
                SELECT State__c
                FROM Job__c
                WHERE Id = :parent.Id
            ];
            Assert.areEqual(Queues.State.WAITING.name(), parent.State__c);
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_cancel_parent_when_child_is_canceled() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c canceledChild = insertJob(Queues.State.CANCELED, parent.Id, null, 'CANCEL_PARENT');

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ canceledChild });

            // Assert
            parent = [
                SELECT FailedReason__c, State__c
                FROM Job__c
                WHERE Id = :parent.Id
            ];
            Assert.areEqual(Queues.State.CANCELED.name(), parent.State__c);
            Assert.areEqual('Child job was canceled.', parent.FailedReason__c);
        }
    }

    @IsTest
    static void should_leave_parent_waiting_when_failed_child_uses_default_policy() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c failedChild = insertJob(Queues.State.FAILED, parent.Id);

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ failedChild });

            // Assert
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), parent.State__c);
        }
    }

    @IsTest
    static void should_leave_parent_waiting_when_canceled_child_uses_default_policy() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c canceledChild = insertJob(Queues.State.CANCELED, parent.Id);

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ canceledChild });

            // Assert
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), parent.State__c);
        }
    }

    @IsTest
    static void should_release_parent_immediately_when_failed_child_continues_parent() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c failedChild = insertJob(Queues.State.FAILED, parent.Id, 'CONTINUE_PARENT', null);
            insertJob(Queues.State.WAITING, parent.Id);

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ failedChild });

            // Assert
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Assert.areEqual(Queues.State.WAITING.name(), parent.State__c);
        }
    }

    @IsTest
    static void should_release_parent_after_remaining_child_completes_when_failed_dependency_is_ignored() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c failedChild = insertJob(Queues.State.FAILED, parent.Id, 'IGNORE_DEPENDENCY', null);
            Job__c waitingChild = insertJob(Queues.State.WAITING, parent.Id);

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ failedChild });
            waitingChild.State__c = Queues.State.COMPLETED.name();
            update waitingChild;
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ waitingChild });

            // Assert
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Assert.areEqual(Queues.State.WAITING.name(), parent.State__c);
        }
    }

    @IsTest
    static void should_remove_failed_dependency_and_release_parent() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c failedChild = insertJob(Queues.State.FAILED, parent.Id, 'REMOVE_DEPENDENCY', null);

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ failedChild });

            // Assert
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            failedChild = [SELECT Parent__c FROM Job__c WHERE Id = :failedChild.Id];
            Assert.areEqual(Queues.State.WAITING.name(), parent.State__c);
            Assert.areEqual(null, failedChild.Parent__c);
        }
    }

    @IsTest
    static void should_skip_removed_dependency_cleanup_query_when_no_removed_dependency_exists() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Set<String> suppressedWorkerQueueNames = new Set<String>{ QueuesTestSupport.DEFAULT_QUEUE_NAME };
            Job__c ignoreParent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c ignoreTrigger = insertJob(Queues.State.COMPLETED, ignoreParent.Id);
            insertJob(Queues.State.FAILED, ignoreParent.Id, 'IGNORE_DEPENDENCY', null);
            Job__c removeParent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c removeTrigger = insertJob(Queues.State.COMPLETED, removeParent.Id);
            insertJob(Queues.State.FAILED, removeParent.Id, 'REMOVE_DEPENDENCY', null);

            // Act
            Integer queriesBeforeIgnore = Limits.getQueries();
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ ignoreTrigger }, suppressedWorkerQueueNames);
            Integer ignoreQueries = Limits.getQueries() - queriesBeforeIgnore;
            Integer queriesBeforeRemove = Limits.getQueries();
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ removeTrigger }, suppressedWorkerQueueNames);
            Integer removeQueries = Limits.getQueries() - queriesBeforeRemove;

            // Assert
            Assert.areEqual(
                ignoreQueries + 1,
                removeQueries,
                'Removed-dependency cleanup query should run only when a removed dependency exists.'
            );
        }
    }

    @IsTest
    static void should_clean_up_stale_removed_dependency_when_sibling_triggers_resolution() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c failedChild = insertJob(Queues.State.FAILED, parent.Id, 'REMOVE_DEPENDENCY', null);
            Job__c completedChild = insertJob(Queues.State.COMPLETED, parent.Id);

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ completedChild });

            // Assert
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            failedChild = [SELECT Parent__c FROM Job__c WHERE Id = :failedChild.Id];
            Assert.areEqual(Queues.State.WAITING.name(), parent.State__c);
            Assert.areEqual(null, failedChild.Parent__c);
        }
    }

    @IsTest
    static void should_remove_failed_dependency_after_parent_is_already_released() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING, null);
            Job__c failedChild = insertJob(Queues.State.FAILED, parent.Id, 'REMOVE_DEPENDENCY', null);

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ failedChild });

            // Assert
            failedChild = [SELECT Parent__c FROM Job__c WHERE Id = :failedChild.Id];
            Assert.areEqual(null, failedChild.Parent__c);
        }
    }

    @IsTest
    static void should_fail_parent_when_canceled_child_uses_failure_policy() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c canceledChild = insertJob(Queues.State.CANCELED, parent.Id, null, 'FAIL_PARENT');

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ canceledChild });

            // Assert
            parent = [SELECT FailedReason__c, State__c FROM Job__c WHERE Id = :parent.Id];
            Assert.areEqual(Queues.State.FAILED.name(), parent.State__c);
            Assert.areEqual('Child job was canceled.', parent.FailedReason__c);
        }
    }

    @IsTest
    static void should_release_parent_when_canceled_dependency_is_ignored() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c canceledChild = insertJob(Queues.State.CANCELED, parent.Id, null, 'IGNORE_DEPENDENCY');

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ canceledChild });

            // Assert
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Assert.areEqual(Queues.State.WAITING.name(), parent.State__c);
        }
    }

    @IsTest
    static void should_propagate_terminal_parent_to_grandparent() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c grandparent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, grandparent.Id, 'FAIL_PARENT', null);
            Job__c failedChild = insertJob(Queues.State.FAILED, parent.Id, 'FAIL_PARENT', null);

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ failedChild });

            // Assert
            Map<Id, Job__c> jobsById = new Map<Id, Job__c>(
                [
                    SELECT State__c
                    FROM Job__c
                    WHERE Id IN :new Set<Id>{ grandparent.Id, parent.Id }
                ]
            );
            Assert.areEqual(Queues.State.FAILED.name(), jobsById.get(parent.Id).State__c);
            Assert.areEqual(Queues.State.FAILED.name(), jobsById.get(grandparent.Id).State__c);
        }
    }

    @IsTest
    static void should_continue_parent_propagation_asynchronously_when_level_limit_is_reached() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c grandparent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, grandparent.Id, 'FAIL_PARENT', null);
            Job__c failedChild = insertJob(Queues.State.FAILED, parent.Id, 'FAIL_PARENT', null);

            // Act
            Test.startTest();
            QueuesFlowProducerService.forceDependencyResolutionBatchCountLimit = 1;
            try {
                service.resolveParentDependenciesAsSystem(new List<Job__c>{ failedChild });
            } finally {
                QueuesFlowProducerService.forceDependencyResolutionBatchCountLimit = null;
            }
            Test.stopTest();

            // Assert
            Map<Id, Job__c> jobsById = new Map<Id, Job__c>(
                [
                    SELECT State__c
                    FROM Job__c
                    WHERE Id IN :new Set<Id>{ grandparent.Id, parent.Id }
                ]
            );
            Assert.areEqual(Queues.State.FAILED.name(), jobsById.get(parent.Id).State__c);
            Assert.areEqual(Queues.State.FAILED.name(), jobsById.get(grandparent.Id).State__c);
        }
    }

    @IsTest
    static void should_continue_cross_queue_parent_propagation_asynchronously() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c grandparent = insertJob(Queues.State.WAITING_CHILDREN, null);
            grandparent.QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME + '-other';
            update grandparent;
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, grandparent.Id, 'FAIL_PARENT', null);
            Job__c failedChild = insertJob(Queues.State.FAILED, parent.Id, 'FAIL_PARENT', null);

            // Act
            Test.startTest();
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ failedChild });
            Job__c waitingGrandparent = [SELECT State__c FROM Job__c WHERE Id = :grandparent.Id];
            Test.stopTest();

            // Assert
            Job__c failedGrandparent = [SELECT State__c FROM Job__c WHERE Id = :grandparent.Id];
            Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), waitingGrandparent.State__c);
            Assert.areEqual(Queues.State.FAILED.name(), failedGrandparent.State__c);
        }
    }

    @IsTest
    static void should_resolve_parent_dependencies_in_bounded_batches() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            List<Job__c> completedChildren = new List<Job__c>();
            for (Integer index = 0; index < 11; index++) {
                Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
                completedChildren.add(insertJob(Queues.State.COMPLETED, parent.Id));
            }

            // Act
            service.resolveParentDependenciesAsSystem(completedChildren);

            // Assert
            String waitingState = Queues.State.WAITING.name();
            Assert.areEqual(11, [SELECT COUNT() FROM Job__c WHERE State__c = :waitingState]);
        }
    }

    @IsTest
    static void should_dispatch_released_queueable_parent_as_system() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            RecordingQueueableDispatchService queueableDispatchService = new RecordingQueueableDispatchService();
            QueuesFlowProducerService service = newService(workerProvider, queueableDispatchService);
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            parent.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            update parent;
            Job__c completedChild = insertJob(Queues.State.COMPLETED, parent.Id);

            // Act
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ completedChild });

            // Assert
            Assert.areEqual(1, queueableDispatchService.createWaitingDispatchAsSystemCount);
            Assert.areEqual(0, queueableDispatchService.createWaitingDispatchCount);
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_repair_stale_waiting_children_parent() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesFlowProducerService service = newService(workerProvider);
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            insertJob(Queues.State.COMPLETED, parent.Id);

            // Act
            Integer repairedCount = service.repairStaleParentDependenciesAsSystem(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                System.now().addMinutes(1)
            );

            // Assert
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Assert.areEqual(1, repairedCount);
            Assert.areEqual(Queues.State.WAITING.name(), parent.State__c);
            Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_rotate_stale_waiting_children_repairs_past_unresolved_parents() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            for (Integer index = 0; index < 50; index++) {
                Job__c blockedParent = insertJob(Queues.State.WAITING_CHILDREN, null);
                insertJob(Queues.State.WAITING, blockedParent.Id);
            }
            Job__c repairableParent = insertJob(Queues.State.WAITING_CHILDREN, null);
            insertJob(Queues.State.COMPLETED, repairableParent.Id);
            Datetime staleBefore = System.now().addMinutes(1);

            // Act
            Integer firstRepairCount = service.repairStaleParentDependenciesAsSystem(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                staleBefore
            );
            Integer secondRepairCount = service.repairStaleParentDependenciesAsSystem(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                staleBefore
            );

            // Assert
            repairableParent = [SELECT State__c FROM Job__c WHERE Id = :repairableParent.Id];
            Assert.areEqual(0, firstRepairCount);
            Assert.areEqual(1, secondRepairCount);
            Assert.areEqual(Queues.State.WAITING.name(), repairableParent.State__c);
        }
    }

    @IsTest
    static void should_continue_removed_dependency_cleanup_in_bounded_batches() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            List<Job__c> failedChildren = new List<Job__c>();
            for (Integer index = 0; index < 201; index++) {
                Job__c failedChild = QueuesTestSupport.job(Queues.State.FAILED.name());
                failedChild.Parent__c = parent.Id;
                failedChild.DependencyFailurePolicy__c = Queues.DependencyFailurePolicy.REMOVE_DEPENDENCY.name();
                failedChildren.add(failedChild);
            }
            insert failedChildren;
            Job__c completedChild = insertJob(Queues.State.COMPLETED, parent.Id);

            // Act
            Test.startTest();
            service.resolveParentDependenciesAsSystem(new List<Job__c>{ completedChild });
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            String failedState = Queues.State.FAILED.name();
            String removePolicy = Queues.DependencyFailurePolicy.REMOVE_DEPENDENCY.name();
            Integer attachedBeforeContinuation = [
                SELECT COUNT()
                FROM Job__c
                WHERE Parent__c = :parent.Id AND State__c = :failedState AND DependencyFailurePolicy__c = :removePolicy
            ];
            Test.stopTest();

            // Assert
            Assert.areEqual(Queues.State.WAITING.name(), parent.State__c);
            Assert.areEqual(1, attachedBeforeContinuation);
            Assert.areEqual(
                0,
                [
                    SELECT COUNT()
                    FROM Job__c
                    WHERE
                        Parent__c = :parent.Id
                        AND State__c = :failedState
                        AND DependencyFailurePolicy__c = :removePolicy
                ]
            );
        }
    }

    @IsTest
    static void should_repair_stale_removed_dependencies_after_parent_release() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesFlowProducerService service = newService(new QueuesTestSupport.RecordingStubProvider());
            Job__c parent = insertJob(Queues.State.WAITING, null);
            Job__c failedChild = insertJob(Queues.State.FAILED, parent.Id, 'REMOVE_DEPENDENCY', null);

            // Act
            Integer repairedCount = service.repairStaleRemovedFailureDependenciesAsSystem(
                QueuesTestSupport.DEFAULT_QUEUE_NAME,
                System.now().addMinutes(1)
            );

            // Assert
            failedChild = [SELECT Parent__c FROM Job__c WHERE Id = :failedChild.Id];
            Assert.areEqual(1, repairedCount);
            Assert.areEqual(null, failedChild.Parent__c);
        }
    }

    @IsTest
    static void should_suppress_worker_dispatch_for_released_parent() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesFlowProducerService service = newService(workerProvider);
            Job__c parent = insertJob(Queues.State.WAITING_CHILDREN, null);
            Job__c completedChild = insertJob(Queues.State.COMPLETED, parent.Id);

            // Act
            service.resolveParentDependenciesAsSystem(
                new List<Job__c>{ completedChild },
                new Set<String>{ QueuesTestSupport.DEFAULT_QUEUE_NAME }
            );

            // Assert
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Assert.areEqual(Queues.State.WAITING.name(), parent.State__c);
            Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
        }
    }

    @IsTest
    static void should_reserve_dependency_resolution_headroom() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Act + Assert
            Assert.areEqual(true, QueuesFlowProducerService.hasDependencyResolutionHeadroom(100, 10, 15, 2000));
            Assert.areEqual(false, QueuesFlowProducerService.hasDependencyResolutionHeadroom(99, 10, 15, 2000));
            Assert.areEqual(false, QueuesFlowProducerService.hasDependencyResolutionHeadroom(100, 9, 15, 2000));
            Assert.areEqual(false, QueuesFlowProducerService.hasDependencyResolutionHeadroom(100, 10, 14, 2000));
            Assert.areEqual(false, QueuesFlowProducerService.hasDependencyResolutionHeadroom(100, 10, 15, 1999));
        }
    }

    private static QueuesFlowProducerService newService(final QueuesTestSupport.RecordingStubProvider workerProvider) {
        return newService(workerProvider, new QueuesQueueableDispatchService());
    }

    private static QueuesFlowProducerService newService(
        final QueuesTestSupport.RecordingStubProvider workerProvider,
        final QueuesQueueableDispatchService queueableDispatchService
    ) {
        return newService(workerProvider, queueableDispatchService, new QueuesJobService());
    }

    private static QueuesFlowProducerService newService(
        final QueuesTestSupport.RecordingStubProvider workerProvider,
        final QueuesQueueableDispatchService queueableDispatchService,
        final QueuesJobService jobService
    ) {
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
        QueuesDefinitionService definitionService = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'findQueueDefinitionsByQueueNamesAsSystem',
                    new Map<String, QueueDefinition__mdt>{ QueuesTestSupport.DEFAULT_QUEUE_NAME => queueDefinition }
                )
        );
        QueuesProcessorService processorService = (QueuesProcessorService) Test.createStub(
            QueuesProcessorService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'requireProcessors',
                    new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
                )
        );
        QueuesWorkerService workerService = (QueuesWorkerService) Test.createStub(
            QueuesWorkerService.class,
            workerProvider
        );
        return new QueuesFlowProducerService(
            definitionService,
            jobService,
            processorService,
            queueableDispatchService,
            new QueuesRuntimeService(),
            workerService,
            new QueuesQueueEventService()
        );
    }

    private static Queues.FlowJob flowJob(final String name, final String jobId) {
        return new Queues.FlowJob(name, QueuesTestSupport.DEFAULT_QUEUE_NAME, QueuesTestSupport.DEFAULT_PROCESSOR_CLASS)
            .opts(new Queues.JobOptions().jobId(jobId));
    }

    private static String repeat(final String value, final Integer count) {
        String repeated = '';
        for (Integer index = 0; index < count; index++) {
            repeated += value;
        }
        return repeated;
    }

    private static Job__c insertJob(final Queues.State state, final Id parentId) {
        return insertJob(state, parentId, null, null);
    }

    private static Job__c insertJob(
        final Queues.State state,
        final Id parentId,
        final String dependencyFailurePolicy,
        final String dependencyCanceledPolicy
    ) {
        Job__c job = QueuesTestSupport.job(state.name());
        job.Parent__c = parentId;
        if (dependencyFailurePolicy != null) {
            job.DependencyFailurePolicy__c = dependencyFailurePolicy;
        }
        if (dependencyCanceledPolicy != null) {
            job.DependencyCanceledPolicy__c = dependencyCanceledPolicy;
        }
        insert job;
        return job;
    }

    private class RecordingQueueableDispatchService extends QueuesQueueableDispatchService {
        private Integer createWaitingDispatchCount = 0;
        private Integer createWaitingDispatchAsSystemCount = 0;

        public override QueueableDispatch__c createWaitingDispatch(
            final String queueName,
            final String dedupeKey,
            final Queues.ExecutionMode executionMode,
            final List<Job__c> jobs,
            final Boolean asSystem
        ) {
            if (asSystem == true) {
                createWaitingDispatchAsSystemCount++;
            } else {
                createWaitingDispatchCount++;
            }
            return new QueueableDispatch__c(
                QueueName__c = queueName,
                DedupeKey__c = dedupeKey,
                Mode__c = executionMode.name(),
                State__c = Queues.QueueableDispatchState.WAITING.name()
            );
        }
    }

    private class DuplicateInsertRaceRepositoryStubProvider implements System.StubProvider {
        private Integer queryJobsByExternalIdsCount = 0;
        private Job__c winner;

        public Object handleMethodCall(
            final Object stubbedObject,
            final String stubbedMethodName,
            final Type returnType,
            final List<Type> listOfParamTypes,
            final List<String> listOfParamNames,
            final List<Object> listOfArgs
        ) {
            switch on stubbedMethodName {
                when 'queryJobsByExternalIds' {
                    queryJobsByExternalIdsCount++;
                    return queryJobsByExternalIdsCount == 1 ? new List<Job__c>() : new List<Job__c>{ winner };
                }
                when 'insertJobs' {
                    return listOfArgs[0];
                }
                when 'insertJobsAllowingPartialSuccess' {
                    List<Job__c> jobs = (List<Job__c>) listOfArgs[0];
                    winner = jobs[0].clone(false, true, false, false);
                    insert winner;
                    return Database.insert(jobs, false);
                }
                when else {
                    return null;
                }
            }
        }
    }
}
