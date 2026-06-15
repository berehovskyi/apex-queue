trigger QueuesQueueErrorEventTrigger on QueueErrorEvent__e(after insert) {
    new QueuesQueueErrorEventHandler().handle(Trigger.new);
}
