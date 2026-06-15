trigger QueuesBatchApexErrorEventTrigger on BatchApexErrorEvent(after insert) {
    new QueuesBatchApexErrorEventHandler().handle(Trigger.new);
}
