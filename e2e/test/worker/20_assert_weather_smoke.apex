Set<String> externalIds = new Set<String>{ 'weather:kyiv', 'weather:london', 'weather:tokyo' };

List<Job__c> jobs = [
    SELECT Id, ExternalId__c, State__c, FailedReason__c, ReturnValue__c, Progress__c
    FROM Job__c
    WHERE QueueName__c = 'e2e-weather-sync' AND ExternalId__c IN :externalIds
    ORDER BY ExternalId__c ASC
];
QueuesSupport.isTrue(
    jobs.size() == externalIds.size(),
    'Expected ' + externalIds.size() + ' smoke jobs but found ' + jobs.size() + '.'
);

List<String> unfinishedJobs = new List<String>();
List<String> completedCities = new List<String>();
for (Job__c job : jobs) {
    Queues.State state = QueuesSupport.toState(job.State__c);
    if (state != Queues.State.COMPLETED) {
        unfinishedJobs.add(job.ExternalId__c + '=' + job.State__c + ' (' + job.FailedReason__c + ')');
        continue;
    }

    Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(job.ReturnValue__c);
    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);

    QueuesSupport.isTrue(returnValue != null, 'Job "' + job.ExternalId__c + '" did not store a return value.');
    QueuesSupport.isTrue(progress != null, 'Job "' + job.ExternalId__c + '" did not store progress.');
    QueuesSupport.isTrue(
        String.valueOf(returnValue.get('source')) == 'open-meteo',
        'Job "' + job.ExternalId__c + '" did not use the Open-Meteo source.'
    );
    QueuesSupport.isTrue(
        String.isNotBlank(String.valueOf(returnValue.get('observedAt'))),
        'Job "' + job.ExternalId__c + '" did not store observedAt.'
    );
    QueuesSupport.isTrue(
        returnValue.get('temperature2m') != null,
        'Job "' + job.ExternalId__c + '" did not store temperature2m.'
    );
    QueuesSupport.isTrue(
        returnValue.get('windSpeed10m') != null,
        'Job "' + job.ExternalId__c + '" did not store windSpeed10m.'
    );
    QueuesSupport.isTrue(
        returnValue.get('weatherCode') != null,
        'Job "' + job.ExternalId__c + '" did not store weatherCode.'
    );
    QueuesSupport.isTrue(
        String.valueOf(returnValue.get('city')) == String.valueOf(progress.get('city')),
        'Job "' + job.ExternalId__c + '" city did not match progress.'
    );
    completedCities.add(String.valueOf(returnValue.get('city')));
}

QueuesSupport.isTrue(
    unfinishedJobs.isEmpty(),
    'Weather smoke jobs are not finished yet or failed: ' + String.join(unfinishedJobs, ', ')
);

List<JobRun__c> runs = [
    SELECT Job__r.ExternalId__c, Status__c, Message__c
    FROM JobRun__c
    WHERE Job__r.QueueName__c = 'e2e-weather-sync' AND Job__r.ExternalId__c IN :externalIds
];
QueuesSupport.isTrue(
    runs.size() == externalIds.size(),
    'Expected ' + externalIds.size() + ' job runs but found ' + runs.size() + '.'
);
for (JobRun__c run : runs) {
    QueuesSupport.isTrue(
        QueuesSupport.normalizeEnumValue(run.Status__c) == Queues.RunStatus.COMPLETED.name(),
        'Job run for "' + run.Job__r.ExternalId__c + '" was not completed: ' + run.Status__c
    );
}

Integer queueErrorCount = [SELECT COUNT() FROM QueueError__c WHERE QueueName__c = 'e2e-weather-sync'];
QueuesSupport.isTrue(queueErrorCount == 0, 'Expected no queue errors but found ' + queueErrorCount + '.');

List<QueueRuntime__c> runtimes = [
    SELECT ActiveWorkerCount__c, WorkerToken__c, AsyncApexJobId__c
    FROM QueueRuntime__c
    WHERE QueueName__c = 'e2e-weather-sync'
    LIMIT 1
];
if (!runtimes.isEmpty()) {
    QueueRuntime__c runtime = runtimes[0];
    QueuesSupport.isTrue(
        QueuesSupport.toInteger(runtime.ActiveWorkerCount__c, 0) == 0,
        'Expected the e2e runtime to have zero active workers.'
    );
    QueuesSupport.isTrue(
        String.isBlank(runtime.WorkerToken__c),
        'Expected the e2e runtime worker token to be cleared.'
    );
    QueuesSupport.isTrue(
        String.isBlank(runtime.AsyncApexJobId__c),
        'Expected the e2e runtime async job id to be cleared.'
    );
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'queueName' => 'e2e-weather-sync',
            'completedCities' => completedCities,
            'jobCount' => jobs.size()
        }
    )
);
