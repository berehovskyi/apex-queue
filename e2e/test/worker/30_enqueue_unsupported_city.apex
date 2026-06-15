Queues.Job job = Queues.of('e2e-weather-sync')
    .add(
        'fetch-weather',
        'QueuesE2EWeatherProcessor',
        new Map<String, Object>{ 'city' => 'Atlantis' },
        new Queues.JobOptions().jobId('weather:unsupported-atlantis')
    );

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'queueName' => 'e2e-weather-sync',
            'externalId' => 'weather:unsupported-atlantis',
            'jobId' => String.valueOf(job.getId())
        }
    )
);
