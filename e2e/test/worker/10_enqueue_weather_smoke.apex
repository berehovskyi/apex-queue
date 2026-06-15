List<Queues.Job> jobs = Queues.of('e2e-weather-sync')
    .addBulk(
        new List<Queues.JobData>{
            new Queues.JobData(
                'fetch-weather',
                'QueuesE2EWeatherProcessor',
                new Map<String, Object>{ 'city' => 'Kyiv' },
                new Queues.JobOptions().jobId('weather:kyiv')
            ),
            new Queues.JobData(
                'fetch-weather',
                'QueuesE2EWeatherProcessor',
                new Map<String, Object>{ 'city' => 'London' },
                new Queues.JobOptions().jobId('weather:london')
            ),
            new Queues.JobData(
                'fetch-weather',
                'QueuesE2EWeatherProcessor',
                new Map<String, Object>{ 'city' => 'Tokyo' },
                new Queues.JobOptions().jobId('weather:tokyo')
            )
        }
    );

List<String> jobIds = new List<String>();
for (Queues.Job job : jobs) {
    jobIds.add(String.valueOf(job.getId()));
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'queueName' => 'e2e-weather-sync',
            'externalIds' => new List<String>{ 'weather:kyiv', 'weather:london', 'weather:tokyo' },
            'jobIds' => jobIds
        }
    )
);
