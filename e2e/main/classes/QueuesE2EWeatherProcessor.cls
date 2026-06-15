@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesE2EWeatherProcessor implements Queues.JobProcessor {
    private QueuesE2EWeatherClient weatherClient = new QueuesE2EWeatherClient();

    public Queues.ProcessResult process(final Queues.JobContext ctx) {
        Map<String, Object> data = (Map<String, Object>) ctx.data();
        String city = data?.get('city')?.toString();

        try {
            WeatherSnapshot snapshot = weatherClient.fetchCurrentWeather(city);
            return ctx.complete(snapshot.toReturnValue()).withProgress(snapshot.toProgressValue());
        } catch (Exception ex) {
            return ctx.fail(QueuesSupport.resolveExceptionReason(ex));
        }
    }

    @TestVisible
    private void mockWeatherClient(final QueuesE2EWeatherClient client) {
        weatherClient = client;
    }

    public without sharing class QueuesE2EWeatherClient {
        private final String FORECAST_ENDPOINT = 'https://api.open-meteo.com/v1/forecast';
        private final String CURRENT_FIELDS = 'temperature_2m,wind_speed_10m,weather_code';
        private final Integer CALLOUT_TIMEOUT_MS = 20000;

        private final String SUPPORTED_CITIES_MESSAGE = 'Supported cities: Kyiv, London, Tokyo, Sydney.';

        public WeatherSnapshot fetchCurrentWeather(final String cityName) {
            CityCoordinates city = requireCity(cityName);

            HttpRequest request = new HttpRequest();
            request.setMethod('GET');
            request.setEndpoint(buildEndpoint(city));
            request.setTimeout(CALLOUT_TIMEOUT_MS);

            HttpResponse response = new Http().send(request);
            QueuesSupport.isTrue(
                response.getStatusCode() >= 200 && response.getStatusCode() < 300,
                'Open-Meteo callout failed with status ' + response.getStatusCode() + ': ' + response.getStatus()
            );

            Map<String, Object> payload = (Map<String, Object>) JSON.deserializeUntyped(response.getBody());
            Map<String, Object> current = (Map<String, Object>) payload.get('current');
            QueuesSupport.isTrue(current != null, 'Open-Meteo response did not contain current weather.');

            String observedAt = current.get('time')?.toString();
            QueuesSupport.isTrue(
                String.isNotBlank(observedAt),
                'Open-Meteo response did not contain observation time.'
            );

            Decimal temperature2m = toDecimal(current.get('temperature_2m'));
            Decimal windSpeed10m = toDecimal(current.get('wind_speed_10m'));
            Integer weatherCode = QueuesSupport.toInteger(current.get('weather_code'), null);

            QueuesSupport.isTrue(temperature2m != null, 'Open-Meteo response did not contain temperature_2m.');
            QueuesSupport.isTrue(windSpeed10m != null, 'Open-Meteo response did not contain wind_speed_10m.');
            QueuesSupport.isTrue(weatherCode != null, 'Open-Meteo response did not contain weather_code.');

            return new WeatherSnapshot(
                city.label,
                city.latitude,
                city.longitude,
                payload.get('timezone')?.toString(),
                observedAt,
                temperature2m,
                windSpeed10m,
                weatherCode
            );
        }

        private CityCoordinates requireCity(final String cityName) {
            String normalizedCityName = cityName?.trim()?.toLowerCase();
            if (normalizedCityName == 'kyiv') {
                return new CityCoordinates('Kyiv', 50.4501, 30.5234);
            }
            if (normalizedCityName == 'london') {
                return new CityCoordinates('London', 51.5072, -0.1276);
            }
            if (normalizedCityName == 'tokyo') {
                return new CityCoordinates('Tokyo', 35.6764, 139.6500);
            }
            if (normalizedCityName == 'sydney') {
                return new CityCoordinates('Sydney', -33.8688, 151.2093);
            }

            throw new Queues.QueueException('Unsupported city "' + cityName + '". ' + SUPPORTED_CITIES_MESSAGE);
        }

        private String buildEndpoint(final CityCoordinates city) {
            return FORECAST_ENDPOINT +
                '?latitude=' +
                EncodingUtil.urlEncode(city.latitude.toString(), 'UTF-8') +
                '&longitude=' +
                EncodingUtil.urlEncode(city.longitude.toString(), 'UTF-8') +
                '&current=' +
                EncodingUtil.urlEncode(CURRENT_FIELDS, 'UTF-8') +
                '&timezone=auto&forecast_days=1';
        }

        private Decimal toDecimal(final Object value) {
            String decimalValue = value?.toString();
            return String.isBlank(decimalValue) ? null : Decimal.valueOf(decimalValue);
        }
    }

    public class WeatherSnapshot {
        public String city { get; private set; }
        public Decimal latitude { get; private set; }
        public Decimal longitude { get; private set; }
        public String timezone { get; private set; }
        public String observedAt { get; private set; }
        public Decimal temperature2m { get; private set; }
        public Decimal windSpeed10m { get; private set; }
        public Integer weatherCode { get; private set; }

        public WeatherSnapshot(
            final String city,
            final Decimal latitude,
            final Decimal longitude,
            final String timezone,
            final String observedAt,
            final Decimal temperature2m,
            final Decimal windSpeed10m,
            final Integer weatherCode
        ) {
            this.city = city;
            this.latitude = latitude;
            this.longitude = longitude;
            this.timezone = timezone;
            this.observedAt = observedAt;
            this.temperature2m = temperature2m;
            this.windSpeed10m = windSpeed10m;
            this.weatherCode = weatherCode;
        }

        public Map<String, Object> toReturnValue() {
            return new Map<String, Object>{
                'source' => 'open-meteo',
                'city' => city,
                'latitude' => latitude,
                'longitude' => longitude,
                'timezone' => timezone,
                'observedAt' => observedAt,
                'temperature2m' => temperature2m,
                'windSpeed10m' => windSpeed10m,
                'weatherCode' => weatherCode
            };
        }

        public Map<String, Object> toProgressValue() {
            return new Map<String, Object>{ 'city' => city, 'status' => 'completed', 'observedAt' => observedAt };
        }
    }

    private class CityCoordinates {
        private final String label;
        private final Decimal latitude;
        private final Decimal longitude;

        private CityCoordinates(final String label, final Decimal latitude, final Decimal longitude) {
            this.label = label;
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }
}
