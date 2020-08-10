# OpenWeatherMap Input Plugin

Collect current weather and forecast data from OpenWeatherMap.

To use this plugin you will need an [api key][] (app_id).

City identifiers can be found in the [city list][]. Alternately you
can [search][] by name; the `city_id` can be found as the last digits
of the URL: https://openweathermap.org/city/2643743. Language
identifiers can be found in the [lang list][]. Documentation for
condition ID, icon, and main is at [weather conditions][].

### Configuration

```toml
[[inputs.openweathermap]]
  ## OpenWeatherMap API key.
  app_id = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

  ## City ID's to collect weather data from.
  city_id = ["5391959"]

  ## Language of the description field. Can be one of "ar", "bg",
  ## "ca", "cz", "de", "el", "en", "fa", "fi", "fr", "gl", "hr", "hu",
  ## "it", "ja", "kr", "la", "lt", "mk", "nl", "pl", "pt", "ro", "ru",
  ## "se", "sk", "sl", "es", "tr", "ua", "vi", "zh_cn", "zh_tw"
  # lang = "en"

  ## APIs to fetch; can contain "weather" or "forecast".
  fetch = ["weather", "forecast"]

  ## OpenWeatherMap base URL
  # base_url = "https://api.openweathermap.org/"

  ## Timeout for HTTP response.
  # response_timeout = "5s"

  ## Preferred unit system for temperature and wind speed. Can be one of
  ## "metric", "imperial", or "standard".
  # units = "metric"

  ## Query interval; OpenWeatherMap weather data is updated every 10
  ## minutes.
  interval = "10m"
```

### Metrics

- weather
  - tags:
    - city_id
    - forecast
    - condition_id
    - condition_main
  - fields:
    - cloudiness (int, percent)
    - humidity (int, percent)
    - pressure (float, atmospheric pressure hPa)
    - rain (float, rain volume for the last 1-3 hours (depending on API response) in mm)
    - sunrise (int, nanoseconds since unix epoch)
    - sunset (int, nanoseconds since unix epoch)
    - temperature (float, degrees)
    - visibility (int, meters, not available on forecast data)
    - wind_degrees (float, wind direction in degrees)
    - wind_speed (float, wind speed in meters/sec or miles/sec)
    - condition_description (string, localized long description)
    - condition_icon


### Example Output

```
> weather,city=San\ Francisco,city_id=5391959,condition_id=800,condition_main=Clear,country=US,forecast=* cloudiness=1i,condition_description="clear sky",condition_icon="01d",humidity=35i,pressure=1012,rain=0,sunrise=1570630329000000000i,sunset=1570671689000000000i,temperature=21.52,visibility=16093i,wind_degrees=280,wind_speed=5.7 1570659256000000000
> weather,city=San\ Francisco,city_id=5391959,condition_id=800,condition_main=Clear,country=US,forecast=3h cloudiness=0i,condition_description="clear sky",condition_icon="01n",humidity=41i,pressure=1010,rain=0,temperature=22.34,wind_degrees=249.393,wind_speed=2.085 1570665600000000000
> weather,city=San\ Francisco,city_id=5391959,condition_id=800,condition_main=Clear,country=US,forecast=6h cloudiness=0i,condition_description="clear sky",condition_icon="01n",humidity=50i,pressure=1012,rain=0,temperature=17.09,wind_degrees=310.754,wind_speed=3.009 1570676400000000000
```

[api key]: https://openweathermap.org/appid
[city list]: http://bulk.openweathermap.org/sample/city.list.json.gz
[search]: https://openweathermap.org/find
[lang list]: https://openweathermap.org/current#multi
[weather conditions]: https://openweathermap.org/weather-conditions
