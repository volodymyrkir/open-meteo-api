"""This module contains constant values required to extract,
 transform and load Open-Meteo data."""

TARGET_CITIES = [
    'Kyiv', 'Tel Aviv-Yafo', 'Lhasa', 'Ulaanbaatar',
    'Reykjavík', 'Johannesburg', 'Dublin', 'Bern',
    'Brasília', 'Kingston'
]

BASE_WEATHER_URL = 'https://api.open-meteo.com/v1/forecast?'
HOURLY_FIELDS = 'temperature_2m'
GEO_FIELDS = [
    'latitude', 'longitude', 'elevation',
    'timezone_abbreviation', 'hourly.temperature_2m', 'hourly.time'
]

HISTORICAL_WEATHER_URL = 'https://historical-forecast-api.open-meteo.com/v1/forecast?'
HISTORICAL_HOURLY_FIELDS = ['temperature_2m', 'relative_humidity_2m', 'wind_speed_10m']
HISTORICAL_FIELDS = [
    'latitude', 'longitude', 'elevation',
    'hourly.temperature_2m', 'hourly.relative_humidity_2m', 'hourly.wind_speed_10m'
]
HISTORICAL_DATE = '2024-01-01'
HISTORICAL_TIMESTAMP = '2024-01-01T00:00'

DAYS_LOOKUP = 8
ELEVATION_THRESHOLD = 50
AVERAGE_TEMP_THRESHOLD = 20

CITIES_DATA_TABLE_NAME = 'cities_data'
WEATHER_AGGREGATED_TABLE_NAME = 'weather_aggregated'
