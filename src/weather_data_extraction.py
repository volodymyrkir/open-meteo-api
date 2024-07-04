"""This module is used to extract and transform open-meteo data."""
import logging

import requests
from geopy.geocoders import Nominatim
import pandas as pd

from src.consts import (
    TARGET_CITIES, BASE_WEATHER_URL, HOURLY_FIELDS, GEO_FIELDS,
    HISTORICAL_WEATHER_URL, HISTORICAL_FIELDS, HISTORICAL_HOURLY_FIELDS,
    HISTORICAL_DATE, HISTORICAL_TIMESTAMP,
    DAYS_LOOKUP, ELEVATION_THRESHOLD, AVERAGE_TEMP_THRESHOLD
)

logging.basicConfig(format='%(message)s', level=logging.INFO)


def create_coordinates_dict(target_cities: list[str]) -> tuple[list[float], list[float]]:
    """
    Based on the cities provided, finds their coordinates.

    Args:
        target_cities (list[str]): list of the target cities:

    Returns:
        tuple[list[float], list[float]]: list of latitudes and list of longitudes.
    """
    lats, lots = [], []
    geolocator = Nominatim(user_agent='CoordinatesForCities')

    for city in target_cities:
        location = geolocator.geocode(city)
        logging.info(f"Adding city - {city}, coordinates: "
                     f"{location.latitude}, {location.longitude}")
        lats.append(location.latitude)
        lots.append(location.longitude)

    return lats, lots


def get_raw_weather_df(
        url: str,
        params: dict,
        required_fields: list[str],
        inner_fields: list[str]
) -> pd.DataFrame:
    """
    Based on the url, params and required fields, gets the raw weather data.
        The response is read in with Pandas. Only necessary columns are left,
        lists of inner fields are exploded for further aggregations.

    Args:
        url (str): the url of the API.
        params (dict): the params to pass to the API.
        required_fields (list[str]): list of the required fields to be selected from a response.
        inner_fields (list[str]): list of the inner fields:

    Returns:

    """
    response = requests.get(url, params=params)
    if response.ok:
        json_response = response.json()
        weather_df = pd.json_normalize(json_response)
        weather_df = weather_df.loc[:, required_fields]
        weather_df.columns = weather_df.columns.str.replace(r'.*\.', '', regex=True)
        weather_df[['latitude', 'longitude']] = weather_df[['latitude', 'longitude']].astype(str)
        return weather_df.explode(inner_fields).reset_index(drop=True)
    else:
        raise RuntimeError(f"Request failed with status code {response.text}")


def load_aggregate_weather_data(cities: list[str]) -> pd.DataFrame:
    """
    Loads and aggregates the main weather data.
        1. Loads data based on the input cities and amount of days to lookup.
        2. Smaller dataframe is created by only including previous week data.
        3. Calculates mean temperature for smaller df,
            filters out cities that have average temperature > 20.
        4. Joins smaller df with the original to avoid calculating
            average temperature for the next week.
        5. Calculates average temperature for the next week.
        6. Returns aggregated dataframe.

    Args:
        cities (list[str]): list of the target cities:

    Returns:
        pd.DataFrame: weather data with average temperature.
    """
    latitudes, longitudes = create_coordinates_dict(cities)
    main_request_params = {
        'latitude': ','.join(map(str, latitudes)),
        'longitude': ','.join(map(str, longitudes)),
        'past_days': DAYS_LOOKUP,
        'forecast_days': DAYS_LOOKUP,
        'hourly': HOURLY_FIELDS,
        'timezone': 'auto',
    }

    weather_df = get_raw_weather_df(
        BASE_WEATHER_URL, main_request_params,
        GEO_FIELDS, ['time', 'temperature_2m']
    )

    weather_df['time'] = pd.to_datetime(weather_df['time'])
    today_midnight = pd.Timestamp(pd.to_datetime('today').normalize())
    grouping_cols = ['latitude', 'longitude', 'elevation']

    last_week_df = weather_df.loc[weather_df['time'] < today_midnight]
    avg_temps_last_week = (
        last_week_df.groupby(grouping_cols, as_index=False).agg({'temperature_2m': 'mean'})
        .rename(columns={'temperature_2m': 'avg_temp_last_week'})
    )
    filtered_temps = avg_temps_last_week.loc[
        (avg_temps_last_week['avg_temp_last_week'] < AVERAGE_TEMP_THRESHOLD)
        & (avg_temps_last_week['elevation'] > ELEVATION_THRESHOLD)
        ]

    filtered_weather_df = pd.merge(weather_df, filtered_temps, how="inner", on=grouping_cols)
    aggregated_df = filtered_weather_df.loc[
        filtered_weather_df['time'] >= today_midnight
        ].drop(['time'], axis=1)

    return (
        aggregated_df
        .groupby([col for col in aggregated_df.columns if col != 'temperature_2m'], as_index=False)
        .agg({'temperature_2m': 'mean'})
        .rename(columns={'temperature_2m': 'avg_temp_next_week',
                         'timezone_abbreviation': 'timezone'})
    )


def append_historical_data(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """
    Appends historical data loaded for 2024-01-01 to the main data.
        Extracts temp, wind and humidity fields and merges.

    Args:
        aggregated_df (pd.DataFrame): Final aggregated weather data .

    Returns:
        pd.DataFrame: Final aggregated weather data with all required fields.
    """
    historical_request_params = {
        'latitude': ','.join(aggregated_df['latitude'].values.tolist()),
        'longitude': ','.join(aggregated_df['longitude'].values.tolist()),
        'start_date': HISTORICAL_DATE,
        'end_date': HISTORICAL_DATE,
        'start_hour': HISTORICAL_TIMESTAMP,
        'end_hour': HISTORICAL_TIMESTAMP,
        'hourly': ','.join(HISTORICAL_HOURLY_FIELDS),
    }
    historical_hourly_fields_mapping = {
        'temperature_2m': 'year_start_temp',
        'relative_humidity_2m': 'year_start_humidity',
        'wind_speed_10m': 'year_start_wind_speed',
    }
    raw_historical_weather_df = get_raw_weather_df(
        HISTORICAL_WEATHER_URL, historical_request_params,
        HISTORICAL_FIELDS, HISTORICAL_HOURLY_FIELDS
    )
    historical_weather_df = (
        raw_historical_weather_df
        .rename(historical_hourly_fields_mapping, axis=1)
        .loc[:, ['latitude', 'longitude'] + list(historical_hourly_fields_mapping.values())]
    )
    return pd.merge(
        aggregated_df, historical_weather_df,
        how="inner", on=['latitude', 'longitude']
    )


def get_weather_data(cities: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Entrypoint function of the module.
        Collects aggregated weather data and appends historical data.
        Splits dataframe according to the destination tables schema.

    Args:
        cities (str): list of cities to be provided to the load_aggregate_weather_data.

    Returns:
        pd.DataFrame: Final dataframe of cities data and aggregated weather data.
    """
    aggregated_df = load_aggregate_weather_data(cities)
    final_df = append_historical_data(aggregated_df)

    cities_data_df = final_df[['latitude', 'longitude', 'elevation', 'timezone']]
    aggregated_insights_df = final_df[
        [
            'latitude', 'longitude', 'avg_temp_last_week', 'avg_temp_next_week',
            'year_start_temp', 'year_start_humidity', 'year_start_wind_speed'
        ]
    ]
    return cities_data_df, aggregated_insights_df


if __name__ == '__main__':
    result = get_weather_data(TARGET_CITIES)
    print(result)
