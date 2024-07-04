"""Entrypoint for the project. Extracts data from open-meteo and loads to a MySql database."""
from weather_data_extraction import get_weather_data
from insert_data_rds import insert_df_to_db
from consts import TARGET_CITIES, CITIES_DATA_TABLE_NAME, WEATHER_AGGREGATED_TABLE_NAME

if __name__ == '__main__':
    cities_df, weather_df = get_weather_data(TARGET_CITIES)
    mapping = {
        CITIES_DATA_TABLE_NAME: cities_df,
        WEATHER_AGGREGATED_TABLE_NAME: weather_df,
    }

    for table, df in mapping.items():
        insert_df_to_db(df, table)
