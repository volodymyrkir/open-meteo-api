CREATE TABLE weather_aggregated(
    latitude VARCHAR(40) NOT NULL,
    longitude VARCHAR(40) NOT NULL,
    avg_temp_last_week DOUBLE NOT NULL,
    avg_temp_next_week DOUBLE NULL,
    year_start_temp FLOAT NULL,
    year_start_humidity INT NULL,
    year_start_wind_speed FLOAT NULL,
    PRIMARY KEY(latitude, longitude),
    FOREIGN KEY (latitude, longitude) REFERENCES cities_data(latitude, longitude)
);
