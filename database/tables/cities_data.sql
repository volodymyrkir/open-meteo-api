CREATE TABLE cities_data(
    latitude VARCHAR(40) NOT NULL,
    longitude VARCHAR(40) NOT NULL,
    elevation FLOAT NOT NULL,
    timezone VARCHAR(10) NULL,
    PRIMARY KEY(latitude, longitude)
);
