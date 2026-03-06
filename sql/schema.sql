DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS stations;
DROP TABLE IF EXISTS rideable;
DROP TABLE IF EXISTS members;

CREATE TABLE members (
    member_casual VARCHAR(20),
    member_type_id INTEGER
);

CREATE TABLE rideable(
    rideable_type VARCHAR(20),
    ride_type_id INTEGER
);

CREATE TABLE stations(
    station_id VARCHAR(20),
    station_name VARCHAR(100),
    lat NUMERIC,
    lng NUMERIC
);

CREATE TABLE trips(
    ride_id VARCHAR(50),
    rideable_type_id INTEGER,
    member_type_id INTEGER,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    start_station_id VARCHAR(20),
    end_station_id VARCHAR(20)
);