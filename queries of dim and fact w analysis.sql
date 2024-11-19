CREATE TABLE dim_date (
    date_id INT IDENTITY(1,1) PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    dow INT NOT NULL
);
CREATE TABLE dim_time (
    time_id INT IDENTITY(1,1) PRIMARY KEY,
    hour INT NOT NULL UNIQUE
);
CREATE TABLE dim_weather_station (
    station_id INT IDENTITY(1,1) PRIMARY KEY,
    station_name VARCHAR(255) NOT NULL,
    cimis_region VARCHAR(255) NOT NULL,
    UNIQUE(station_name, cimis_region)
);
CREATE TABLE dim_location (
    location_id INT IDENTITY(1,1) PRIMARY KEY,
    county VARCHAR(255) NOT NULL,
    city VARCHAR(255) NOT NULL,
    UNIQUE(county, city)
);
CREATE TABLE dim_road (
    road_id VARCHAR(100) PRIMARY KEY,
    primary_rd VARCHAR(255) NOT NULL,
    secondary_rd VARCHAR(255)
);

--date dimension-------------------
INSERT INTO dim_date (date, year, month, day, dow)
SELECT DISTINCT
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(DOW FROM date) AS dow
FROM staging_table;
--time dim----------------
INSERT INTO dim_time (hour)
SELECT DISTINCT
    hour
FROM staging_table;

--dim_weather_station---------------
INSERT INTO dim_weather_station (station_name, cimis_region)
SELECT DISTINCT
    station_name,
    cimis_region
FROM stagging_table;

--location dim----------------
INSERT INTO dim_location (county, city)
SELECT DISTINCT
    county,
    city
FROM staging_table;


--dim road-------------------
INSERT INTO dim_road (road_id, primary_rd, secondary_rd)
SELECT DISTINCT
    primary_rd || '_' || CAST(ROW_NUMBER() OVER (PARTITION BY primary_rd ORDER BY case_id) AS VARCHAR) AS road_id,
    primary_rd,
    secondary_rd
FROM 
    staging_table;
--fact table create---------------
CREATE TABLE fact_traffic_analysis (
    fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_id INT NOT NULL,
    time_id INT NOT NULL,
    location_id INT NOT NULL,
    weather_station_id INT NOT NULL,
    road_id VARCHAR(100) NOT NULL,
    precip_in DECIMAL(10, 2),
    solar_rad DECIMAL(10, 2),
    vapour_pressure_mbars DECIMAL(10, 2),
    air_temp_f DECIMAL(10, 2),
    rel_humidity_percent DECIMAL(5, 2),
    dew_point_f DECIMAL(10, 2),
    wind_speed_mph DECIMAL(10, 2),
    wind_dir_degree INT,
    soil_temp_f DECIMAL(10, 2),
    vehicles BIGINT,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (weather_station_id) REFERENCES dim_weather_station(station_id),
    FOREIGN KEY (road_id) REFERENCES dim_road(road_id)
);
-- insert in fact-----------------------
INSERT INTO fact_traffic_analysis (
    date_id,
    time_id,
    location_id,
    weather_station_id,
    road_id,
    precip_in,
    solar_rad,
    vapour_pressure_mbars,
    air_temp_f,
    rel_humidity_percent,
    dew_point_f,
    wind_speed_mph,
    wind_dir_degree,
    soil_temp_f,
    vehicles
)
SELECT
    d.date_id,
    t.time_id,
    l.location_id,
    w.station_id,
   s.primary_rd || '_' || COALESCE(s.secondary_rd, 'NULL') || '_' || 
    CAST(ROW_NUMBER() OVER (PARTITION BY s.primary_rd, s.secondary_rd ORDER BY s.case_id) AS VARCHAR) AS road_id,
    s.precip_in,
    s.solar_rad_ly_day,
    s.vapor_pressure_mbars,
    s.air_temp_f,
    s.rel_humidity_percent,
    s.dew_point_f,
    s.wind_speed_mph,
    s.wind_dir_degrees,
    s.soil_temp_f,
    s.value AS vehicles
FROM staging_table s
JOIN dim_date d ON s.date = d.date
JOIN dim_time t ON s.hour = t.hour
JOIN dim_location l ON s.county = l.county AND s.city = l.city
JOIN dim_weather_station w ON s.station_name = w.station_name AND s.cimis_region = w.cimis_region;


--------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- aggregation queries

--Volume of traffic based on month and year
SELECT d.month,
    d.year,
    SUM(traffic.total_vehicles) AS total_traffic
FROM (
		SELECT DISTINCT f.date_id,
        t.hour,
        f.vehicles AS total_vehicles
    FROM fact_traffic_analysis f
        JOIN dim_time t ON f.time_id = t.time_id
	) traffic
    JOIN dim_date d ON traffic.date_id = d.date_id
GROUP BY d.month,
	d.year
ORDER BY d.month,
	d.year DESC
LIMIT 36;
----------------------------------------------------------------
--Volume of traffic based on month only
SELECT 
    d
.month, 
    SUM
(traffic.vehicles) AS total_traffic
FROM
(
        SELECT DISTINCT
    f.date_id,
    t.hour,
    f.vehicles AS vehicles
FROM
    fact_traffic_analysis f
    JOIN
    dim_time t ON f.time_id = t.time_id
    )
traffic
JOIN 
    dim_date d ON traffic.date_id = d.date_id
GROUP BY 
    d.month
ORDER BY 
    d.month ASC
LIMIT 12;

----------------------------------------------------------------
--Volume of traffic based on year only
SELECT
    d.year,
    SUM(traffic.vehicles) AS total_traffic
FROM
    (
        SELECT DISTINCT
        f.date_id,
        t.hour,
        f.vehicles AS vehicles
    FROM
        fact_traffic_analysis f
        JOIN
        dim_time t ON f.time_id = t.time_id
    ) traffic
    JOIN
    dim_date d ON traffic.date_id = d.date_id
GROUP BY 
    d.year
ORDER BY 
    total_traffic DESC
LIMIT 3;

----------------------------------------------------------------
--Volume of traffic based on DOW only
SELECT 
    d
.dow, 
    SUM
(traffic.vehicles) AS total_traffic
FROM
(
        SELECT DISTINCT
    f.date_id,
    t.hour,
    f.vehicles AS vehicles
FROM
    fact_traffic_analysis f
    JOIN
    dim_time t ON f.time_id = t.time_id
    )
traffic
JOIN 
    dim_date d ON traffic.date_id = d.date_id
GROUP BY 
    d.dow
ORDER BY 
    d.dow ASC
LIMIT 7;

----------------------------------------------------------------
--Volume of average traffic based on temperature (affect of temperature on traffic)
WITH
    temp_counts
    AS
    (
        SELECT
            ROUND(f.air_temp_f) AS rounded_temp,
            COUNT(DISTINCT f.date_id) AS days
        FROM
            fact_traffic_analysis f
        GROUP BY 
        ROUND(f.air_temp_f)
    ),
    traffic_data
    AS
    (
        SELECT
            ROUND(f.air_temp_f) AS rounded_temp,
            SUM(f.vehicles) AS total_traffic
        FROM
            fact_traffic_analysis f
        GROUP BY 
        ROUND(f.air_temp_f)
    )
SELECT
    t.rounded_temp,
    t.total_traffic::float / c.days AS avg_traffic_per_day
FROM
    traffic_data t
    JOIN
    temp_counts c
    ON 
    t.rounded_temp = c.rounded_temp
ORDER BY 
    rounded_temp DESC
LIMIT 100;
----------------------------------------------------------------
--Volume of accidents on rainy days based on hour in the day (affect of precipitation on hourly accident rate over 3 years)
SELECT 
    dt
.hour AS accident_hour,
    SUM
(CASE 
            WHEN fact.precip_in > 0.1 THEN 1 
            ELSE 0
END) AS accidents_during_rain,
    AVG
(fact.air_temp_f) AS avg_air_temperature,
    AVG
(fact.wind_speed_mph) AS avg_wind_speed
FROM 
    fact_traffic_analysis fact
JOIN 
    dim_time dt
    ON fact.time_id = dt.time_id
GROUP BY 
    dt.hour
ORDER BY 
    dt.hour;

----------------------------------------------------------------
--# of accidents across months and years
SELECT
    d.year,
    d.month,
    COUNT(r.primary_rd) AS distinct_roads,
    SUM(f.vehicles) AS total_traffic
FROM
    fact_traffic_analysis f
    JOIN
    dim_date d ON f.date_id = d.date_id
    JOIN
    dim_road r ON f.road_id = r.road_id
WHERE 
    r.primary_rd != '0000'
GROUP BY 
    d.year, 
    d.month
ORDER BY 
    d.year ASC, 
    d.month ASC
LIMIT 36;