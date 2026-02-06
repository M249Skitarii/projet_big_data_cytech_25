DROP TABLE IF EXISTS fact_trips;
DROP TABLE IF EXISTS dim_zones;
DROP TABLE IF EXISTS dim_boroughs;
DROP TABLE IF EXISTS dim_payment_types;
DROP TABLE IF EXISTS dim_rate_codes;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_time;

-- ==========================================================
-- 1. DIMENSIONS GÉOGRAPHIQUES (FLOCON)
-- ==========================================================
CREATE TABLE dim_zones (
    location_id INT PRIMARY KEY,
    zone_name VARCHAR(255),
    borough_name VARCHAR(25)
);

-- ==========================================================
-- 2. DIMENSIONS TEMPORELLES (SÉPARÉES)
-- ==========================================================
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY, -- Format: 20251101
    full_date DATE NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE dim_time (
    time_key INT PRIMARY KEY, -- Format: 1430 (pour 14h30)
    full_time TIME NOT NULL,
    hour INT NOT NULL,
    minute INT NOT NULL,
    time_of_day VARCHAR(20) -- Matin, Après-midi, Soir, Nuit
);

-- ==========================================================
-- 3. AUTRES DIMENSIONS (RÉFÉRENCES MÉTIER)
-- ==========================================================
CREATE TABLE dim_payment_types (
    payment_type_id INT PRIMARY KEY,
    payment_name VARCHAR(50) NOT NULL
);

CREATE TABLE dim_rate_codes (
    rate_code_id INT PRIMARY KEY,
    rate_description VARCHAR(100)
);

-- ==========================================================
-- 4. TABLE DE FAITS (FACT_TRIPS)
-- ==========================================================
CREATE TABLE fact_trips (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INT, -- 1= Creative Mobile, 2= VeriFone [cite: 5]
    
    -- Clés temporelles (Pickup)
    pickup_date_key INT REFERENCES dim_date(date_key),
    pickup_time_key INT REFERENCES dim_time(time_key),
    
    -- Clés temporelles (Dropoff)
    dropoff_date_key INT REFERENCES dim_date(date_key),
    dropoff_time_key INT REFERENCES dim_time(time_key),
    
    -- Clés géographiques (Coordonnées PULocationID/DOLocationID)
    pickup_location_id INT REFERENCES dim_zones(location_id),
    dropoff_location_id INT REFERENCES dim_zones(location_id),
    
    -- Références
    rate_code_id INT REFERENCES dim_rate_codes(rate_code_id),
    payment_type_id INT REFERENCES dim_payment_types(payment_type_id),
    
    -- Mesures (Métrique de la course)
    passenger_count INT,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION, 
    airport_fee DOUBLE PRECISION,          
    cbd_congestion_fee DOUBLE PRECISION
);