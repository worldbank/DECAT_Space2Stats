-- Space2Stats Development Database Initialization
DROP TABLE IF EXISTS space2stats CASCADE;

-- Create space2stats table with complete schema matching space2stats_sample_cs.parquet
CREATE TABLE IF NOT EXISTS space2stats (
    hex_id VARCHAR PRIMARY KEY,
    geometry VARCHAR,
    sum_built_area_m_2030 BIGINT,
    pop DOUBLE PRECISION,
    pop_flood DOUBLE PRECISION,
    pop_flood_pct DOUBLE PRECISION,
    sum_pop_f_0_2020 DOUBLE PRECISION,
    sum_pop_f_10_2020 DOUBLE PRECISION,
    sum_pop_f_15_2020 DOUBLE PRECISION,
    sum_pop_f_1_2020 DOUBLE PRECISION,
    sum_pop_f_20_2020 DOUBLE PRECISION,
    sum_pop_f_25_2020 DOUBLE PRECISION,
    sum_pop_f_30_2020 DOUBLE PRECISION,
    sum_pop_f_35_2020 DOUBLE PRECISION,
    sum_pop_f_40_2020 DOUBLE PRECISION,
    sum_pop_f_45_2020 DOUBLE PRECISION,
    sum_pop_f_50_2020 DOUBLE PRECISION,
    sum_pop_f_55_2020 DOUBLE PRECISION,
    sum_pop_f_5_2020 DOUBLE PRECISION,
    sum_pop_f_60_2020 DOUBLE PRECISION,
    sum_pop_f_65_2020 DOUBLE PRECISION,
    sum_pop_f_70_2020 DOUBLE PRECISION,
    sum_pop_f_75_2020 DOUBLE PRECISION,
    sum_pop_f_80_2020 DOUBLE PRECISION,
    sum_pop_m_0_2020 DOUBLE PRECISION,
    sum_pop_m_10_2020 DOUBLE PRECISION,
    sum_pop_m_15_2020 DOUBLE PRECISION,
    sum_pop_m_1_2020 DOUBLE PRECISION,
    sum_pop_m_20_2020 DOUBLE PRECISION,
    sum_pop_m_25_2020 DOUBLE PRECISION,
    sum_pop_m_30_2020 DOUBLE PRECISION,
    sum_pop_m_35_2020 DOUBLE PRECISION,
    sum_pop_m_40_2020 DOUBLE PRECISION,
    sum_pop_m_45_2020 DOUBLE PRECISION,
    sum_pop_m_50_2020 DOUBLE PRECISION,
    sum_pop_m_55_2020 DOUBLE PRECISION,
    sum_pop_m_5_2020 DOUBLE PRECISION,
    sum_pop_m_60_2020 DOUBLE PRECISION,
    sum_pop_m_65_2020 DOUBLE PRECISION,
    sum_pop_m_70_2020 DOUBLE PRECISION,
    sum_pop_m_75_2020 DOUBLE PRECISION,
    sum_pop_m_80_2020 DOUBLE PRECISION,
    sum_pop_f_2020 DOUBLE PRECISION,
    sum_pop_m_2020 DOUBLE PRECISION,
    sum_pop_2020 DOUBLE PRECISION,
    ghs_11_count BIGINT,
    ghs_12_count BIGINT,
    ghs_13_count BIGINT,
    ghs_21_count BIGINT,
    ghs_22_count BIGINT,
    ghs_23_count BIGINT,
    ghs_30_count BIGINT,
    ghs_total_count BIGINT,
    ghs_11_pop DOUBLE PRECISION,
    ghs_12_pop DOUBLE PRECISION,
    ghs_13_pop DOUBLE PRECISION,
    ghs_21_pop DOUBLE PRECISION,
    ghs_22_pop DOUBLE PRECISION,
    ghs_23_pop DOUBLE PRECISION,
    ghs_30_pop DOUBLE PRECISION,
    ghs_total_pop DOUBLE PRECISION,
    sum_viirs_ntl_2012 DOUBLE PRECISION,
    sum_viirs_ntl_2013 DOUBLE PRECISION,
    sum_viirs_ntl_2014 DOUBLE PRECISION,
    sum_viirs_ntl_2015 DOUBLE PRECISION,
    sum_viirs_ntl_2016 DOUBLE PRECISION,
    sum_viirs_ntl_2017 DOUBLE PRECISION,
    sum_viirs_ntl_2018 DOUBLE PRECISION,
    sum_viirs_ntl_2019 DOUBLE PRECISION,
    sum_viirs_ntl_2020 DOUBLE PRECISION,
    sum_viirs_ntl_2021 DOUBLE PRECISION,
    sum_viirs_ntl_2022 DOUBLE PRECISION,
    sum_viirs_ntl_2023 DOUBLE PRECISION,
    sum_viirs_ntl_2024 DOUBLE PRECISION,
    sum_built_area_m_1975 DOUBLE PRECISION,
    sum_built_area_m_1980 DOUBLE PRECISION,
    sum_built_area_m_1985 DOUBLE PRECISION,
    sum_built_area_m_1990 DOUBLE PRECISION,
    sum_built_area_m_1995 DOUBLE PRECISION,
    sum_built_area_m_2000 DOUBLE PRECISION,
    sum_built_area_m_2005 DOUBLE PRECISION,
    sum_built_area_m_2010 DOUBLE PRECISION,
    sum_built_area_m_2015 DOUBLE PRECISION,
    sum_built_area_m_2020 DOUBLE PRECISION,
    sum_built_area_m_2025 DOUBLE PRECISION
);

-- Create climate table
CREATE TABLE IF NOT EXISTS climate (
    hex_id TEXT,
    date DATE,
    spi FLOAT,
    PRIMARY KEY (hex_id, date)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_space2stats_hex_id ON space2stats (hex_id);
CREATE INDEX IF NOT EXISTS idx_climate_hex_id ON climate (hex_id);
CREATE INDEX IF NOT EXISTS idx_climate_date ON climate (date);

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS h3;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";



