CREATE DATABASE IF NOT EXISTS nyse;
USE nyse;

CREATE EXTERNAL TABLE IF NOT EXISTS prices_clean (
    `date` DATE,
    symbol STRING,
    `open` DOUBLE,
    `close` DOUBLE,
    low DOUBLE,
    high DOUBLE,
    volume DOUBLE,
    daily_return_pct DOUBLE
)
STORED AS PARQUET
LOCATION '/data/nyse/prices_clean';

-- Sqoop text output is not CSV-quoted. Parse the stable fields from both ends
-- so embedded commas in sub-industry and headquarters do not shift columns.
CREATE EXTERNAL TABLE IF NOT EXISTS securities_raw (
    symbol STRING,
    security STRING,
    sec_filings STRING,
    gics_sector STRING,
    gics_sub_industry STRING,
    headquarters_city STRING,
    headquarters_state STRING,
    date_first_added_raw STRING,
    cik BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    'input.regex'='^([^,]*),([^,]*),([^,]*),([^,]*),(.+),([^,]*), ([^,]*),(null|[0-9]{4}-[0-9]{2}-[0-9]{2}),([0-9]+)$'
)
STORED AS TEXTFILE
LOCATION '/data/nyse/securities_raw';

CREATE OR REPLACE VIEW securities AS
SELECT
    symbol,
    security,
    sec_filings,
    gics_sector,
    gics_sub_industry,
    CONCAT(headquarters_city, ', ', headquarters_state) AS headquarters,
    CASE
        WHEN date_first_added_raw = 'null' THEN CAST(NULL AS DATE)
        ELSE CAST(date_first_added_raw AS DATE)
    END AS date_first_added,
    cik
FROM securities_raw;

CREATE OR REPLACE VIEW prices_with_securities AS
SELECT
    p.`date`,
    p.symbol,
    s.security AS company,
    s.gics_sector AS sector,
    s.gics_sub_industry AS sub_industry,
    s.headquarters,
    p.`open`,
    p.`close`,
    p.low,
    p.high,
    p.volume,
    p.daily_return_pct
FROM prices_clean p
INNER JOIN securities s
    ON p.symbol = s.symbol;

CREATE OR REPLACE VIEW sector_monthly_avg AS
SELECT
    DATE_FORMAT(`date`, 'yyyy-MM') AS `month`,
    sector,
    COUNT(DISTINCT symbol) AS company_count,
    COUNT(*) AS trading_records,
    ROUND(AVG(`close`), 4) AS avg_close,
    ROUND(AVG(daily_return_pct), 4) AS avg_daily_return_pct,
    CAST(SUM(volume) AS BIGINT) AS total_volume
FROM prices_with_securities
GROUP BY DATE_FORMAT(`date`, 'yyyy-MM'), sector;

-- Hive 3.1.3 removed CREATE INDEX support. Parquet column pruning and
-- predicate pushdown are used instead of defining a deprecated Hive index.
