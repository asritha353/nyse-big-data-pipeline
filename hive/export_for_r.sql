USE nyse;

INSERT OVERWRITE DIRECTORY '/data/nyse/r_export'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
    CAST(`date` AS STRING),
    symbol,
    company,
    sector,
    CAST(`close` AS STRING),
    CAST(daily_return_pct AS STRING),
    CAST(volume AS STRING)
FROM prices_with_securities
ORDER BY `date`, symbol;
