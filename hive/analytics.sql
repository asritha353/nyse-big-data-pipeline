USE nyse;

-- Average price and return by sector.
SELECT
    sector,
    COUNT(DISTINCT symbol) AS company_count,
    ROUND(AVG(`close`), 4) AS avg_close,
    ROUND(AVG(daily_return_pct), 4) AS avg_daily_return_pct
FROM prices_with_securities
GROUP BY sector
ORDER BY avg_daily_return_pct DESC;

-- Highest-volume companies.
SELECT
    symbol,
    company,
    sector,
    CAST(SUM(volume) AS BIGINT) AS total_volume
FROM prices_with_securities
GROUP BY symbol, company, sector
ORDER BY total_volume DESC
LIMIT 10;

-- Highest-volume sectors.
SELECT
    sector,
    CAST(SUM(volume) AS BIGINT) AS total_volume
FROM prices_with_securities
GROUP BY sector
ORDER BY total_volume DESC;

-- Recent monthly sector results from the reusable analytical view.
SELECT
    `month`,
    sector,
    company_count,
    trading_records,
    avg_close,
    avg_daily_return_pct,
    total_volume
FROM sector_monthly_avg
ORDER BY `month` DESC, sector
LIMIT 25;
