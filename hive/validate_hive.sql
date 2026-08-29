USE nyse;

SELECT
    'prices_clean' AS check_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT symbol) AS symbol_count,
    MIN(`date`) AS min_date,
    MAX(`date`) AS max_date
FROM prices_clean;

SELECT
    'securities_raw' AS check_name,
    COUNT(*) AS row_count,
    COUNT(symbol) AS parsed_row_count,
    COUNT(DISTINCT symbol) AS symbol_count
FROM securities_raw;

SELECT
    'prices_with_securities' AS check_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT symbol) AS symbol_count
FROM prices_with_securities;

SELECT
    'unmatched_prices' AS check_name,
    COUNT(*) AS row_count
FROM prices_clean p
LEFT JOIN securities s
    ON p.symbol = s.symbol
WHERE s.symbol IS NULL;

SELECT
    'sector_monthly_avg' AS check_name,
    COUNT(*) AS row_count
FROM sector_monthly_avg;

SELECT
    symbol,
    gics_sub_industry,
    headquarters
FROM securities
ORDER BY symbol;
