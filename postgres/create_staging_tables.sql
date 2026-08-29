CREATE TABLE IF NOT EXISTS staging_prices (
    date DATE,
    symbol VARCHAR(10),
    open DOUBLE PRECISION,
    close DOUBLE PRECISION,
    low DOUBLE PRECISION,
    high DOUBLE PRECISION,
    volume DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS staging_securities (
    symbol VARCHAR(10),
    security TEXT,
    sec_filings TEXT,
    gics_sector TEXT,
    gics_sub_industry TEXT,
    headquarters TEXT,
    date_first_added DATE,
    cik BIGINT
);
