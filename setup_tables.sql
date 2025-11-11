
-- Tabulka: csu_wages (průměrné mzdy podle krajů)
CREATE TABLE IF NOT EXISTS csu_wages (
    id BIGSERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    avg_wage DECIMAL(10, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(region)
);

-- Tabulka: csu_wages_by_sector (mzdy podle odvětví)
CREATE TABLE IF NOT EXISTS csu_wages_by_sector (
    id BIGSERIAL PRIMARY KEY,
    region TEXT,
    sector TEXT,
    avg_wage DECIMAL(10, 2),
    employees INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tabulka: csu_wages_timeseries (časové řady)
CREATE TABLE IF NOT EXISTS csu_wages_timeseries (
    id BIGSERIAL PRIMARY KEY,
    region TEXT,
    year INTEGER,
    quarter INTEGER,
    avg_wage DECIMAL(10, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tabulka: csu_wage_structure (struktura mezd)
CREATE TABLE IF NOT EXISTS csu_wage_structure (
    id BIGSERIAL PRIMARY KEY,
    region TEXT,
    category TEXT,
    value DECIMAL(10, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tabulka: job_listings (pracovní nabídky)
CREATE TABLE IF NOT EXISTS job_listings (
    id BIGSERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    salary_offer INTEGER NOT NULL,
    source TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tabulka: wages_by_source (agregace podle zdroje)
CREATE TABLE IF NOT EXISTS wages_by_source (
    id BIGSERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    source TEXT NOT NULL,
    avg_offer DECIMAL(10, 2),
    offers INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(region, source)
);

-- Tabulka: wages_comparison (výsledné metriky)
CREATE TABLE IF NOT EXISTS wages_comparison (
    id BIGSERIAL PRIMARY KEY,
    region TEXT NOT NULL UNIQUE,
    avg_wage DECIMAL(10, 2),
    avg_offer DECIMAL(10, 2),
    median_offer DECIMAL(10, 2),
    min_offer INTEGER,
    max_offer INTEGER,
    offers INTEGER,
    pay_gap DECIMAL(10, 2),
    pay_gap_pct DECIMAL(5, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexy pro rychlejší dotazy
CREATE INDEX IF NOT EXISTS idx_job_listings_region ON job_listings(region);
CREATE INDEX IF NOT EXISTS idx_job_listings_source ON job_listings(source);
CREATE INDEX IF NOT EXISTS idx_wages_comparison_region ON wages_comparison(region);
CREATE INDEX IF NOT EXISTS idx_wages_by_source_region_source ON wages_by_source(region, source);
