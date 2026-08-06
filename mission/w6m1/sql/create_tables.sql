USE bike_db;

-- =========================================================
-- 1. Raw API data
-- =========================================================
CREATE TABLE IF NOT EXISTS bike_usage_raw (
    raw_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    dag_run_id VARCHAR(250) NOT NULL,
    source_date DATE NOT NULL,

    page_start_index INT UNSIGNED NOT NULL,
    page_end_index INT UNSIGNED NOT NULL,
    row_number_in_page INT UNSIGNED NULL,

    rent_dt VARCHAR(30) NULL,
    rent_id VARCHAR(100) NULL,
    rent_nm VARCHAR(255) NULL,
    rent_type VARCHAR(100) NULL,
    gender_cd VARCHAR(100) NULL,
    age_type VARCHAR(100) NULL,

    use_cnt VARCHAR(100) NULL,
    exer_amt VARCHAR(100) NULL,
    carbon_amt VARCHAR(100) NULL,
    move_meter VARCHAR(100) NULL,
    move_time VARCHAR(100) NULL,

    raw_record JSON NULL,
    collected_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    PRIMARY KEY (raw_id),

    KEY idx_raw_dag_run_id (dag_run_id),
    KEY idx_raw_source_date (source_date),
    KEY idx_raw_rent_dt_rent_id (rent_dt, rent_id),
    KEY idx_raw_page (
        source_date,
        page_start_index,
        page_end_index
    )
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;


-- =========================================================
-- 2. Cleaned Silver data
-- =========================================================
CREATE TABLE IF NOT EXISTS bike_usage_silver (
    silver_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    dag_run_id VARCHAR(250) NOT NULL,
    raw_id BIGINT UNSIGNED NULL,

    record_hash CHAR(64) NOT NULL,

    rent_dt DATE NOT NULL,
    rent_id VARCHAR(100) NOT NULL,
    rent_nm VARCHAR(255) NULL,
    rent_type VARCHAR(100) NULL,
    gender_cd VARCHAR(100) NULL,
    age_type VARCHAR(100) NULL,

    use_cnt BIGINT UNSIGNED NULL,
    exer_amt DECIMAL(20, 6) NULL,
    carbon_amt DECIMAL(20, 6) NULL,
    move_meter DECIMAL(20, 3) NULL,
    move_time DECIMAL(20, 3) NULL,

    cleaned_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    PRIMARY KEY (silver_id),

    UNIQUE KEY uk_silver_run_record_hash (
    dag_run_id,
    record_hash
    ),
    UNIQUE KEY uk_raw_run_page_row (
    dag_run_id,
    source_date,
    page_start_index,
    page_end_index,
    row_number_in_page
    ),
    UNIQUE KEY uk_reject_run_raw_error (
    dag_run_id,
    raw_id,
    error_column,
    error_reason
    ),

    KEY idx_silver_dag_run_id (dag_run_id),
    KEY idx_silver_rent_dt (rent_dt),
    KEY idx_silver_rent_id (rent_id),
    KEY idx_silver_rent_dt_rent_id (rent_dt, rent_id),

    CONSTRAINT fk_silver_raw
        FOREIGN KEY (raw_id)
        REFERENCES bike_usage_raw (raw_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;


-- =========================================================
-- 3. Rejected or partially invalid data
-- =========================================================
CREATE TABLE IF NOT EXISTS bike_usage_reject (
    reject_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    dag_run_id VARCHAR(250) NOT NULL,
    raw_id BIGINT UNSIGNED NULL,
    source_date DATE NULL,

    error_column VARCHAR(100) NULL,
    error_value TEXT NULL,
    error_reason VARCHAR(100) NOT NULL,
    error_message TEXT NULL,

    raw_record JSON NULL,
    rejected_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    PRIMARY KEY (reject_id),

    KEY idx_reject_dag_run_id (dag_run_id),
    KEY idx_reject_source_date (source_date),
    KEY idx_reject_reason (error_reason),
    KEY idx_reject_raw_id (raw_id),

    CONSTRAINT fk_reject_raw
        FOREIGN KEY (raw_id)
        REFERENCES bike_usage_raw (raw_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;


-- =========================================================
-- 4. Aggregated staging data
-- =========================================================
CREATE TABLE IF NOT EXISTS station_period_usage_staging (
    staging_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    dag_run_id VARCHAR(250) NOT NULL,

    period_start_date DATE NOT NULL,
    period_end_date DATE NOT NULL,

    station_id VARCHAR(100) NOT NULL,
    station_name VARCHAR(255) NULL,

    total_usage_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    total_distance_m DECIMAL(24, 3) NOT NULL DEFAULT 0,
    total_duration_min DECIMAL(24, 3) NOT NULL DEFAULT 0,

    usage_valid_row_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    distance_valid_row_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    duration_valid_row_count BIGINT UNSIGNED NOT NULL DEFAULT 0,

    staged_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    PRIMARY KEY (staging_id),

    UNIQUE KEY uk_staging_run_period_station (
        dag_run_id,
        period_start_date,
        period_end_date,
        station_id
    ),

    KEY idx_staging_dag_run_id (dag_run_id),
    KEY idx_staging_period (
        period_start_date,
        period_end_date
    ),
    KEY idx_staging_station_id (station_id),

    CONSTRAINT chk_staging_period
        CHECK (period_start_date <= period_end_date),

    CONSTRAINT chk_staging_nonnegative
        CHECK (
            total_usage_count >= 0
            AND total_distance_m >= 0
            AND total_duration_min >= 0
        )
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;


-- =========================================================
-- 5. Final Gold aggregation table
-- =========================================================
CREATE TABLE IF NOT EXISTS station_period_usage (
    period_start_date DATE NOT NULL,
    period_end_date DATE NOT NULL,

    station_id VARCHAR(100) NOT NULL,
    station_name VARCHAR(255) NULL,

    total_usage_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    total_distance_m DECIMAL(24, 3) NOT NULL DEFAULT 0,
    total_duration_min DECIMAL(24, 3) NOT NULL DEFAULT 0,

    usage_valid_row_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    distance_valid_row_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    duration_valid_row_count BIGINT UNSIGNED NOT NULL DEFAULT 0,

    source_dag_run_id VARCHAR(250) NOT NULL,

    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),

    PRIMARY KEY (
        period_start_date,
        period_end_date,
        station_id
    ),

    KEY idx_gold_station_id (station_id),
    KEY idx_gold_period (
        period_start_date,
        period_end_date
    ),
    KEY idx_gold_source_dag_run_id (source_dag_run_id),

    CONSTRAINT chk_gold_period
        CHECK (period_start_date <= period_end_date),

    CONSTRAINT chk_gold_nonnegative
        CHECK (
            total_usage_count >= 0
            AND total_distance_m >= 0
            AND total_duration_min >= 0
        )
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;


-- =========================================================
-- 6. Pipeline alert log table 
-- =========================================================
CREATE TABLE IF NOT EXISTS pipeline_alert_log (
    alert_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    dag_id VARCHAR(250) NOT NULL,
    dag_run_id VARCHAR(250) NOT NULL,
    task_id VARCHAR(250) NULL,
    alert_level VARCHAR(30) NOT NULL,
    error_type VARCHAR(100) NULL,
    error_message TEXT NOT NULL,
    target_dates JSON NULL,
    try_number INT UNSIGNED NULL,
    created_at DATETIME(6)
        NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (alert_id),
    KEY idx_alert_dag_run_id (dag_run_id),
    KEY idx_alert_task_id (task_id),
    KEY idx_alert_created_at (created_at)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;