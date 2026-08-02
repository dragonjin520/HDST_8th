CREATE TABLE IF NOT EXISTS city_population_current (
    area_code VARCHAR(30) NOT NULL,
    forecast_hour SMALLINT NOT NULL,

    base_time TIMESTAMPTZ NOT NULL,
    target_time TIMESTAMPTZ NOT NULL,

    congestion_level VARCHAR(30),
    congestion_message TEXT,

    population_min INTEGER,
    population_max INTEGER,
    population_avg NUMERIC(12, 2),

    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        area_code,
        forecast_hour
    ),

    CONSTRAINT fk_city_population_current_area
        FOREIGN KEY (area_code)
        REFERENCES city_area(area_code),

    CONSTRAINT chk_city_population_current_hour
        CHECK (
            forecast_hour BETWEEN 0 AND 12
        ),

    CONSTRAINT chk_city_population_current_range
        CHECK (
            population_min IS NULL
            OR population_max IS NULL
            OR population_min <= population_max
        ),

    CONSTRAINT chk_city_population_current_nonnegative
        CHECK (
            (population_min IS NULL OR population_min >= 0)
            AND
            (population_max IS NULL OR population_max >= 0)
        ),

    CONSTRAINT chk_city_population_current_time
        CHECK (
            target_time >= base_time
        )
);

COMMENT ON TABLE city_population_current
IS '121개 장소별 최신 현재 및 1~12시간 후 인구 데이터';

COMMENT ON COLUMN city_population_current.forecast_hour
IS '0은 현재 인구, 1~12는 미래 예측 시간';

COMMENT ON COLUMN city_population_current.base_time
IS '실시간 인구 기준 시각';

COMMENT ON COLUMN city_population_current.target_time
IS '현재 또는 미래 예측 대상 시각';