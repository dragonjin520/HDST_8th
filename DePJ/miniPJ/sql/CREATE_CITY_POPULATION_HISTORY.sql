CREATE TABLE IF NOT EXISTS city_population_history (
    area_code VARCHAR(30) NOT NULL,
    population_time TIMESTAMPTZ NOT NULL,

    congestion_level VARCHAR(30),
    congestion_message TEXT,

    population_min INTEGER,
    population_max INTEGER,
    population_avg NUMERIC(12, 2),

    male_population_rate NUMERIC(6, 3),
    female_population_rate NUMERIC(6, 3),

    population_rate_0 NUMERIC(6, 3),
    population_rate_10 NUMERIC(6, 3),
    population_rate_20 NUMERIC(6, 3),
    population_rate_30 NUMERIC(6, 3),
    population_rate_40 NUMERIC(6, 3),
    population_rate_50 NUMERIC(6, 3),
    population_rate_60 NUMERIC(6, 3),
    population_rate_70 NUMERIC(6, 3),

    resident_rate NUMERIC(6, 3),
    nonresident_rate NUMERIC(6, 3),

    replace_yn CHAR(1),
    forecast_yn CHAR(1),

    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        area_code,
        population_time
    ),

    CONSTRAINT fk_city_population_history_area
        FOREIGN KEY (area_code)
        REFERENCES city_area(area_code),

    CONSTRAINT chk_city_population_history_range
        CHECK (
            population_min IS NULL
            OR population_max IS NULL
            OR population_min <= population_max
        ),

    CONSTRAINT chk_city_population_history_nonnegative
        CHECK (
            (population_min IS NULL OR population_min >= 0)
            AND
            (population_max IS NULL OR population_max >= 0)
        )
);

COMMENT ON TABLE city_population_history
IS '모델 학습용 장소별 실제 관측 인구 이력';

COMMENT ON COLUMN city_population_history.population_time
IS 'API가 제공한 실제 인구 기준 시각';

COMMENT ON COLUMN city_population_history.collected_at
IS 'API를 호출한 시각';

COMMENT ON COLUMN city_population_history.loaded_at
IS 'PostgreSQL에 적재한 시각';