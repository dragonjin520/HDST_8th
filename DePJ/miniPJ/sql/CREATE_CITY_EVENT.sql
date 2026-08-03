CREATE TABLE IF NOT EXISTS city_event_current (
    area_code VARCHAR(30) NOT NULL,

    event_name TEXT NOT NULL,
    event_period TEXT,
    event_start_time TIMESTAMPTZ,
    event_end_time TIMESTAMPTZ,

    event_place TEXT,

    event_longitude NUMERIC(10, 7),
    event_latitude NUMERIC(10, 7),

    pay_yn CHAR(1),

    thumbnail_url TEXT,
    detail_url TEXT,
    event_etc_detail TEXT,

    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        area_code,
        event_name,
        event_period
    ),

    CONSTRAINT fk_city_event_current_area
        FOREIGN KEY (area_code)
        REFERENCES city_area(area_code),

    CONSTRAINT chk_city_event_current_pay_yn
        CHECK (
            pay_yn IS NULL
            OR pay_yn IN ('Y', 'N')
        ),

    CONSTRAINT chk_city_event_current_time
        CHECK (
            event_start_time IS NULL
            OR event_end_time IS NULL
            OR event_start_time <= event_end_time
        )
);