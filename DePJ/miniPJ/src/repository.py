from __future__ import annotations

from datetime import datetime

import psycopg

from src.config import get_settings
from src.parser import CityPopulationData


def get_connection() -> psycopg.Connection:
    settings = get_settings()

    return psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )


def upsert_area(
    cursor: psycopg.Cursor,
    area_code: str,
    area_name: str,
) -> None:
    cursor.execute(
        """
        INSERT INTO city_area (
            area_code,
            area_name
        )
        VALUES (%s, %s)
        ON CONFLICT (area_code)
        DO UPDATE SET
            area_name = EXCLUDED.area_name,
            is_active = TRUE,
            updated_at = NOW()
        """,
        (area_code, area_name),
    )


def upsert_current_realtime(
    cursor: psycopg.Cursor,
    data: CityPopulationData,
    collected_at: datetime,
) -> None:
    realtime = data.realtime

    cursor.execute(
        """
        INSERT INTO city_population_current (
            area_code,
            forecast_hour,
            base_time,
            target_time,
            congestion_level,
            congestion_message,
            population_min,
            population_max,
            population_avg,
            collected_at
        )
        VALUES (
            %s, 0, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (area_code, forecast_hour)
        DO UPDATE SET
            base_time = EXCLUDED.base_time,
            target_time = EXCLUDED.target_time,
            congestion_level = EXCLUDED.congestion_level,
            congestion_message = EXCLUDED.congestion_message,
            population_min = EXCLUDED.population_min,
            population_max = EXCLUDED.population_max,
            population_avg = EXCLUDED.population_avg,
            collected_at = EXCLUDED.collected_at,
            loaded_at = NOW()
        """,
        (
            realtime.area_code,
            realtime.population_time,
            realtime.population_time,
            realtime.congestion_level,
            realtime.congestion_message,
            realtime.population_min,
            realtime.population_max,
            realtime.population_avg,
            collected_at,
        ),
    )


def upsert_current_forecasts(
    cursor: psycopg.Cursor,
    data: CityPopulationData,
    collected_at: datetime,
) -> None:
    for forecast in data.forecasts:
        cursor.execute(
            """
            INSERT INTO city_population_current (
                area_code,
                forecast_hour,
                base_time,
                target_time,
                congestion_level,
                congestion_message,
                population_min,
                population_max,
                population_avg,
                collected_at
            )
            VALUES (
                %s, %s, %s, %s, %s, NULL,
                %s, %s, %s, %s
            )
            ON CONFLICT (area_code, forecast_hour)
            DO UPDATE SET
                base_time = EXCLUDED.base_time,
                target_time = EXCLUDED.target_time,
                congestion_level = EXCLUDED.congestion_level,
                congestion_message = NULL,
                population_min = EXCLUDED.population_min,
                population_max = EXCLUDED.population_max,
                population_avg = EXCLUDED.population_avg,
                collected_at = EXCLUDED.collected_at,
                loaded_at = NOW()
            """,
            (
                forecast.area_code,
                forecast.forecast_hour,
                forecast.base_time,
                forecast.target_time,
                forecast.congestion_level,
                forecast.population_min,
                forecast.population_max,
                forecast.population_avg,
                collected_at,
            ),
        )


def upsert_history(
    cursor: psycopg.Cursor,
    data: CityPopulationData,
    collected_at: datetime,
) -> None:
    realtime = data.realtime

    cursor.execute(
        """
        INSERT INTO city_population_history (
            area_code,
            population_time,
            congestion_level,
            congestion_message,
            population_min,
            population_max,
            population_avg,
            male_population_rate,
            female_population_rate,
            population_rate_0,
            population_rate_10,
            population_rate_20,
            population_rate_30,
            population_rate_40,
            population_rate_50,
            population_rate_60,
            population_rate_70,
            resident_rate,
            nonresident_rate,
            replace_yn,
            forecast_yn,
            collected_at
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s
        )
        ON CONFLICT (area_code, population_time)
        DO UPDATE SET
            congestion_level = EXCLUDED.congestion_level,
            congestion_message = EXCLUDED.congestion_message,
            population_min = EXCLUDED.population_min,
            population_max = EXCLUDED.population_max,
            population_avg = EXCLUDED.population_avg,
            male_population_rate = EXCLUDED.male_population_rate,
            female_population_rate = EXCLUDED.female_population_rate,
            population_rate_0 = EXCLUDED.population_rate_0,
            population_rate_10 = EXCLUDED.population_rate_10,
            population_rate_20 = EXCLUDED.population_rate_20,
            population_rate_30 = EXCLUDED.population_rate_30,
            population_rate_40 = EXCLUDED.population_rate_40,
            population_rate_50 = EXCLUDED.population_rate_50,
            population_rate_60 = EXCLUDED.population_rate_60,
            population_rate_70 = EXCLUDED.population_rate_70,
            resident_rate = EXCLUDED.resident_rate,
            nonresident_rate = EXCLUDED.nonresident_rate,
            replace_yn = EXCLUDED.replace_yn,
            forecast_yn = EXCLUDED.forecast_yn,
            collected_at = EXCLUDED.collected_at,
            loaded_at = NOW()
        """,
        (
            realtime.area_code,
            realtime.population_time,
            realtime.congestion_level,
            realtime.congestion_message,
            realtime.population_min,
            realtime.population_max,
            realtime.population_avg,
            realtime.male_population_rate,
            realtime.female_population_rate,
            realtime.population_rate_0,
            realtime.population_rate_10,
            realtime.population_rate_20,
            realtime.population_rate_30,
            realtime.population_rate_40,
            realtime.population_rate_50,
            realtime.population_rate_60,
            realtime.population_rate_70,
            realtime.resident_rate,
            realtime.nonresident_rate,
            realtime.replace_yn,
            realtime.forecast_yn,
            collected_at,
        ),
    )


def save_city_population(data: CityPopulationData) -> None:
    collected_at = datetime.now().astimezone()

    with get_connection() as conn:
        with conn.cursor() as cursor:
            upsert_area(
                cursor,
                data.realtime.area_code,
                data.realtime.area_name,
            )

            upsert_current_realtime(
                cursor,
                data,
                collected_at,
            )

            upsert_current_forecasts(
                cursor,
                data,
                collected_at,
            )

            upsert_history(
                cursor,
                data,
                collected_at,
            )

        conn.commit()