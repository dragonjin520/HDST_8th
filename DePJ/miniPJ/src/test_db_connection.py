import psycopg

from src.config import get_settings


def main() -> None:
    settings = get_settings()

    connection_info = {
        "host": settings.db_host,
        "port": settings.db_port,
        "dbname": settings.db_name,
        "user": settings.db_user,
        "password": settings.db_password,
    }

    try:
        with psycopg.connect(**connection_info) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        current_database(),
                        current_user,
                        NOW()
                    """
                )

                database_name, user_name, current_time = cursor.fetchone()

                print("PostgreSQL 연결 성공")
                print(f"Database : {database_name}")
                print(f"User     : {user_name}")
                print(f"Time     : {current_time}")

    except psycopg.Error as exc:
        print("PostgreSQL 연결 실패")
        print(exc)
        raise


if __name__ == "__main__":
    main()