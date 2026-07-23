from pathlib import Path

from pyspark.sql import DataFrame


def save_dataframe(
    df: DataFrame,
    output_path: str,
    output_format: str,
    mode: str = "overwrite",
) -> None:
    """Spark DataFrame을 지정한 형식으로 저장한다."""

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    writer = df.coalesce(1).write.mode(mode)

    if output_format == "csv":
        writer.option("header", True).csv(output_path)
    elif output_format == "parquet":
        writer.parquet(output_path)
    else:
        raise ValueError(
            f"지원하지 않는 저장 형식입니다: {output_format}"
        )