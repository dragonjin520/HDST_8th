from pathlib import Path

from pyspark.sql import DataFrame


def save_dataframe(
    df: DataFrame,
    output_path: Path,
    output_format: str,
    output_mode: str,
    coalesce_to_one: bool = False,
) -> None:
    """DataFrame을 지정한 형식과 경로로 저장한다."""
    output_df = df.coalesce(1) if coalesce_to_one else df

    (
        output_df.write
        .mode(output_mode)
        .format(output_format)
        .save(str(output_path))
    )