import pandas as pd

input_path = "/data/population/unique_250m_grids_20260729.csv"
output_path = "/data/population/서울시 공공자전거 대여이력 2606.parquet"

df = pd.read_csv(
    input_path,
    encoding="cp949",
    low_memory=False,
)

df.to_parquet(
    output_path,
    index=False,
    compression="zstd",
)

print(df.shape)
print(output_path)