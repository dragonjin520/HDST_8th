# 학습 목표
The goal of this project is to apply your knowledge of Apache Spark to process and analyze the 'NYC Taxi and Limousine Commission (TLC) Trip Record Data.' You will write a Spark application in Python that performs several key analyses on the dataset.

# 기능요구사항
## Data Ingestion:
The application should be able to ingest multiple months or years of data efficiently.

Handle different file formats such as CSV, Parquet, etc.

## Data Cleaning and Transformation:
Handle missing values by either removing or imputing them.

Convert all relevant time fields to a standard timestamp format.

Filter out records with non-sensical values (e.g., negative trip duration or distance).

## Metrics Calculation:
Average Trip Duration: Compute the average duration of trips.

Average Trip Distance: Compute the average distance of trips.

Ensure the results are stored and displayed in a human-readable format.

## Peak Hours Analysis:
Define "peak hours" based on the number of trips starting per hour.

Visualize the distribution of trips across different hours of the day.

Highlight the hours with the highest number of trips.

## Weather Condition Analysis:
Correlate trip demand with weather conditions (e.g., temperature, precipitation).

Present the findings on how different weather conditions affect the number of trips.

Use statistical methods to validate the findings.

## Output:
The final output should be saved to a file (e.g., CSV, Parquet)

Provide all calculated metrics and analysis results.

Generate visualizations (e.g., bar charts, line graphs) to support your findings.

# 프로그래밍 요구사항
## Environment Setup:

Ensure your Spark environment is correctly configured.

## Data Loading:

Load the TLC Trip Record Data into a Spark DataFrame.

## Data Cleaning:

Clean the data to handle any missing or inconsistent entries.

## Calculation of Metrics:

Calculate the average trip duration.

Calculate the average trip distance.

## Peak Hours Identification:

Identify the peak hours for taxi usage.

## Weather Condition Analysis:

Analyze the effect of weather conditions on taxi demand. Use additional datasets if necessary to obtain weather information for the corresponding time periods.

Use Jupyter Notebook to visualize your findings.

# 팀 활동 요구사항
만약 이 데이터가 사람이 운행하는 차량의 데이터가 아니라 '자율주행차' 데이터라면 어떤 Data Product를 만들면 좋을까요? 아이디어를 수립하고 Prototype을 만들어 보세요.