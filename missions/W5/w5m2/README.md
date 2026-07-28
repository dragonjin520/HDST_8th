# W5M2 — NYC Taxi DataFrame 및 DAG 분석

## 1. 과제 목표

NYC Taxi and Limousine Commission(TLC) 운행 데이터를 Apache Spark DataFrame으로 처리하고, 변환 과정에서 생성되는 실행 계획과 DAG를 분석한다.

이번 과제에서는 다음 내용을 확인했다.

- Spark DataFrame을 이용한 Parquet 데이터 로딩과 정제
- `select`, `filter`, `withColumn`, `groupBy`, `agg` Transformation 적용
- `count`, `show`, `write` Action 실행
- Lazy Evaluation과 Action 실행 시점의 관계
- Shuffle을 기준으로 나뉘는 Stage와 DAG 구조
- `persist()`를 통한 반복 연산 최적화
- Spark UI를 이용한 Job, Stage, Task 및 DAG 확인

### 1.1 W5M1과의 차이

| 구분 | W5M1 | W5M2 |
|---|---|---|
| 핵심 기술 | RDD | DataFrame |
| 데이터 표현 | Python 튜플·객체 중심 | 컬럼과 스키마 중심 |
| 주요 목적 | RDD 변환과 복구 원리 이해 | DataFrame 실행 계획과 DAG 최적화 이해 |
| 대표 연산 | `map`, `filter`, `reduce`, `reduceByKey` | `select`, `filter`, `withColumn`, `groupBy`, `agg` |
| 최적화 주체 | 개발자가 연산 구조를 직접 설계 | Catalyst Optimizer와 Spark SQL 엔진이 실행 계획 최적화 |
| 스키마 | 기본적으로 없음 | 컬럼명과 데이터 타입이 존재 |
| 분석 관점 | 데이터를 한 행씩 어떻게 변환하는가 | 컬럼 단위 연산이 어떤 실행 계획으로 바뀌는가 |
| 결과 확인 | RDD 결과, 파티션, Lineage | 논리·물리 계획, DAG, Stage, Shuffle |
| 실무 활용 | 저수준 제어, 비정형 처리 | 정형 데이터 ETL, 집계, 분석 |

W5M1이 RDD의 Immutable 특성, Lineage, 장애 복구 원리를 확인하는 과제였다면, W5M2는 DataFrame 연산이 Spark 내부에서 최적화된 실행 계획으로 변환되는 과정을 확인하는 과제다.

## 2. 분석 데이터

- 데이터 출처: NYC TLC Trip Record Data
- 대상 데이터: Yellow Taxi Trip Records
- 분석 파일: `yellow_tripdata_2026-02.parquet`
- 분석 기간: 2026-02-01 이상, 2026-03-01 미만
- 입력 형식: Parquet
- 파일 크기: 약 56MB
- 입력 파티션 수: 11개

원본 파일에는 2026년 1월 31일 데이터 12건과 2026년 3월 1일 데이터 3건이 포함되어 있었다. 데이터 품질 오류와 분석 범위 제외를 구분하기 위해 해당 15건은 무효 데이터로 분류하지 않고 분석 기간 밖의 데이터로 별도 제외했다.

원본 데이터는 Git 저장소에 포함하지 않고 `data/raw/` 아래에서 관리한다.

## 3. 프로젝트 구조

```text
w5m2/
├── README.md
├── .gitignore
├── config/
│   └── analysis_config.py
├── data/
│   └── raw/
│       └── yellow_tripdata_2026-02.parquet
├── output/
│   ├── hourly_summary/
│   ├── daily_summary/
│   └── quality_summary/
├── screenshots/
│   ├── jobs.png
│   ├── stages.png
│   ├── sql_plan.png
│   └── dag.png
└── src/
    ├── main.py
    ├── extract.py
    ├── transform.py
    └── load.py
```

- `data/`: 원본 입력 데이터
- `src/`: 데이터 로딩, 정제, 집계, 저장 코드
- `config/`: 입력·출력 경로와 정제 기준
- `output/`: 정제 데이터와 집계 결과
- `screenshots/`: Spark UI 실행 화면

## 4. Data / Code / Config 분리

### Data

- 원본 Parquet 데이터
- 시간대별 집계 결과
- 일자별 집계 결과
- 데이터 품질 요약

### Code

- `extract.py`: SparkSession 생성 및 Parquet 로딩
- `transform.py`: 컬럼 선택, 기간 필터, 파생 컬럼 생성, 데이터 검증, 집계
- `load.py`: 결과 저장
- `main.py`: 전체 파이프라인 실행

### Config

`analysis_config.py`에서 다음 값을 관리한다.

- 입력 및 출력 경로
- Spark 애플리케이션 이름
- 실행 Master URL
- 분석 시작일과 종료일
- 최대 이동 거리
- 최대 운행 시간
- 출력 형식과 저장 모드

경로와 정제 기준을 코드에 직접 하드코딩하지 않아 동일한 파이프라인을 다른 월 데이터에도 적용할 수 있도록 구성했다.

## 5. 분석 흐름

```text
Parquet 입력
    ↓
필수 컬럼 선택
    ↓
분석 기간 여부·운행 시간·일자·시간대 파생 컬럼 생성
    ↓
검증 사유 컬럼 생성
    ↓
validated_df persist
    ↓
유효 데이터 필터
    ↓
시간대별·일자별·품질별 집계
    ↓
세 집계 결과만 Parquet 저장
```

### 5.1 Extract

Spark의 Parquet Reader를 이용해 TLC 데이터를 DataFrame으로 읽었다.

```python
trip_df = spark.read.parquet(str(input_path))
```

로딩 후 다음 항목을 확인했다.

- 입력 경로
- DataFrame 스키마
- 파티션 수
- 샘플 데이터

주요 컬럼 타입은 다음과 같다.

```text
tpep_pickup_datetime  : timestamp_ntz
tpep_dropoff_datetime : timestamp_ntz
passenger_count       : long
trip_distance         : double
fare_amount           : double
total_amount          : double
```

### 5.2 Transform

분석에 필요한 컬럼만 초기에 선택하고, 승차 시각을 기준으로 2026년 2월 데이터만 남겼다.

생성한 파생 컬럼은 다음과 같다.

- `trip_duration_minutes`: 승차 시각과 하차 시각의 차이
- `pickup_date`: 승차 일자
- `pickup_hour`: 승차 시간대

#### 데이터 정제 기준

- 필수 컬럼이 Null이 아닐 것
- 하차 시각이 승차 시각보다 늦을 것
- 이동 거리가 0보다 클 것
- 이동 거리가 100마일 이하일 것
- 운행 시간이 300분 이하일 것
- 운임이 음수가 아닐 것
- 총 결제 금액이 음수가 아닐 것

무효 데이터를 바로 제거하지 않고 `validation_reason` 컬럼에 첫 번째 무효 사유를 기록한 뒤 유효 데이터와 무효 데이터를 분리했다.

```text
missing_required_value
invalid_datetime_order
non_positive_distance
excessive_distance
excessive_duration
negative_fare
negative_total_amount
```

이를 통해 파이프라인이 정상 종료되더라도 잘못된 데이터가 조용히 제거되는 Silent Failure를 방지했다.

### 5.3 Aggregate

유효 데이터에 대해 다음 집계를 수행했다.

#### 시간대별 집계

- 운행 건수
- 평균 이동 거리
- 평균 운행 시간
- 총수익
- 건당 평균 수익

#### 일자별 집계

- 운행 건수
- 평균 이동 거리
- 평균 운행 시간
- 총수익

### 5.4 Load

결과를 Parquet 형식으로 저장했다.

- `output/hourly_summary/`: 시간대별 집계
- `output/daily_summary/`: 일자별 집계
- `output/quality_summary/`: `valid`, `out_of_period`, 무효 사유별 건수

초기 구현에서는 정제된 전체 데이터도 저장했지만, 과제 요구사항에 필수적이지 않고 별도의 `write` Action과 Stage를 생성하므로 최종 구현에서는 제거했다. 최종 출력은 모두 수십 행 규모의 소규모 집계 결과이므로 `coalesce(1)`을 적용했다.

## 6. 실행 방법

프로젝트 루트에서 다음 명령으로 실행한다.

```bash
python -m src.main
```

사용한 로컬 실행 환경은 다음과 같다.

```text
PySpark : 4.2.0
Master  : local[*]
```

## 7. 실행 결과

```text
Input path            : data/raw/yellow_tripdata_2026-02.parquet
Partition count       : 11
Raw row count         : 3,399,866
Out-of-period count   : 15
Valid trip count      : 3,210,520
Invalid trip count    : 189,331
```

다음 관계가 성립한다.

```text
Raw row count
= Out-of-period count
+ Valid trip count
+ Invalid trip count
```

```text
3,399,866
= 15 + 3,210,520 + 189,331
```

분석 기간 필터를 적용하기 전 실행에서는 무효 데이터가 189,346건이었으며, 이 값에는 분석 기간 밖의 15건이 포함되어 있었다. 기간 필터 적용 후에는 해당 데이터를 별도 제외하므로 분석 기간 내 무효 데이터는 189,331건이다.

### 7.1 무효 사유별 건수

| 무효 사유 | 건수 |
|---|---:|
| 이동 거리 0 이하 | 122,354 |
| 하차 시각이 승차 시각보다 빠르거나 같음 | 40,598 |
| 음수 운임 | 24,641 |
| 운행 시간 300분 초과 | 1,134 |
| 음수 총 결제 금액 | 447 |
| 이동 거리 100마일 초과 | 172 |

가장 큰 품질 문제는 이동 거리 0 이하 데이터였으며 전체 무효 데이터의 대부분을 차지했다. 또한 샘플 데이터에서도 승차 시각과 하차 시각이 동일하지만 이동 거리와 요금이 존재하는 레코드를 확인할 수 있었다.

### 7.2 시간대별 분석 결과

운행 건수가 가장 많은 시간대는 18시였다.

| 시간대 | 운행 건수 | 총수익 | 건당 평균 수익 |
|---:|---:|---:|---:|
| 18시 | 220,353 | 6,608,299.32 | 29.99 |
| 17시 | 205,117 | 6,325,765.77 | 30.84 |
| 21시 | 200,259 | 6,136,951.03 | 30.65 |
| 19시 | 199,741 | 6,083,905.33 | 30.46 |
| 20시 | 196,858 | 5,931,332.61 | 30.13 |

18시는 운행 건수와 총수익이 모두 가장 높았다. 반면 건당 평균 수익은 새벽 4~5시가 높았다.

```text
04시 평균 수익: 36.10
05시 평균 수익: 36.52
```

이는 새벽 시간대의 운행 건수는 적지만 평균 이동 거리가 더 길기 때문으로 해석할 수 있다.

### 7.3 일자별 분석 결과

분석 기간은 2026년 2월 1일부터 2월 28일까지이며, 기간 필터 적용 후 일자별 결과는 총 28행으로 생성됐다.

운행 건수가 가장 많은 날은 2월 7일이었다.

```text
2026-02-07
운행 건수: 149,980
총수익    : 4,292,921.19
```

운행 건수가 가장 적은 날은 2월 23일이었다.

```text
2026-02-23
운행 건수: 22,840
총수익    : 640,390.74
```

2월 23일은 다른 날짜보다 운행 건수와 총수익이 급격히 낮아 외부 요인 또는 데이터 수집 상태를 추가로 확인할 필요가 있다.

## 8. Lazy Evaluation 검증

`select`, `withColumn`, `filter`, `groupBy`, `agg`, `persist`를 선언하는 시점에는 실제 Job이 실행되지 않는다. 최종 구현에서는 다음 세 개의 `write` Action이 호출될 때 Transformation이 실제로 실행된다.

```python
save_dataframe(hourly_summary_df, ...)
save_dataframe(daily_summary_df, ...)
save_dataframe(quality_summary_df, ...)
```

첫 번째 `write` Action에서 원본 Parquet 스캔, 파생 컬럼 생성, 검증 사유 생성, `validated_df` 캐시 물질화가 실행된다. 이후 일자별·품질별 집계는 `InMemoryTableScan`을 통해 캐시된 결과를 재사용한다.

Spark UI의 SQL / DataFrame 화면에서도 최종 실행은 세 개의 `save` 실행으로 확인됐다. 하나의 `write` Action은 집계, Shuffle, 정렬, 파일 저장을 포함하므로 여러 Job과 Stage를 만들 수 있다.

## 9. DAG 및 Stage 분석

예상한 주요 실행 흐름은 다음과 같다.

```text
Scan Parquet
    ↓
Project — 분석 기간·운행 시간·일자·시간대 생성
    ↓
Project — validation_reason 생성
    ↓
InMemoryRelation / InMemoryTableScan
    ↓
Filter / Project
    ↓
Partial HashAggregate
    ↓
Exchange — groupBy Shuffle
    ↓
Final HashAggregate
    ↓
Exchange — 정렬 Shuffle
    ↓
Sort
    ↓
Coalesce(1)
    ↓
WriteFiles
```

### 9.1 Narrow Transformation

다음 연산은 일반적으로 파티션 간 데이터 이동 없이 같은 Stage 안에서 처리될 수 있다.

- `select`
- `filter`
- `withColumn`

Spark SQL은 이 연산들을 `WholeStageCodegen`으로 묶어 하나의 Java 실행 코드로 생성해 함수 호출 오버헤드를 줄였다.

### 9.2 Shuffle과 Stage 경계

시간대별·일자별 `groupBy`에서는 같은 키를 가진 데이터를 하나의 파티션으로 모아야 한다.

```text
groupBy
→ Exchange
→ Shuffle Write
→ 데이터 재분배
→ Shuffle Read
→ HashAggregate
```

Spark UI의 Stage 화면에서 Shuffle Read와 Shuffle Write가 확인되었고, DAG Visualization에서는 `Exchange` 노드가 나타났다. 이 `Exchange`가 Shuffle 경계이며 새로운 Stage가 만들어지는 기준이 된다.

### 9.3 파티션과 Task

원본 DataFrame의 파티션 수는 11개였다. 따라서 원본 또는 캐시된 데이터를 처리하는 주요 Stage에서는 다음과 같이 11개의 Task가 실행됐다.

```text
입력 파티션 11개
→ Task 11개
```

집계 후 결과 크기가 작아지거나 `coalesce(1)`이 적용된 구간에서는 1개의 Task만 실행된 Stage도 확인할 수 있었다.

### 9.4 Completed Stage 최적화 결과

초기 구현에서는 데이터 확인을 위한 `count()`와 `show()`, 정제 데이터 전체 저장, 별도 품질 집계, 전역 정렬로 인해 32개의 Completed Stage가 발생했다.

최적화 과정은 다음과 같다.

| 단계 | Completed Stages | 주요 변경 |
|---|---:|---|
| 초기 구현 | 32 | 반복 `count()`, `show()`, 정제 데이터 저장 포함 |
| 중복 Action 제거 | 16 | 확인용 `show()` 제거, `count()` 통합 |
| Count 및 품질 처리 통합 | 15 | 두 번의 `first()`를 하나의 품질 흐름으로 축소 |
| 최종 구현 | 12 | 콘솔용 Action과 정제 데이터 저장 제거 |

초기 32개에서 최종 12개로 줄어 Completed Stage를 62.5% 감소시켰다.

```text
감소율 = (32 - 12) / 32 = 62.5%
```

최종 실행에는 시간대별, 일자별, 품질별 세 개의 `write` Action만 남겼다. 각 결과는 서로 다른 키로 집계되므로 각각의 `groupBy` Shuffle과 결과 저장 Stage는 필요하다.

Spark UI에는 9개의 Skipped Stage도 표시됐다. 이는 실패한 Stage가 아니라 AQE가 실행 중 Shuffle 통계를 확인한 뒤 초기 물리 계획 일부를 대체하면서 실제 Task를 실행하지 않은 기록이다. 모두 `0/11` 또는 `0/1` Task로 표시됐으므로 실제 실행 비용은 Completed Stage 12개를 기준으로 평가했다.

## 10. `persist()` 최적화

시간대별, 일자별, 품질별 집계가 동일한 정제·검증 결과를 반복 사용하므로 공통 중간 DataFrame인 `validated_df`를 캐시했다.

```python
validated_df = validated_df.persist(
    StorageLevel.MEMORY_AND_DISK
)
```

첫 번째 `write` Action에서 캐시가 물질화되고, 후속 집계에서는 Spark UI의 `InMemoryTableScan`을 통해 캐시된 결과를 읽는다. 이를 통해 원본 Parquet 스캔과 파생 컬럼·검증 사유 계산을 매 집계마다 반복하지 않도록 했다.

초기 구현에서는 `valid_df`와 `invalid_df`를 각각 persist했지만, 무효 데이터는 품질 집계에서 한 번만 사용하므로 캐시 비용이 더 클 수 있었다. 최종 구현에서는 세 결과가 공통으로 참조하는 `validated_df` 하나만 persist하도록 단순화했다.

모든 저장이 끝난 뒤에는 캐시를 해제한다.

```python
validated_df.unpersist()
```

## 11. 실행 계획 확인

Spark UI의 Final Physical Plan에서 다음 연산을 확인했다.

- `Scan parquet`: 필요한 6개 컬럼만 원본 Parquet에서 읽음
- `Project`: 분석 기간 여부, 운행 시간, 승차 일자, 승차 시간대 생성
- `Project`: `validation_reason` 생성
- `InMemoryRelation`: `validated_df` 캐시 저장
- `InMemoryTableScan`: 후속 집계에서 캐시 재사용
- `Filter`: 분석 기간 내부의 유효 데이터만 선택
- `HashAggregate`: 파티션별 부분 집계와 최종 집계
- `Exchange hashpartitioning`: 시간대·일자·품질 그룹 기준 Shuffle
- `AQEShuffleRead`: AQE가 작은 Shuffle 파티션을 병합해 읽음
- `Exchange rangepartitioning`: 시간대·일자 순서 정렬을 위한 Shuffle
- `Sort`: 시간대와 일자 오름차순 정렬
- `Coalesce(1)`: 소규모 집계 결과를 하나의 출력 파티션으로 축소
- `WriteFiles`: Parquet 결과 저장

### 11.1 시간대별 집계 계획

```text
InMemoryTableScan
→ Filter
→ Project
→ Partial HashAggregate(pickup_hour)
→ Exchange hashpartitioning
→ Final HashAggregate
→ Exchange rangepartitioning
→ Sort
→ Coalesce(1)
→ WriteFiles
```

시간대별 결과는 최종 24행이며, 첫 번째 Shuffle 전 파티션 내부 부분 집계를 수행해 약 100.9MiB 입력을 약 7.5KiB의 중간 결과로 줄인 뒤 이동시켰다.

### 11.2 일자별 집계 계획

```text
InMemoryTableScan
→ Filter
→ Project
→ Partial HashAggregate(pickup_date)
→ Exchange hashpartitioning
→ Final HashAggregate
→ Exchange rangepartitioning
→ Sort
→ Coalesce(1)
→ WriteFiles
```

일자별 결과는 2026년 2월 1일부터 28일까지 최종 28행이다. AQE는 첫 번째 Shuffle 결과 59행, 두 번째 Shuffle 결과 28행을 확인하고 파티션을 병합했다.

### 11.3 품질별 집계 계획

```text
InMemoryTableScan
→ Project(quality_group)
→ Partial HashAggregate
→ Exchange hashpartitioning
→ AQEShuffleRead
→ Final HashAggregate
→ Coalesce(1)
→ WriteFiles
```

품질 결과는 `valid`, `out_of_period`, 각 무효 사유별 건수만 포함하므로 별도의 전역 정렬 없이 한 번의 집계 Shuffle과 저장으로 처리됐다.

## 12. Spark UI 캡처

### 12.1 Completed Jobs

최종 구현에서는 시간대별·일자별·품질별 세 개의 `save` Action만 실행했다.

### 12.2 Completed Stages

초기 32개에서 최종 12개로 줄어든 Completed Stage와, 입력 파티션 11개에 대응하는 Task 및 Shuffle Read·Write를 확인했다.

### 12.3 SQL / DataFrame

최종 실행의 세 개 `save` Action과 각 Action에서 생성된 Job 및 AdaptiveSparkPlan을 확인했다.

### 12.4 DAG Visualization

DAG에서 `Scan parquet`, `Project`, `InMemoryTableScan`, `HashAggregate`, `Exchange`, `AQEShuffleRead`, `Sort`, `WriteFiles`를 확인했다.

## 13. 최적화 전략

### 13.1 필요한 컬럼만 조기에 선택

원본 20개 컬럼 중 분석과 품질 검증에 필요한 6개 컬럼만 읽도록 구성했다. Physical Plan의 `ReadSchema`에서도 해당 6개 컬럼만 확인됐다.

### 13.2 공통 전처리 결과 한 번만 계산

분석 기간 여부, 운행 시간, 승차 일자·시간대, 검증 사유를 포함한 `validated_df`를 한 번 계산해 persist했다. 시간대별·일자별·품질별 집계는 모두 이 캐시를 재사용한다.

### 13.3 부분 집계 후 Shuffle

Spark는 각 입력 파티션에서 `partial HashAggregate`를 먼저 수행하고 작은 중간 결과만 Shuffle했다. 예를 들어 시간대별 집계는 약 100.9MiB 입력을 약 7.5KiB의 중간 결과로 축소한 뒤 전송했다.

### 13.4 콘솔 확인용 Action 제거

초기 구현의 `count()`, `show()`, `first()`는 결과 확인에는 유용하지만 각각 별도의 Job과 Stage를 생성했다. 최종 구현에서는 과제 요구사항에 없는 콘솔 출력 Action을 제거하고 세 개의 `write` Action만 남겼다.

### 13.5 정제 데이터 전체 저장 제거

정제 데이터 전체 저장은 별도의 `write` Action과 Stage를 만들지만 최종 과제 결과에는 필수적이지 않았다. 시간대별·일자별·품질별 집계 결과만 저장하도록 변경했다.

### 13.6 품질 상태 통합

별도의 유효 건수, 무효 건수, 기간 외 건수 Action을 실행하지 않고 `quality_group` 하나로 통합했다.

```text
valid
out_of_period
missing_required_value
invalid_datetime_order
non_positive_distance
excessive_distance
excessive_duration
negative_fare
negative_total_amount
```

### 13.7 AQE 유지

AQE는 작은 Shuffle 파티션을 병합하고 초기 계획의 일부 Stage를 건너뛰었다. Skipped Stage를 없애기 위해 AQE를 끄면 초기 계획의 Task가 실제 실행될 수 있으므로 AQE를 유지했다.

### 13.8 결과 정렬의 비용 확인

시간대별·일자별 실행 계획에는 `Exchange rangepartitioning → Sort`가 남아 있어 각 결과마다 추가 Shuffle이 발생한다. 출력 순서가 필수가 아니라면 `orderBy`를 제거해 Completed Stage를 더 줄일 수 있다. 현재는 가독성 있는 결과 순서를 유지하기 위해 정렬을 남겼다.

## 14. 결론

이번 과제를 통해 DataFrame Transformation은 즉시 실행되지 않고 실행 계획에 기록되며, `write` Action이 호출될 때 실제 Job과 Stage가 생성된다는 점을 확인했다.

초기 구현에서는 반복적인 `count()`, `show()`, 정제 데이터 전체 저장, 별도 품질 집계로 인해 32개의 Completed Stage가 발생했다. 이후 콘솔 확인용 Action 제거, 품질 상태 통합, 공통 `validated_df` persist, 정제 데이터 저장 제거를 적용해 Completed Stage를 12개로 줄였다. 이는 초기 대비 62.5% 감소한 결과다.

최종 Physical Plan에서는 원본 Parquet에서 필요한 6개 컬럼만 읽고, 공통 전처리 결과를 `InMemoryRelation`에 저장한 뒤 세 집계에서 `InMemoryTableScan`으로 재사용했다. 각 집계는 파티션 내부의 부분 `HashAggregate` 후 작은 중간 결과만 Shuffle했으며, AQE는 실행 시점의 통계를 바탕으로 Shuffle 파티션을 병합했다.

남은 Completed Stage는 시간대별·일자별·품질별 세 개의 독립적인 집계와 결과 저장에 필요한 단계다. 시간대별·일자별 출력의 전역 정렬에 추가 Shuffle이 사용되므로, 출력 순서가 필요하지 않다면 정렬 제거를 통해 더 줄일 여지가 있다. 다만 현재 12개 Stage는 정렬된 세 결과를 각각 저장하는 요구를 유지하는 범위에서 합리적인 수준으로 판단했다.

W5M1에서는 RDD의 Lineage와 장애 복구 원리를 중심으로 Spark의 저수준 동작을 확인했다면, W5M2에서는 DataFrame의 스키마, Physical Plan, 부분 집계, Shuffle, AQE, 캐시 재사용을 중심으로 Spark SQL 엔진의 최적화 과정을 확인했다.

