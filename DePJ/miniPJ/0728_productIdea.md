# 2026-07-28 프로젝트 구상

## 주제

날씨와 서울 실시간 인구 데이터를 활용하여 공유자전거 수요를 파악하는 시스템을 설계한다.

과거 따릉이 대여이력으로 대여소별 기본 수요를 계산하고, 현재 날씨와 실시간 인구를 결합하여 가까운 시간대의 예상 수요와 자전거 부족 가능성을 산출한다.

## 해결하려는 문제

따릉이 운영자는 현재 특정 대여소에서 자전거가 부족해질지 미리 판단하기 어렵다.

따라서 다음 정보를 제공하는 데이터 프로덕트를 목표로 한다.

- 대여소별 향후 1시간 예상 대여 수요
- 대여소별 예상 반납 수요
- 현재 자전거 수량 대비 예상 부족 수량
- 자전거 재배치 우선순위
- 시간대별·요일별·날씨별 수요 변화

예시는 다음과 같다.

```text
여의나루역 1번 출구 대여소
현재 자전거: 8대
현재 인구: 평소 대비 1.8배
기온: 24℃
강수량: 0mm
시간대: 평일 18시

예상 1시간 대여 수요: 19건
예상 부족 수량: 11대
운영 판단: 자전거 재배치 필요
```

## 전체 시스템 구조

```text
따릉이 대여이력 CSV
서울 실시간 인구 API
날씨 API
대여소 정보
        ↓
Raw 데이터 저장
        ↓
Spark RDD 정제 및 집계
        ↓
시간·지역 기준 조인
        ↓
대여소별 예상 수요 계산
        ↓
PostgreSQL / Redis / Parquet 저장
        ↓
수요 지도 및 재배치 대시보드 제공
```

## 데이터 구성

### 1. 따릉이 대여이력

과거 수요를 계산하는 핵심 데이터다.

주요 컬럼은 다음과 같다.

- 대여일시
- 대여 대여소 번호
- 반납일시
- 반납 대여소 번호
- 이용시간
- 이용거리

대여이력에서 다음 집계 데이터를 생성한다.

```text
date
hour
day_of_week
station_id
rental_count
return_count
net_demand
```

순수요는 다음과 같이 계산한다.

```text
순수요 = 대여 건수 - 반납 건수
```

- 양수: 자전거가 빠져나가는 대여소
- 음수: 자전거가 들어오는 대여소

단순 대여 건수보다 순수요가 자전거 재배치 필요성을 판단하기에 더 적합하다.

### 2. 서울 실시간 인구

서울 실시간 도시데이터에서 다음 정보를 수집한다.

- area_code
- area_name
- population_min
- population_max
- congestion_level
- observed_at

실시간 인구 데이터는 대여소 단위가 아니라 특정 장소 또는 생활권 단위이므로 다음과 같은 매핑 테이블이 필요하다.

```text
station_id → area_code
```

### 3. 날씨 데이터

주요 컬럼은 다음과 같다.

- observed_at
- temperature
- precipitation
- humidity
- wind_speed
- weather_condition

예상 영향은 다음과 같다.

- 비가 오면 수요 감소
- 강풍이면 수요 감소
- 너무 덥거나 추우면 수요 감소
- 적정 기온에서는 수요 증가

## Spark RDD 처리 흐름

```text
Raw RDD
→ parse
→ filter
→ map
→ reduceByKey
→ join
→ demand score 계산
→ Action
```

RDD는 Transformation을 연결하여 Lineage와 DAG를 구성하고, Action이 호출되는 시점에 실제 연산을 수행한다.

## 따릉이 데이터 처리

### 1. 데이터 로드

```python
bike_raw_rdd = sc.textFile("data/raw/bike/*.csv")
```

### 2. 헤더 제거 및 파싱

```python
header = bike_raw_rdd.first()

bike_rdd = (
    bike_raw_rdd
    .filter(lambda line: line != header)
    .map(parse_bike_record)
)
```

사용된 연산은 다음과 같다.

| 연산 | 종류 | 역할 |
|---|---|---|
| `first()` | Action | CSV 헤더 확인 |
| `filter()` | Transformation | 헤더와 잘못된 행 제거 |
| `map()` | Transformation | 문자열을 구조화된 레코드로 변환 |

## 과거 기본 수요 계산

대여소, 날짜, 시간을 기준으로 일별 대여 건수를 계산한다.

```python
daily_hourly_rdd = (
    bike_rdd
    .map(
        lambda row: (
            (
                row.station_id,
                row.rental_datetime.date(),
                row.rental_datetime.hour
            ),
            1
        )
    )
    .reduceByKey(lambda a, b: a + b)
)
```

이후 대여소, 요일, 시간 기준으로 평균을 계산한다.

```python
baseline_demand_rdd = (
    daily_hourly_rdd
    .map(convert_to_weekday_key)
    .mapValues(lambda count: (count, 1))
    .reduceByKey(
        lambda a, b: (
            a[0] + b[0],
            a[1] + b[1]
        )
    )
    .mapValues(lambda x: x[0] / x[1])
)
```

결과 구조는 다음과 같다.

```text
((station_id, weekday, hour), average_rental_count)
```

이 값이 날씨와 실시간 인구를 적용하기 전의 기본 수요다.

## 날씨 데이터 처리

```python
weather_rdd = (
    sc.textFile("data/raw/weather/*.json")
    .map(parse_weather)
    .filter(is_valid_weather)
    .map(
        lambda row: (
            truncate_to_hour(row.observed_at),
            {
                "temperature": row.temperature,
                "precipitation": row.precipitation,
                "humidity": row.humidity,
                "wind_speed": row.wind_speed
            }
        )
    )
)
```

동일 시간대에 여러 관측값이 존재하면 시간 단위 평균을 계산한다.

## 실시간 인구 데이터 처리

```python
population_rdd = (
    sc.textFile("data/raw/population/latest/*.json")
    .map(parse_population)
    .filter(is_valid_population)
    .map(
        lambda row: (
            row.area_code,
            {
                "population": (
                    row.population_min + row.population_max
                ) / 2,
                "congestion_level": row.congestion_level,
                "observed_at": row.observed_at
            }
        )
    )
)
```

대여소와 실시간 인구 지역을 연결한다.

```python
station_population_rdd = (
    station_area_rdd
    .join(population_rdd)
    .map(
        lambda row: (
            row[1][0],
            row[1][1]
        )
    )
)
```

결과 구조는 다음과 같다.

```text
station_id → current_population
```

## 기본 수요와 실시간 데이터 조인

현재 요일과 시간에 해당하는 기본 수요를 선택한다.

```python
current_baseline_rdd = baseline_demand_rdd.filter(
    lambda row:
        row[0][1] == current_weekday
        and row[0][2] == current_hour
)
```

대여소 ID를 기준으로 실시간 인구와 조인한다.

```python
demand_population_rdd = (
    station_baseline_rdd
    .leftOuterJoin(station_population_rdd)
)
```

날씨 데이터가 서울 전체에서 동일하게 적용된다면 Broadcast Variable을 사용해 큰 셔플을 줄일 수 있다.

```python
weather_broadcast = sc.broadcast(current_weather)
```

## 수요 계산 방식

초기 프로토타입에서는 다음과 같은 규칙 기반 계산식을 사용할 수 있다.

```text
예상 수요
= 과거 기본 수요
× 인구 보정 계수
× 날씨 보정 계수
```

예시는 다음과 같다.

```text
인구가 평소와 비슷함: 1.0
인구가 평소보다 20% 많음: 1.2
인구가 평소보다 50% 많음: 1.5

비 없음: 1.0
약한 비: 0.8
강한 비: 0.5
강풍: 0.7
적정 기온: 1.1
```

다만 최종 시스템에서는 임의로 계수를 정하기보다 과거 데이터로 다음 관계를 검증해야 한다.

```text
따릉이 대여량
↔ 실시간 인구 변화율
↔ 기온
↔ 강수량
↔ 풍속
```

## Transformation과 Action

### Narrow Transformation

부모 파티션 하나의 데이터만으로 처리할 수 있다.

- `map`
- `filter`
- `flatMap`
- `mapValues`

파티션 간 데이터 이동이 적어 비교적 빠르다.

### Wide Transformation

여러 파티션의 데이터를 다시 모아야 하므로 셔플이 발생한다.

- `reduceByKey`
- `groupByKey`
- `join`
- `distinct`
- `sortByKey`

이번 시스템에서는 다음 단계에서 셔플이 발생할 가능성이 크다.

- 대여소별 집계
- 시간대별 집계
- 인구 데이터 조인
- 대여소와 지역 매핑 조인

`groupByKey()`보다 `reduceByKey()`를 우선 사용하여 셔플 데이터 양을 줄인다.

### Action

Action이 호출될 때 실제 DAG 실행이 시작된다.

- `count`
- `collect`
- `take`
- `takeOrdered`
- `first`
- `saveAsTextFile`
- `foreach`

예상 수요가 높은 대여소를 확인하는 예시는 다음과 같다.

```python
top_demand_stations = (
    predicted_demand_rdd
    .takeOrdered(
        20,
        key=lambda row: -row[1]["predicted_demand"]
    )
)
```

## 캐싱 전략

과거 기본 수요 데이터는 여러 분석에서 반복 사용되므로 캐싱할 수 있다.

```python
baseline_demand_rdd.cache()
baseline_demand_rdd.count()
```

다음 분석에서 공통으로 재사용할 수 있다.

- 날씨 반영 수요
- 인구 반영 수요
- 대여소별 부족 예측
- 요일별 수요 대시보드
- 시간대별 수요 대시보드

## Airflow 운영 구조

```text
fetch_bike_history
        ↓
validate_bike_data
        ↓
build_baseline_demand
        ↓
fetch_weather ─────┐
                   ├→ calculate_demand
fetch_population ──┘
        ↓
validate_prediction
        ↓
load_serving_database
        ↓
refresh_dashboard
```

예상 실행 주기는 다음과 같다.

- 과거 따릉이 기본 수요 집계: 하루 1회
- 날씨 수집: 10분 또는 1시간마다
- 실시간 인구 수집: API 갱신 주기에 맞춤
- 현재 수요 계산: 10분마다
- 대시보드 갱신: 계산 완료 후

## 실시간 처리 시 고려사항

일반 RDD는 배치 처리 모델이므로 API를 주기적으로 수집하고 Spark 작업을 실행하면 엄밀한 실시간 처리보다는 마이크로 배치에 가깝다.

```text
10분마다 API 호출
→ JSON 저장
→ Spark RDD 실행
→ 수요 계산
→ DB 갱신
```

초기 프로젝트에서는 다음과 같이 역할을 나누는 것이 적절하다.

```text
과거 대용량 따릉이 분석
→ Spark RDD 또는 DataFrame

실시간 인구·날씨 처리
→ 주기적 마이크로 배치

전체 작업 스케줄링
→ Airflow
```

추후 실시간성을 강화할 경우 다음 구조로 확장할 수 있다.

```text
실시간 인구·날씨 API
→ Kafka
→ Spark Structured Streaming
→ Redis / PostgreSQL
→ API 및 대시보드
```

## 최종 권장 아키텍처

```text
[Batch 영역]

따릉이 대여이력
    ↓
Spark RDD
    ↓
대여소 × 요일 × 시간 기본 수요
    ↓
Parquet / PostgreSQL


[실시간 영역]

서울 실시간 인구 API ─┐
                      ├→ Raw JSON
날씨 API ─────────────┘
                           ↓
                  Spark 마이크로 배치
                           ↓
                기본 수요와 실시간 조인
                           ↓
                 예상 대여·반납 수요
                           ↓
               PostgreSQL + Redis
                           ↓
       지도 대시보드 / 재배치 추천 API
```

## 핵심 RDD 연산 흐름

```text
textFile
→ filter
→ map
→ reduceByKey
→ leftOuterJoin
→ mapValues
→ cache
→ takeOrdered / count / saveAsTextFile
```

## 최종 데이터 프로덕트 정의

과거 대여 패턴, 현재 유동 인구, 현재 날씨를 조합하여 대여소별 단기 수요와 자전거 부족 가능성을 계산하고, 운영자에게 재배치 우선순위를 제공하는 데이터 프로덕트를 만든다.

초기 구현 범위는 다음 세 가지로 제한한다.

1. 대여소별 향후 1시간 예상 대여 건수
2. 현재 자전거 수량 대비 예상 부족 수량
3. 자전거 재배치 우선순위