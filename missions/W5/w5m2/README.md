# W5M2 — NYC Taxi DataFrame 및 DAG 분석

## 1. 과제 목표

NYC Taxi and Limousine Commission(TLC) 운행 데이터를 Apache Spark DataFrame으로 처리하고, 변환 과정에서 생성되는 DAG와 실행 계획을 분석한다.

이번 과제에서는 다음 내용을 확인한다.

- Spark DataFrame을 이용한 대용량 데이터 로딩과 정제
- `filter`, `withColumn`, `groupBy`, `join` 등의 Transformation 적용
- `count`, `collect`, `write` 등의 Action 실행
- Lazy Evaluation과 Action 실행 시점의 관계
- Shuffle을 기준으로 나뉘는 Stage와 DAG 구조
- `cache()` 또는 `persist()`를 통한 반복 연산 최적화
- Spark UI를 이용한 Job, Stage, Task 및 DAG 확인

### 1.1 과제 w5m1과 비교
|구분|W5M1|W5M2|
|--|--|--|
|핵심 기술|RDD|DataFrame|
|데이터 표현|Python 튜플·객체 중심|컬럼과 스키마 중심|
|주요 목적|RDD 변환과 복구 원리 이해|DataFrame 실행 계획과 DAG 최적화 이해|
|대표 연산|map, filter, reduce, reduceByKey|select, filter, withColumn, groupBy, agg, join|
|최적화 주체|개발자가 연산 구조를 직접 설계|Catalyst Optimizer와 Spark SQL 엔진이 실행 계획 최적화|
|스키마|기본적으로 없음|명확한 컬럼명과 데이터 타입 존재|
|분석 관점|데이터를 한 행씩 어떻게 변환하는가|컬럼 단위로 어떤 분석 계획을 만드는가|
|결과 확인|RDD 결과와 파티션 처리|논리 계획, 물리 계획, DAG, Stage, Shuffle|
|실무 활용|저수준 제어, 비정형 처리|정형 데이터 분석, ETL, 집계에서 주로 사용|

## 2. 분석 데이터

- 데이터 출처: NYC TLC Trip Record Data
- 대상 데이터: Yellow Taxi Trip Records
- 분석 기간: 실행 시 선택한 월별 데이터
- 입력 형식: Parquet

원본 데이터는 용량이 크므로 Git 저장소에 포함하지 않고 `data/raw/` 아래에 별도로 저장한다.

## 3. 프로젝트 구조

```text
w5m2/
├── README.md
├── config/
│   └── analysis_config.py
├── data/
│   └── raw/
├── output/
│   ├── cleaned_trips/
│   ├── hourly_summary/
│   ├── daily_summary/
│   └── quality_summary/
├── screenshots/
│   └── spark_ui/
└── src/
    ├── main.py
    ├── extract.py
    ├── transform.py
    └── load.py
```

- `data/`: 원본 입력 데이터
- `src/`: 데이터 로딩, 정제, 집계, 저장 코드
- `config/`: 경로와 정제 기준 등 실행 설정
- `output/`: 분석 및 품질 검증 결과
- `screenshots/`: Spark UI의 DAG와 실행 결과 화면

## 4. 분석 흐름

```text
Parquet 입력
    ↓
필수 컬럼 선택
    ↓
결측치 및 비정상 데이터 검증
    ↓
유효 데이터와 무효 데이터 분리
    ↓
파생 컬럼 생성
    ↓
시간대별·일자별 집계
    ↓
결과 저장
```

### 4.1 Extract

Spark의 Parquet Reader를 이용해 TLC 데이터를 DataFrame으로 읽는다.

확인 항목:

- 입력 경로
- 스키마
- 파티션 수
- 원본 행 수

### 4.2 Transform

필수 컬럼의 결측값을 제거하고 비현실적인 운행 데이터를 필터링한다.

정제 기준 예시:

- 승차 시각과 하차 시각이 존재할 것
- 하차 시각이 승차 시각보다 늦을 것
- 이동 거리가 0보다 클 것
- 운임과 총 결제 금액이 음수가 아닐 것
- 설정한 최대 이동 거리와 최대 이동 시간을 넘지 않을 것

주요 Transformation:

1. `select`: 분석에 필요한 컬럼 선택
2. `filter`: 결측치 및 비정상 운행 제거
3. `withColumn`: 운행 시간, 운행 일자, 승차 시간대 등 파생 컬럼 생성
4. `groupBy().agg()`: 시간대별·일자별 운행 건수, 평균 거리, 총수익 계산
5. `join`: 필요하면 외부 데이터와 결합

### 4.3 Load

최종 결과를 Parquet 또는 CSV 형식으로 저장한다.

저장 대상:

- 정제된 운행 데이터
- 시간대별 집계 결과
- 일자별 집계 결과
- 데이터 품질 요약

## 5. Lazy Evaluation 검증

Spark의 Transformation은 호출 즉시 실행되지 않고 논리 실행 계획에 기록된다.

다음 순서로 Lazy Evaluation을 확인한다.

1. DataFrame 로딩 및 Transformation 정의
2. 로그를 통해 아직 Job이 실행되지 않았음을 확인
3. `explain()`으로 논리·물리 실행 계획 확인
4. `count()` 또는 `write()` 실행
5. Spark UI에서 Job과 Stage가 생성된 것을 확인

## 6. Action

과제에서 사용할 주요 Action은 다음과 같다.

- `count()`: 원본, 유효, 무효 운행 건수 계산
- `collect()` 또는 `take()`: 소량의 결과를 Driver로 가져와 확인
- `write`: 집계 결과 저장

대규모 데이터를 Driver로 모두 가져오는 `collect()`는 피하고, 샘플 또는 소규모 집계 결과에만 사용한다.

## 7. DAG 및 Stage 분석

Spark UI에서 다음 내용을 확인한다.

- 하나의 Action마다 생성된 Job
- Narrow Transformation이 연결된 실행 구간
- `groupBy` 또는 `join`에서 발생하는 Shuffle
- Shuffle 경계를 기준으로 나뉜 Stage
- 각 Stage에서 실행된 Task 수와 입력 파티션의 관계

예상 DAG 흐름:

```text
Scan Parquet
    ↓
Project
    ↓
Filter
    ↓
WithColumn
    ↓
Exchange — Shuffle 발생
    ↓
HashAggregate
    ↓
Write
```

`select`, `filter`, `withColumn`은 일반적으로 Narrow Transformation으로 같은 Stage 안에서 처리될 수 있다. 반면 `groupBy`, `join`, `repartition`은 데이터 재분배가 필요해 Shuffle과 새로운 Stage를 만들 수 있다.

## 8. 최적화 전략

### 8.1 필요한 컬럼만 조기에 선택

분석에 사용하지 않는 컬럼을 초기에 제거해 이후 연산에서 처리할 데이터 크기를 줄인다.

### 8.2 필터를 집계보다 먼저 수행

결측치와 비정상 데이터를 먼저 제거한 뒤 집계해 Shuffle 대상 데이터의 양을 줄인다.

### 8.3 반복 사용하는 DataFrame 캐시

정제된 DataFrame을 여러 Action과 집계에서 반복 사용할 경우 `persist()`를 적용한다.

```python
cleaned_df = cleaned_df.persist()
```

모든 작업이 끝난 뒤에는 캐시를 해제한다.

```python
cleaned_df.unpersist()
```

### 8.4 불필요한 Action 방지

중간 확인을 위한 반복적인 `count()`와 `collect()`는 각각 별도의 Job을 생성하므로 필요한 경우에만 사용한다.

### 8.5 출력 파티션 조정

출력 파일이 지나치게 많이 생성되는 경우 결과 데이터 크기를 고려해 `coalesce()`를 사용한다. 단, 무조건 하나의 파티션으로 합치면 병렬성이 낮아질 수 있으므로 소규모 집계 결과에만 적용한다.

## 9. 데이터 품질 및 Silent Failure 방지

유효하지 않은 행을 단순히 제거하는 데서 끝내지 않고, 제거 사유별 건수를 집계한다.

예시 품질 지표:

- 필수 컬럼 결측 건수
- 하차 시각이 승차 시각보다 빠르거나 같은 건수
- 이동 거리 0 이하 건수
- 음수 운임 건수
- 최대 이동 시간 초과 건수
- 최대 이동 거리 초과 건수

이를 통해 파이프라인은 정상 종료되었지만 실제 데이터가 잘못 처리되는 Silent Failure를 방지한다.

## 10. 실행 방법

```bash
spark-submit src/main.py
```

실제 명령어와 실행 환경은 구현 완료 후 추가한다.

## 11. 실행 결과

구현 완료 후 다음 내용을 기록한다.

```text
Input path        :
Partition count   :
Raw row count     :
Valid trip count  :
Invalid trip count:
Total revenue     :
Average distance  :
```

## 12. Spark UI 캡처

다음 화면을 `screenshots/spark_ui/`에 저장하고 README에 첨부한다.

1. Jobs 화면
2. Stage 상세 화면
3. DAG Visualization
4. SQL 실행 계획 화면

```markdown
![Spark DAG](screenshots/spark_ui/dag.png)
```

## 13. 결과 해석

구현 완료 후 아래 내용을 중심으로 정리한다.

- 어떤 Transformation이 같은 Stage에서 실행되었는가?
- 어느 연산에서 Shuffle이 발생했는가?
- Action을 실행하기 전에는 왜 Job이 만들어지지 않았는가?
- 캐시 적용 전후 반복 Action의 실행 차이는 무엇인가?
- 필터를 집계 전에 적용한 것이 DAG와 처리량에 어떤 영향을 주었는가?

## 14. 과제 체크리스트

- [ ] TLC Parquet 데이터를 Spark DataFrame으로 읽었다.
- [ ] 스키마와 파티션 수를 확인했다.
- [ ] 필수 컬럼의 결측값을 처리했다.
- [ ] 비현실적인 운행 값을 필터링했다.
- [ ] 최소 세 종류 이상의 Transformation을 적용했다.
- [ ] 최소 두 종류 이상의 Action을 실행했다.
- [ ] 정제 결과와 집계 결과를 파일로 저장했다.
- [ ] `explain()`으로 실행 계획을 확인했다.
- [ ] Spark UI에서 DAG와 Stage를 확인했다.
- [ ] 반복 사용하는 DataFrame에 캐시 또는 persist를 적용했다.
- [ ] 무효 데이터 사유별 건수를 기록했다.
- [ ] Spark UI 화면을 README에 첨부했다.
