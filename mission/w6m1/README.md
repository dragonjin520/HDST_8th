# 과제 진행 단계

## 1. 요구사항 분석 및 처리 기준 정의

과제에서 요구하는 입력 데이터, 처리 기간, 최종 결과 테이블과 품질 검증 조건을 먼저 정리한다.

- 수집 대상 API: `tbCycleRentUseDayInfo`
- 수집 기간: `2026-06-27` ~ `2026-06-28`
- 실행 단위: 두 날짜를 하나의 Airflow DAG Run에서 처리
- 최종 테이블: `station_period_usage`
- 집계 단위: 대여소별 1행
- 중복 방지 기준: 동일 기간과 동일 대여소 데이터는 재실행하더라도 중복 생성되지 않아야 함

이 단계에서 결측값, 숫자 변환 실패, 음수 값, API 오류를 어떻게 처리할지 팀 기준을 확정한다.

## 2. 프로젝트 구조 및 환경 구성

Airflow와 MySQL을 Docker Compose로 실행할 수 있도록 프로젝트 구조와 환경 변수를 구성한다.

예상 프로젝트 구조는 다음과 같다.

```text
w6m1/
├── dags/
│   └── station_period_usage_dag.py
├── src/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── sql/
│   └── create_station_period_usage.sql
├── logs/
├── .env
├── docker-compose.yml
├── requirements.txt
└── README.md
```

주요 설정값은 코드에 직접 작성하지 않고 `.env` 또는 Airflow Variable/Connection으로 분리한다.

- 서울시 OpenAPI 인증키
- API 기본 URL
- 요청 시작·종료 인덱스
- 요청 타임아웃과 재시도 횟수
- MySQL 접속 정보
- 처리 시작일과 종료일

## 3. MySQL 테이블 설계

최종 결과를 저장할 `station_period_usage` 테이블을 생성한다.

필수 컬럼은 다음과 같다.

- `period_start_date`
- `period_end_date`
- `station_id`
- `station_name`
- `total_usage_count`
- `total_distance_m`
- `total_duration_min`

동일 기간과 동일 대여소의 중복 적재를 방지하기 위해 다음 컬럼 조합을 기본키 또는 유니크 키로 설정한다.

```text
(period_start_date, period_end_date, station_id)
```

재실행 시에는 `INSERT ... ON DUPLICATE KEY UPDATE`, 기간 단위 삭제 후 재적재, 또는 staging table 교체 방식 중 하나를 선택해 멱등성을 보장한다.

## 4. Airflow DAG 설계

전체 ETL 흐름을 Airflow Task 단위로 나눈다.

```text
start
  → create_table
  → extract_2026_06_27
  → extract_2026_06_28
  → validate_raw_data
  → transform_and_aggregate
  → validate_aggregated_data
  → load_to_mysql
  → verify_loaded_data
  → end
```

두 날짜의 수집 Task는 병렬 실행할 수 있지만, 두 날짜의 수집이 모두 완료된 후 정제와 집계를 수행하도록 의존성을 설정한다.

DAG에는 다음 실행 정책을 적용한다.

- Task 재시도 횟수와 재시도 간격 설정
- 실패 로그 기록
- 과거 스케줄 자동 실행 여부 결정
- 동일 실행일 중복 실행 시 결과 충돌 방지
- 중간 산출물의 실행 단위별 경로 또는 이름 분리

## 5. API 데이터 수집 구현

`tbCycleRentUseDayInfo` API를 호출하여 두 날짜의 전체 데이터를 수집한다.

각 날짜에 대해 다음 과정을 수행한다.

1. 시작 인덱스와 종료 인덱스를 지정해 첫 페이지를 요청한다.
2. API가 반환한 전체 건수를 확인한다.
3. 페이지 크기만큼 인덱스를 증가시키며 모든 페이지를 반복 요청한다.
4. HTTP 상태 코드와 OpenAPI 응답 코드를 확인한다.
5. 타임아웃이나 일시적 네트워크 오류 발생 시 재시도한다.
6. 날짜별 원본 데이터를 중간 파일 또는 staging 영역에 저장한다.

수집 단계에서는 다음 정보를 로그로 남긴다.

- 요청 날짜
- 페이지 범위
- 페이지별 수집 건수
- 날짜별 총 수집 건수
- 재시도 횟수
- 실패한 요청과 오류 메시지

## 6. 원본 데이터 품질 검사

수집이 끝나면 원본 데이터에 대한 기본 품질 검사를 수행한다.

- 두 날짜의 데이터가 모두 존재하는지 확인
- API 전체 건수와 실제 수집 건수 비교
- 필수 컬럼 존재 여부 확인
- 대여소 ID와 대여소명 결측 건수 확인
- `USE_CNT`, `MOVE_METER`, `MOVE_TIME`의 숫자 변환 가능 여부 확인
- 음수 값 존재 여부 확인
- 완전히 동일한 원본 행의 중복 여부 확인

품질 검사에 실패한 경우 후속 Task가 실행되지 않도록 예외를 발생시킨다.

## 7. 데이터 정제

두 날짜의 데이터를 하나로 결합한 뒤 사전에 정의한 규칙에 따라 정제한다.

기본 정제 규칙은 다음과 같다.

- `station_id` 또는 `station_name`이 없는 행은 집계 대상에서 제외
- `USE_CNT`, `MOVE_METER`, `MOVE_TIME`은 숫자형으로 변환
- 숫자 변환에 실패한 행은 제외하거나 오류 테이블에 별도 저장
- 사용 건수, 이동 거리, 이용 시간이 음수인 행은 제외
- 문자열 앞뒤 공백과 불필요한 형식을 정리
- 동일한 원본 행이 중복 수집된 경우 중복 제거

정제 결과에는 다음 건수를 로그로 남긴다.

- 전체 입력 행 수
- 정상 처리 행 수
- 필수값 결측 제외 건수
- 숫자 변환 실패 건수
- 음수 값 제외 건수
- 중복 제거 건수

## 8. 대여소별 집계

정상 데이터만 사용하여 두 날짜 기간의 대여소별 이용량을 집계한다.

집계 기준은 다음과 같다.

```text
GROUP BY station_id, station_name
```

집계 컬럼은 다음과 같이 계산한다.

- `total_usage_count`: `USE_CNT` 합계
- `total_distance_m`: `MOVE_METER` 합계
- `total_duration_min`: `MOVE_TIME` 합계
- `period_start_date`: `2026-06-27`
- `period_end_date`: `2026-06-28`

집계 결과는 대여소별로 한 행만 존재해야 한다.

## 9. 집계 결과 품질 검사

MySQL 적재 전에 최종 집계 데이터의 품질을 검증한다.

- 결과 행 수가 0보다 큰지 확인
- `station_id` 기준 중복이 없는지 확인
- 기간 시작일과 종료일이 모든 행에서 동일한지 확인
- 합계 컬럼에 NULL 또는 음수가 없는지 확인
- 정제 데이터의 총합과 집계 데이터의 총합이 일치하는지 확인

예를 들어 다음 값을 비교한다.

```text
정제 데이터 USE_CNT 합계 = 집계 결과 total_usage_count 합계
정제 데이터 MOVE_METER 합계 = 집계 결과 total_distance_m 합계
정제 데이터 MOVE_TIME 합계 = 집계 결과 total_duration_min 합계
```

## 10. MySQL 적재 및 멱등성 검증

검증을 통과한 집계 데이터를 `station_period_usage` 테이블에 적재한다.

동일한 처리 기간으로 DAG를 다시 실행한 뒤 다음을 확인한다.

- 테이블 행 수가 불필요하게 증가하지 않는지
- 동일 기간·동일 대여소 데이터가 중복되지 않는지
- 기존 결과가 동일한 값으로 유지 또는 갱신되는지
- 중간 파일이나 staging 데이터가 충돌하지 않는지

최종적으로 동일 기간을 두 번 실행해도 결과가 동일해야 한다.

## 11. 최종 결과 확인

SQL 쿼리로 적재 결과를 확인한다.

```sql
SELECT *
FROM station_period_usage
WHERE period_start_date = '2026-06-27'
  AND period_end_date = '2026-06-28'
ORDER BY total_usage_count DESC
LIMIT 20;
```

추가로 전체 행 수와 합계를 확인한다.

```sql
SELECT
    COUNT(*) AS station_count,
    SUM(total_usage_count) AS total_usage_count,
    SUM(total_distance_m) AS total_distance_m,
    SUM(total_duration_min) AS total_duration_min
FROM station_period_usage
WHERE period_start_date = '2026-06-27'
  AND period_end_date = '2026-06-28';
```

## 12. 제출 자료 정리

과제 제출 전 다음 항목을 확인한다.

- Airflow DAG 및 ETL 소스 코드
- Docker Compose와 환경 설정 예시
- MySQL 테이블 생성 SQL
- 설치 및 실행 방법
- 데이터 정제·제외 규칙
- 재시도와 오류 처리 방식
- 멱등성 보장 방식
- 데이터 품질 검사 내용
- 성공한 Airflow DAG 실행 화면
- 최종 SQL 조회 쿼리와 실행 결과
- 팀에서 추가 구현하기로 결정한 기능과 결정 근거

# 권장 구현 순서

실제 개발은 다음 순서로 진행한다.

```text
1. Docker Compose로 Airflow와 MySQL 실행
2. MySQL 연결 및 결과 테이블 생성 확인
3. 하루·한 페이지 기준 API 호출 테스트
4. 전체 페이지 수집 로직 구현
5. 날짜별 수집 Task 구현
6. 정제 및 제외 건수 로그 구현
7. 대여소별 집계 구현
8. MySQL 멱등 적재 구현
9. 원본·집계·적재 품질 검사 구현
10. 동일 기간 DAG 재실행 테스트
11. Airflow 실행 화면과 SQL 결과 캡처
12. README와 팀 Wiki 최종 정리
```



# W6M1 서울시 공공자전거 이용정보 ETL 파이프라인

서울시 OpenAPI의 `tbCycleRentUseDayInfo` 데이터를 날짜별로 수집하고, Raw/Silver/Reject 계층으로 정제한 뒤 대여소별 기간 집계 결과를 MySQL에 적재하는 Airflow 기반 ETL 파이프라인이다.

## 1. 과제 목표

- 수집 대상 API: `tbCycleRentUseDayInfo`
- 수집 기간: `2026-06-27` ~ `2026-06-28`
- 실행 방식: Airflow 수동 DAG 실행
- 수집 단위: 날짜별 병렬 처리
- 최종 집계 단위: 대여소별 1행
- 최종 테이블: `station_period_usage`
- 핵심 목표:
  - API 전체 페이지 수집
  - 정제 규칙 적용
  - 오류 데이터 분리
  - Raw/Silver/Reject 건수 정합성 검증
  - 대여소별 기간 집계
  - 동일 기간 재실행 시 중복 방지

## 2. 프로젝트 구조

```text
w6m1/
├── dags/
│   └── bike_usage_pipeline.py
├── src/
│   ├── __init__.py
│   ├── api_client.py
│   ├── extract.py
│   ├── load.py
│   ├── transform.py
│   ├── aggregate.py
│   ├── quality.py
│   └── notification.py
├── config/
│   └── config.json
├── sql/
├── data/
│   └── raw/
├── logs/
├── .env
├── compose.yml
├── requirements.txt
└── README.md
```

## 3. 전체 파이프라인 구조

```text
collect_20260627 ─→ load_raw_20260627 ┐
                                      ├→ transform_all_dates
collect_20260628 ─→ load_raw_20260628 ┘
                                      ↓
                             aggregate_all_dates
                                      ↓
                    ┌─────────────────┼─────────────────┐
                    ↓                 ↓                 ↓
             quality_sample     quality_count     quality_schema
```

처리 순서는 다음과 같다.

1. 날짜별 API 전체 페이지 수집
2. 날짜별 Raw 데이터 적재
3. 두 날짜 Raw 데이터 통합 정제
4. 정상 데이터는 Silver, 오류 데이터는 Reject에 저장
5. 대여소별 기간 집계
6. Gold 테이블 적재
7. 표본·건수·스키마 품질검사 병렬 수행

## 4. 실행 환경

- Airflow 3.3
- Python 3.13
- MySQL
- PostgreSQL Airflow Metadata DB
- Docker Compose
- LocalExecutor

주요 Airflow 서비스는 다음과 같다.

- `airflow-api-server`
- `airflow-scheduler`
- `airflow-dag-processor`
- `mysql`
- `postgres`

## 5. 환경 변수와 설정

비밀값은 `.env`에 저장하고, 일반 실행 설정은 `config/config.json`에서 관리한다.

### `.env`

```env
SEOUL_API_KEY=...
MYSQL_DATABASE=bike_db
MYSQL_USER=bike_user
MYSQL_PASSWORD=...
AIRFLOW_JWT_SECRET=...
```

실제 비밀값은 저장소에 커밋하지 않는다.

### Airflow 내부 통신 설정

Airflow 3에서는 Scheduler의 Task Runner가 API Server의 Execution API에 접근해야 한다.

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-api-server:8080/execution/
AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET}
```

Scheduler와 API Server는 동일한 JWT Secret을 사용해야 한다.

## 6. API 수집

### 요청 기준

- 요청 서비스명: `tbCycleRentUseDayInfo`
- 응답 데이터 키: `cycleRentUseDayInfo`
- 페이지 크기: 1,000건
- HTTP 타임아웃: 15초
- 페이지 요청 재시도: 최초 요청 포함 최대 3회
- 재시도 간격: 5초 고정

### 검증 항목

HTTP 상태가 200이어도 다음 항목을 추가 검증한다.

- 응답 JSON 파싱 가능 여부
- 서비스 응답 키 존재 여부
- `RESULT.CODE == INFO-000`
- `list_total_count` 존재 여부
- 페이지별 `row` 존재 여부
- 최종 수집 건수와 `list_total_count` 일치 여부

### 수집 결과

| 날짜 | API 전체 건수 | 실제 수집 건수 | 결과 |
|---|---:|---:|---|
| 2026-06-27 | 68,670 | 68,670 | 정상 |
| 2026-06-28 | 63,630 | 63,630 | 정상 |

## 7. Raw 데이터 적재

Raw 데이터는 API 응답을 최대한 원형에 가깝게 저장한다.

주요 컬럼:

- `raw_id`
- `dag_run_id`
- `source_date`
- `page_start_index`
- `page_end_index`
- `row_number_in_page`
- `rent_dt`
- `rent_id`
- `rent_nm`
- `rent_type`
- `gender_cd`
- `age_type`
- `use_cnt`
- `exer_amt`
- `carbon_amt`
- `move_meter`
- `move_time`
- `raw_record`
- `collected_at`

Raw는 DAG 실행 이력을 보존하기 위해 `dag_run_id`별로 저장한다. 따라서 동일 날짜를 다른 DAG Run에서 재수집하면 전체 테이블 기준 건수는 증가할 수 있다. 품질검사는 반드시 특정 `dag_run_id` 기준으로 수행한다.

## 8. 데이터 정제 규칙

### 필수 컬럼

다음 컬럼이 없거나 유효하지 않으면 해당 행 전체를 Silver에서 제외하고 Reject에 저장한다.

- `RENT_DT`
- `RENT_ID`
- `RENT_TYPE`
- `GENDER_CD`
- `AGE_TYPE`

추가 규칙:

- `RENT_ID = '0'`은 유효하지 않은 값으로 처리
- `00746`과 같은 선행 0이 포함된 대여소 ID는 유효
- `GENDER_CD`는 대문자로 정규화

### 측정값 컬럼

다음 값은 숫자 변환 실패 또는 음수일 경우 행 전체를 제외하지 않고 해당 값만 `NULL`로 저장한다.

- `USE_CNT`
- `EXER_AMT`
- `CARBON_AMT`
- `MOVE_METER`
- `MOVE_TIME`

0은 유효한 값으로 처리한다.

### Silver와 Reject 저장 정책

- 정상 행: `bike_usage_silver`
- 제외 또는 보정 사유: `bike_usage_reject`
- Silver는 `dag_run_id + raw_id` 기준으로 동일 실행 내 중복을 방지
- Reject에는 오류 컬럼, 오류 값, 오류 사유, 원본 JSON을 기록

## 9. 정제 결과

성공한 DAG Run:

```text
manual__2026-08-06T12:51:36.122214+00:00
```

날짜별 결과:

| 날짜 | Raw | Silver | Reject | 차이 |
|---|---:|---:|---:|---:|
| 2026-06-27 | 68,670 | 49,400 | 19,270 | 0 |
| 2026-06-28 | 63,630 | 45,762 | 17,868 | 0 |
| 합계 | 132,300 | 95,162 | 37,138 | 0 |

정합성 관계:

```text
Raw = Silver + Reject
```

두 날짜 모두 차이가 0이므로 정제 과정에서 유실되거나 중복된 행이 없다.

이번 데이터에서 제외된 37,138건은 모두 `GENDER_CD` 결측에 해당했다.

## 10. 대여소별 기간 집계

Silver 데이터를 기준으로 `2026-06-27`부터 `2026-06-28`까지 대여소별 이용량을 집계한다.

집계 기준:

```text
GROUP BY rent_id
```

주요 결과 컬럼:

- `period_start_date`
- `period_end_date`
- `station_id`
- `station_name`
- `total_usage_count`
- `total_distance_m`
- `total_duration_min`
- `usage_valid_row_count`
- `distance_valid_row_count`
- `duration_valid_row_count`
- `source_dag_run_id`

동일 기간과 대여소의 중복을 막기 위해 다음 복합키를 사용한다.

```text
(period_start_date, period_end_date, station_id)
```

## 11. 멱등성 보장

집계는 staging table을 이용한다.

```text
Silver 집계
→ station_period_usage_staging 적재
→ 대상 기간 Gold 데이터 삭제
→ staging 결과를 Gold에 삽입
```

동일 기간으로 재실행해도 `station_period_usage`의 결과 행 수가 증가하지 않으며 최신 집계 결과로 교체된다.

검증 결과:

```text
첫 실행: 2,737행 삽입
재실행: 기존 2,737행 삭제 후 2,737행 삽입
최종 행 수: 2,737행
```

## 12. 품질검사

### 12.1 표본 재검증

Silver 데이터의 5%를 표본 추출해 정제 규칙 위반 여부를 다시 검사했다.

```text
전체 Silver: 95,162행
표본: 4,759행
위반: 0행
결과: 통과
```

### 12.2 건수 정합성

날짜별로 다음 관계를 검사했다.

```text
Raw = Silver + Reject
```

두 날짜 모두 차이 0으로 통과했다.

### 12.3 스키마 검증

다음 테이블의 컬럼명과 타입을 기대 스키마와 비교했다.

- `bike_usage_silver`
- `station_period_usage`

검증 결과 모두 통과했다.

## 13. 최종 Gold 결과

최종 집계 대여소 수:

```text
2,737개
```

상위 대여소 예시:

| 순위 | 대여소 ID | 대여소명 | 총 이용 건수 |
|---:|---|---|---:|
| 1 | 05515 | 5515.한강버스 망원 선착장 | 875 |
| 2 | 05891 | 5891.한강버스 여의도 선착장 | 649 |
| 3 | 05153 | 5153.한강버스 뚝섬 선착장 | 617 |
| 4 | 02715 | 2715.마곡나루역 2번 출구 | 606 |
| 5 | 05651 | 5651.한강버스 옥수 선착장 | 539 |

확인 SQL:

```sql
SELECT
    station_id,
    station_name,
    total_usage_count,
    total_distance_m,
    total_duration_min
FROM station_period_usage
WHERE period_start_date = '2026-06-27'
  AND period_end_date = '2026-06-28'
ORDER BY total_usage_count DESC
LIMIT 10;
```

## 14. Airflow 실행 결과

성공한 DAG Run의 모든 Task가 `success`로 종료됐다.

```text
collect_20260627       success
collect_20260628       success
load_raw_20260627      success
load_raw_20260628      success
transform_all_dates    success
aggregate_all_dates    success
quality_sample         success
quality_count          success
quality_schema         success
```

Airflow 실행 정책:

- 수동 실행
- `catchup=False`
- `max_active_runs=1`
- Task 재시도 3회
- 지수 백오프 1분 → 2분 → 4분
- 최대 재시도 간격 4분
- 수집·정제 Task timeout 15분
- 적재·집계·품질검사 Task timeout 5분

## 15. 주요 트러블슈팅

### 15.1 DAG가 목록에 나타나지 않음

증상:

```text
DAG IDs: []
Import errors: {}
```

원인:

- DAG 파일이 Airflow 안전 검색 대상에서 제외
- 생성된 DAG 객체가 전역 변수에 명확히 할당되지 않음

해결:

```python
# Airflow DAG: 서울 따릉이 이용정보 ETL 파이프라인
bike_usage_pipeline_dag = bike_usage_pipeline()
```

### 15.2 Task 실행 전 Connection refused

증상:

```text
httpx.ConnectError: [Errno 111] Connection refused
```

원인:

- Scheduler의 Task Runner가 기본 `localhost:8080`으로 Execution API에 접근
- Docker 환경에서는 API Server 서비스명으로 접근해야 함

해결:

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-api-server:8080/execution/
```

### 15.3 Invalid auth token

증상:

```text
airflow.sdk.api.client.ServerResponseError: Invalid auth token
```

원인:

- Scheduler와 API Server에 공통 JWT Secret 미설정

해결:

```yaml
AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET}
```

### 15.4 새 DAG Run이 queued 상태로 대기

원인:

- `max_active_runs=1`
- 이전 실패 Run이 `running` 상태로 남아 있음

해결:

- 이전 Run을 UI에서 `failed` 처리
- 이후 대기 중인 새 Run이 실행됨

### 15.5 MySQL 한글 출력 깨짐

증상:

```text
station_name = ????
```

해결:

```sql
SET NAMES utf8mb4;
```

또는 접속 시 문자셋을 지정한다.

```bash
docker compose exec mysql \
  mysql --default-character-set=utf8mb4 -u bike_user -p bike_db
```

## 16. 실행 방법

### Docker Compose 실행

```bash
docker compose up -d
```

### DAG 목록 확인

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list
```

### DAG 활성화

```bash
docker compose exec -T airflow-scheduler \
  airflow dags unpause -y bike_usage_pipeline
```

### DAG 수동 실행

```bash
docker compose exec -T airflow-scheduler \
  airflow dags trigger bike_usage_pipeline
```

### DAG Run 확인

```bash
docker compose exec -T airflow-scheduler \
  airflow dags list-runs bike_usage_pipeline \
  --output table
```

### Task 상태 확인

```bash
docker compose exec -T airflow-scheduler \
  airflow tasks states-for-dag-run \
  bike_usage_pipeline \
  "<실제_run_id>"
```

`<실제_run_id>`는 예시 문자열이므로 실제 Run ID로 교체해야 한다.

## 17. 최종 결과

이번 과제를 통해 다음을 구현하고 검증했다.

- 서울시 OpenAPI 전체 페이지 수집
- 날짜별 병렬 수집과 Raw 적재
- 필수값 및 측정값 정제 규칙 적용
- Silver/Reject 분리 저장
- Raw/Silver/Reject 건수 정합성 보장
- 대여소별 기간 집계
- staging 기반 멱등 적재
- 표본·건수·스키마 품질검사
- Airflow 3 Execution API 및 JWT 기반 내부 통신 설정

최종적으로 `2026-06-27`부터 `2026-06-28`까지 132,300건의 원본 데이터를 처리해 95,162건의 Silver 데이터와 37,138건의 Reject 데이터를 생성하고, 2,737개 대여소의 기간 집계 결과를 Gold 테이블에 적재했다.