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


