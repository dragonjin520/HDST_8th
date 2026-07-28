# 서울 따릉이 수요 분석 및 예측 시스템 아키텍처

## 1. 시스템 목표

서울시 따릉이 대여이력, 실시간 인구, 날씨, 대여소 정보를 결합하여 대여소별 단기 수요를 예측하고, 자전거 부족 위험과 재배치 우선순위를 운영자에게 제공한다.

이 프로젝트는 다음 전체 흐름을 구현하는 것을 목표로 한다.

```text
데이터 수집
→ Raw 저장
→ 데이터 검증
→ Spark 정제 및 조인
→ 분석용 데이터 생성
→ 수요 예측
→ 서빙 DB 적재
→ 대시보드 및 알림 제공
```

---

## 2. 전체 아키텍처

```text
[데이터 소스]
 ├─ 서울시 따릉이 대여이력 CSV
 │   └─ 월별 배치 데이터
 ├─ 서울시 실시간 도시데이터 API
 │   └─ 장소별 실시간 인구 및 혼잡도
 ├─ 기상청 또는 날씨 API
 │   └─ 기온, 강수량, 풍속, 습도
 ├─ 따릉이 대여소 정보
 │   └─ 대여소 위치, 수용량
 └─ 선택: 실시간 대여소 자전거 현황 API
     └─ 대여 가능 자전거 수
                    │
                    ▼
[수집 및 오케스트레이션]
 ├─ Apache Airflow
 │   ├─ 대여이력 수집 DAG
 │   ├─ 실시간 인구 수집 DAG
 │   ├─ 날씨 수집 DAG
 │   ├─ Spark 가공 DAG
 │   ├─ 데이터 품질 검사 DAG
 │   └─ 서빙 테이블 갱신 DAG
 ├─ Airflow Scheduler 또는 EventBridge
 └─ Lambda 또는 Python Collector
                    │
                    ▼
[Raw 저장 계층]
 Amazon S3
 ├─ raw/bike_rental/
 ├─ raw/city_population/
 ├─ raw/weather/
 ├─ raw/station/
 └─ quarantine/
                    │
                    ▼
[정제 및 통합 계층]
 Apache Spark on EMR 또는 EC2
 ├─ 스키마 검증
 ├─ 중복 제거
 ├─ 결측치 및 이상치 처리
 ├─ 시간 단위 통일
 ├─ 좌표 기반 대여소-도시지역 매핑
 ├─ 날씨·인구·대여이력 조인
 └─ 수요 예측용 파생 변수 생성
                    │
                    ▼
[Processed 저장 계층]
 Amazon S3 Parquet
 ├─ silver/bike_rental/
 ├─ silver/population/
 ├─ silver/weather/
 ├─ silver/station/
 └─ gold/bike_demand_features/
                    │
                    ▼
[분석 및 예측]
 Spark ML 또는 Python 모델
 ├─ 시간대별 수요 분석
 ├─ 요일별 수요 분석
 ├─ 날씨별 이용량 분석
 ├─ 실시간 인구와 이용량 관계 분석
 ├─ 대여소별 단기 수요 예측
 └─ 자전거 부족 및 과잉 위험 계산
                    │
                    ▼
[서빙 계층]
 Amazon Redshift 또는 PostgreSQL
 ├─ 시간대별 수요 집계
 ├─ 대여소별 예상 수요
 ├─ 부족 위험 대여소
 ├─ 날씨·인구 영향 분석
 └─ 모델 예측 결과
                    │
                    ▼
[사용자 제공]
 Streamlit 대시보드
 ├─ 지도 기반 대여소 현황
 ├─ 시간대별 예상 수요
 ├─ 부족 위험 대여소 순위
 ├─ 수요 영향 요인 분석
 └─ 운영자 재배치 추천
                    │
                    ▼
[모니터링 및 알림]
 CloudWatch + SNS 또는 Slack
 ├─ 데이터 수집 실패
 ├─ 데이터 지연
 ├─ 데이터 품질 검사 실패
 ├─ Spark Job 실패
 └─ 수요 급증 및 자전거 부족 경고
```

---

## 3. 계층별 구성

| 계층 | 주요 기술 | 역할 | 주요 산출물 |
|---|---|---|---|
| 데이터 소스 | 서울 열린데이터광장, 날씨 API | 대여이력, 인구, 날씨, 대여소 데이터 제공 | CSV, JSON |
| 수집 | Airflow, Python, Lambda | 배치 및 주기적 API 데이터 수집 | 원본 파일 |
| Raw 저장 | Amazon S3 | 원본 데이터를 변경하지 않고 보존 | Raw CSV, JSON |
| 정제 | Spark on EMR | 대규모 정제, 중복 제거, 타입 변환 | 정제 Parquet |
| 통합 | Spark | 시간 및 공간 기준으로 데이터 조인 | 수요 분석 데이터 |
| 분석·예측 | Spark ML, Python | 수요 패턴 분석 및 예측 | 대여소별 예상 수요 |
| 서빙 | Redshift 또는 PostgreSQL | 대시보드 조회용 집계 데이터 제공 | Fact, Dimension 테이블 |
| 시각화 | Streamlit | 운영자에게 분석 결과 제공 | 수요 모니터링 화면 |
| 운영 | Airflow, CloudWatch, SNS | 재시도, 로그, 품질 검사, 장애 알림 | 실행 기록, 경고 |

---

## 4. 데이터 처리 흐름

### 4.1 과거 데이터 처리

```text
따릉이 월별 대여이력
→ S3 Raw 적재
→ Spark 정제
→ 시간대·요일·대여소별 이용량 집계
→ 날씨 데이터 조인
→ 실시간 인구 데이터의 지역별 패턴 조인
→ 학습용 Feature 데이터 생성
→ 수요 예측 모델 학습
→ 모델 및 평가 결과 저장
```

### 4.2 실시간에 가까운 예측 처리

```text
실시간 인구 API + 최신 날씨 API
→ 5~10분 주기 수집
→ S3 Raw 저장
→ 최신 데이터 정제
→ 대여소 인근 인구·날씨 Feature 생성
→ 학습된 모델로 향후 수요 예측
→ Redshift 또는 PostgreSQL 갱신
→ Streamlit 대시보드 표시
→ 부족 위험이 높으면 Slack 또는 SNS 알림
```

현재 프로젝트에서는 초 단위 스트리밍보다 5~10분 단위 마이크로 배치 방식이 적합하다. 실시간 인구와 날씨 데이터의 갱신 주기보다 빠르게 Kafka 스트리밍을 구성해도 얻는 효과가 크지 않기 때문이다.

---

## 5. Airflow DAG 구조

```text
start
  │
  ├─ collect_bike_history
  ├─ collect_station_info
  ├─ collect_weather
  └─ collect_population
          │
          ▼
    validate_raw_data
          │
     ┌────┴────┐
     │         │
 정상 데이터   오류 데이터
     │         └─ quarantine_to_s3
     ▼
 run_spark_cleaning
          │
          ▼
 run_spatial_temporal_join
          │
          ▼
 create_feature_dataset
          │
     ┌────┴─────────┐
     │              │
 모델 재학습일       일반 실행일
     │              │
 train_model         load_latest_model
     └──────┬───────┘
            ▼
     predict_demand
            │
            ▼
     quality_check_result
            │
            ▼
     load_serving_tables
            │
            ▼
     refresh_dashboard
            │
            ▼
        send_status
```

### DAG 실행 주기

| DAG | 실행 주기 | 설명 |
|---|---:|---|
| `bike_history_ingestion` | 월 1회 | 월별 대여이력 파일 적재 |
| `station_sync` | 일 1회 | 대여소 위치 및 수용량 갱신 |
| `population_ingestion` | 5~10분 | 장소별 실시간 인구 수집 |
| `weather_ingestion` | 10분~1시간 | 관측 및 예보 날씨 수집 |
| `demand_prediction` | 10분 | 최신 Feature 생성 및 예측 |
| `model_training` | 주 1회 | 누적 데이터를 이용한 모델 재학습 |
| `data_quality_check` | 각 파이프라인 종료 후 | 누락, 중복, 지연, 이상치 검사 |

---

## 6. S3 저장 구조

```text
s3://bike-demand-project/
├── raw/
│   ├── bike_rental/year=2026/month=05/
│   ├── population/date=2026-07-28/hour=15/
│   ├── weather/date=2026-07-28/hour=15/
│   └── station/snapshot_date=2026-07-28/
│
├── silver/
│   ├── bike_rental/
│   ├── population/
│   ├── weather/
│   └── station/
│
├── gold/
│   ├── station_hourly_demand/
│   ├── demand_features/
│   ├── demand_predictions/
│   └── relocation_recommendations/
│
├── model/
│   ├── demand_prediction/
│   └── evaluation/
│
└── quarantine/
    ├── schema_error/
    ├── duplicate/
    └── invalid_value/
```

- Raw 계층은 원본 복구와 재처리를 위해 수정하지 않고 보존한다.
- Silver 계층은 데이터별 정제 결과를 저장한다.
- Gold 계층은 분석, 예측, 서비스 목적의 통합 데이터를 저장한다.
- Quarantine 계층은 스키마 오류, 유효하지 않은 값, 처리 실패 데이터를 분리한다.

---

## 7. 분석용 데이터 모델

### 7.1 Fact 테이블

| 테이블 | Grain | 주요 컬럼 |
|---|---|---|
| `fact_bike_rental` | 대여 1건 | 대여시각, 반납시각, 대여소, 반납소, 이용시간 |
| `fact_station_hourly_demand` | 대여소·1시간 | 대여건수, 반납건수, 순수요, 자전거 잔여량 |
| `fact_demand_prediction` | 대여소·예측시각 | 예측 수요, 실제 수요, 부족 위험도 |
| `fact_population_observation` | 지역·관측시각 | 실시간 인구, 혼잡도 |
| `fact_weather_observation` | 지역·관측시각 | 기온, 강수량, 풍속, 습도 |

### 7.2 Dimension 테이블

| 테이블 | 내용 |
|---|---|
| `dim_station` | 대여소 ID, 이름, 위도, 경도, 수용량 |
| `dim_area` | 도시데이터 측정 지역, 행정구역 |
| `dim_datetime` | 일자, 시간, 요일, 주말 여부, 공휴일 여부 |
| `dim_weather_condition` | 맑음, 비, 눈 등 날씨 분류 |
| `dim_model_version` | 모델 버전, 학습일, 성능 지표 |

### 7.3 핵심 통합 테이블

`gold_station_demand_feature`

| 컬럼 | 설명 |
|---|---|
| 기준시각 | Feature가 생성된 기준 시간 |
| 대여소 ID | 예측 대상 대여소 |
| 지역 ID | 실시간 인구 측정 지역 |
| 최근 실시간 인구 | 최근 수집된 지역 인구 |
| 인구 혼잡도 | 여유, 보통, 붐빔 등 혼잡 상태 |
| 기온 | 기준시각 기온 |
| 강수량 | 기준시각 강수량 |
| 풍속 | 기준시각 풍속 |
| 습도 | 기준시각 습도 |
| 시간 | 0~23시 |
| 요일 | 월요일~일요일 |
| 주말 여부 | 주말 구분 |
| 공휴일 여부 | 공휴일 구분 |
| 이전 1시간 대여량 | 직전 1시간 대여건수 |
| 이전 24시간 동일 시간대 대여량 | 전일 동일 시간대 대여건수 |
| 대여소 수용량 | 거치 가능한 자전거 수 |
| 현재 자전거 수 | 현재 대여 가능한 자전거 수 |
| 예측 대상 수요 | 향후 30분 또는 1시간 대여 수요 |

---

## 8. 데이터 조인 기준

| 데이터 조합 | 조인 기준 | 처리 방법 |
|---|---|---|
| 대여이력 ↔ 대여소 | 대여소 ID | 일반 Key Join |
| 대여소 ↔ 실시간 인구 지역 | 위도·경도 | 지역 Polygon 또는 최근접 지역 매핑 |
| 대여이력 ↔ 날씨 | 지역 + 시간 | 시간 단위 또는 최근 관측값 기준 조인 |
| 대여이력 ↔ 날짜 | 날짜·시간 | 날짜 Dimension Join |
| 실시간 입력 ↔ 학습 Feature | 대여소 + 기준시각 | 학습 데이터와 동일한 Feature 구조로 변환 |

실시간 인구 데이터가 일부 주요 장소에 대해서만 제공되는 경우에는 모든 대여소에 억지로 연결하지 않는다.

초기 MVP에서는 다음 기준을 사용한다.

```text
실시간 인구 측정 지역 내부에 있거나
해당 지역 중심점에서 일정 거리 이내에 있는 대여소만 분석 대상으로 포함한다.
```

이후 행정동, 상권 또는 격자 단위로 확장할 수 있다.

---

## 9. 데이터 품질 및 장애 대응

| 검사 항목 | 검사 기준 | 실패 처리 |
|---|---|---|
| 수집 지연 | 예정 시각보다 일정 시간 이상 지연 | 재시도 후 알림 |
| 중복 | API 응답 ID, 지역, 관측시각 중복 | 최신 또는 최초 1건 유지 |
| 필수값 누락 | 대여소 ID, 시각, 인구수 등 NULL | Quarantine 분리 |
| 범위 오류 | 인구수 음수, 습도 100% 초과 등 | 오류 데이터 분리 |
| 시간 오류 | 반납시각이 대여시각보다 빠름 | 분석 대상에서 제외 |
| 위치 오류 | 서울 외 좌표 | Quarantine 분리 |
| 스키마 변경 | 예상하지 않은 컬럼 추가 또는 삭제 | DAG 실패 및 알림 |
| 데이터 급감 | 평소 대비 수집량 급감 | 경고 후 이전 데이터 사용 여부 판단 |
| 예측 결과 오류 | 음수 수요 예측 | 0으로 보정하고 로그 기록 |

Airflow에서는 다음 운영 기능을 적용한다.

- Task별 재시도 횟수 및 대기시간 설정
- 실행 날짜를 기준으로 한 멱등성 보장
- 재처리 시 기존 파티션 덮어쓰기 또는 Upsert
- 실패 Task부터 재실행
- 데이터 품질 검사 실패 시 후속 적재 중단
- CloudWatch 및 Slack을 통한 실패 알림

---

## 10. Spark 처리 흐름 및 최적화

```text
S3 Raw CSV 또는 JSON 읽기
→ 필요한 컬럼만 선택
→ 날짜 조건 Predicate Pushdown
→ 타입 변환 및 유효성 검사
→ 대여소·날씨·인구 데이터 정제
→ 작은 Dimension Broadcast Join
→ 시간·지역 기준 Repartition
→ 대여소·시간 단위 Aggregate
→ Parquet 저장
```

| 병목 가능 지점 | 적용 방법 |
|---|---|
| 대용량 대여이력 전체 스캔 | Parquet 변환, 날짜 파티셔닝 |
| 대여소 정보 조인 | Broadcast Join |
| 특정 지역 데이터 집중 | 지역·날짜 복합 파티셔닝 또는 Salting |
| 작은 파일 증가 | 적정 크기로 Repartition 또는 Coalesce |
| 동일 데이터 반복 사용 | 필요한 구간만 Cache |
| 불필요한 셔플 | 조인 키와 파티션 키 정렬 |
| 집계 속도 저하 | 사전 집계된 Silver 테이블 활용 |
| 성능 검증 | `explain()`과 Spark UI로 전후 비교 |

성능 최적화는 기술을 많이 적용하는 것이 목적이 아니라, 실제 실행 계획과 Spark UI를 통해 병목을 확인한 후 필요한 방법만 적용한다.

---

## 11. 사용자 제공 기능

| 화면 | 제공 정보 | 주요 사용자 |
|---|---|---|
| 실시간 현황 지도 | 대여소별 자전거 수, 인구 혼잡도, 날씨 | 운영자 |
| 수요 예측 지도 | 향후 30분 또는 1시간 예상 대여량 | 운영자 |
| 부족 위험 목록 | 부족 가능성이 높은 대여소 순위 | 재배치 담당자 |
| 재배치 추천 | 출발 대여소, 도착 대여소, 추천 수량 | 운영자 |
| 수요 분석 | 시간, 요일, 날씨, 인구별 이용량 | 정책 담당자 |
| 모델 성능 | MAE, RMSE, 실제값과 예측값 비교 | 데이터 담당자 |

최종 데이터 프로덕트는 다음과 같이 정의한다.

> 날씨와 실시간 인구 변화를 반영하여 대여소별 단기 수요를 예측하고, 자전거 부족 위험과 재배치 우선순위를 제공하는 운영 지원 시스템

---

## 12. 권장 AWS 구성

| 영역 | 권장 서비스 | 선택 이유 |
|---|---|---|
| 저장 | Amazon S3 | Raw, Silver, Gold 계층 저장 |
| 수집 | Lambda 또는 EC2 Python | 외부 API 및 파일 수집 |
| 오케스트레이션 | Airflow on EC2 또는 MWAA | 일정, 의존성, 재처리 관리 |
| 대용량 처리 | Amazon EMR Spark | 대여이력 정제, 집계, 조인 |
| 분석 DB | Amazon Redshift 또는 PostgreSQL | 대시보드 조회용 데이터 저장 |
| 대시보드 | Streamlit on EC2 | 구현과 시연이 간단함 |
| 모니터링 | CloudWatch | 로그, 지표, 실패 확인 |
| 알림 | SNS 또는 Slack Webhook | 수집 및 품질 오류 통보 |
| 보안 | IAM Role, Security Group, VPC Endpoint | 최소 권한 및 내부 통신 보장 |

### VPC 배치 원칙

```text
Public Subnet
 ├─ Airflow Webserver 또는 Bastion
 └─ 외부 API 수집 컴포넌트

Private Subnet
 ├─ Amazon EMR
 ├─ Redshift 또는 PostgreSQL
 └─ 내부 처리용 EC2

공통
 ├─ S3 VPC Endpoint
 ├─ 최소 권한 IAM Role
 ├─ Security Group 기반 접근 제한
 └─ CloudWatch 로그 수집
```

EMR과 데이터베이스는 Private Subnet에 배치한다. 외부 API 호출이 필요한 수집 컴포넌트만 Public Subnet에 두거나 NAT Gateway를 통해 인터넷에 접근하도록 구성한다.

---

## 13. 초기 MVP 범위

초기 구현 범위는 다음과 같이 제한한다.

```text
월별 따릉이 대여이력
+ 날씨 데이터
+ 실시간 인구 데이터
→ 시간·지역별 수요 분석 및 예측
→ Streamlit 대시보드 제공
```

### 1차 구현

- 따릉이 대여이력과 날씨 데이터 적재
- 시간대, 요일, 일자별 수요 분석
- Spark 기반 정제 및 집계
- S3 Raw, Silver, Gold 구조 구현
- Streamlit 분석 화면 구현

### 2차 구현

- 실시간 인구 API 수집
- 대여소와 실시간 인구 지역의 공간 매핑
- 수요 예측 모델 구현
- Airflow 자동화 및 품질 검사

### 확장 기능

- 실시간 대여소 잔여 자전거 데이터 연동
- 자전거 부족 위험 알림
- 대여소 간 재배치 추천
- 모델 성능 모니터링 및 자동 재학습

---

## 14. 발표용 요약

```text
서울시 따릉이, 날씨, 실시간 인구 데이터를 Airflow로 주기적으로 수집하고,
S3에 Raw, Silver, Gold 계층으로 저장한다.

Spark를 이용하여 시간과 공간 기준으로 데이터를 정제하고 통합한 뒤,
대여소별 단기 수요와 자전거 부족 위험을 예측한다.

예측 결과는 Redshift 또는 PostgreSQL에 적재하고,
Streamlit 대시보드와 Slack 또는 SNS 알림을 통해 운영자에게 제공한다.
```