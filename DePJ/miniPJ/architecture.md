# 서울 따릉이 수요 분석 및 예측 시스템 아키텍처

## 1. 시스템 목표

서울시 따릉이 대여이력, 실시간 인구, 날씨, 대여소 정보를 결합하여 대여소별 단기 수요를 예측하고, 자전거 부족 위험과 재배치 우선순위를 운영자에게 제공한다.

이 프로젝트는 다음 전체 흐름을 구현하는 것을 목표로 한다.

```text
1. 따릉이 대여이력, 실시간 인구, 날씨, 대여소 현황 수집
2. Docker 컨테이너로 수집 코드 실행
3. Airflow가 각 수집 작업의 주기와 순서를 관리
4. 수집 원본을 AWS S3 Raw 계층에 저장
5. Airflow가 EMR Spark Job 실행
6. Spark가 데이터 정제 및 형식 통일
7. 대여소와 시간 기준으로 날씨·인구·대여이력 조인
8. Spark Transformation 정의
9. Action 호출로 Job 실행
10. Shuffle 경계에서 Stage 자동 분리
11. 여러 Executor가 Task를 병렬 처리
12. 정제 결과를 S3 Silver에 저장
13. 수요 Feature와 예측 결과를 S3 Gold에 저장
14. 현재 자전거 수와 예상 대여·반납량 비교
15. 예상 잔여 자전거 수 계산
16. 부족, 정상, 과잉, 포화 상태 판단
17. 결과를 RDS 또는 Redshift에 적재
18. Streamlit Docker 컨테이너가 결과 조회
19. 사용자가 대시보드에서 상태 확인
20. 장애나 데이터 오류는 CloudWatch와 Slack으로 알림
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



## 3.1 기술별 핵심 역할

|기술|역할|
|--|--|
|Docker|로컬과 AWS의 실행 환경 통일|
|ECR|Docker 이미지 저장|
|Airflow|수집 주기, 의존성, 재시도, 재처리 관리|
|S3|Raw·Silver·Gold 데이터 저장|
|Spark|대규모 데이터 정제, 조인, 집계|
|EMR|AWS에서 Spark 클러스터 실행|
|RDS/Redshift|대시보드 조회용 결과 저장|
|Streamlit|사용자 화면 제공|
|EC2/ECS|Docker 애플리케이션 실행|
|CloudWatch|로그와 장애 모니터링|
|SNS/Slack|실패 및 이상 알림|

## 3.2 기술 선택지 비교

### 데이터 수집 실행 방식

| 선택지 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| Python 스크립트 on EC2 | 구현이 단순하고 로그 확인이 쉽다 | 실행 환경이 서버마다 달라질 수 있고 의존성 관리가 어렵다 | 초기 테스트에는 적합하지만 운영 환경에는 Docker 방식이 더 적합하다 |
| Docker on EC2 | 로컬과 AWS의 실행 환경을 동일하게 유지할 수 있고 수집기별 분리가 쉽다 | EC2와 Docker를 모두 직접 운영해야 한다 | **초기 프로젝트의 권장안** |
| AWS Lambda | 서버 관리가 필요 없고 짧은 API 수집 작업에 적합하다 | 실행 시간, 메모리, 임시 저장 공간 제약이 있다 | 날씨·인구처럼 짧은 API 수집 작업의 확장안 |
| ECS Fargate | 서버 없이 컨테이너를 실행하고 작업별 자원 분리가 쉽다 | ECS, IAM, 네트워크 설정이 추가로 필요하다 | 운영 확장 시 고려할 수 있으나 초기 프로젝트에는 다소 복잡하다 |

### 오케스트레이션

| 선택지 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| Cron | 가장 단순하고 비용이 적다 | 작업 의존성, 재처리, 실패 지점 재실행이 어렵다 | 단일 스크립트에는 가능하지만 전체 파이프라인에는 부적합하다 |
| Airflow on EC2 | DAG, 재시도, Backfill, 로그, 재처리를 직접 구현할 수 있다 | Webserver, Scheduler, Metadata DB를 직접 운영해야 한다 | **프로젝트 권장안** |
| Amazon MWAA | AWS가 Airflow 인프라를 관리하고 S3·IAM·CloudWatch 연동이 쉽다 | 비용과 환경 제약이 있다 | 실제 운영에는 유리하지만 프로젝트 비용과 학습 범위를 고려하면 후순위다 |
| Step Functions | AWS 서비스 간 워크플로와 재시도 관리가 안정적이다 | 데이터 날짜 기반 재처리와 Backfill은 Airflow보다 불편하다 | AWS 서비스 중심 파이프라인으로 확장할 때 고려한다 |
| EventBridge Scheduler | 단순 주기 실행에 적합하다 | 복잡한 의존성 관리 기능이 부족하다 | Airflow를 대체하기보다 개별 트리거 용도로 적합하다 |

### 원본 및 가공 데이터 저장소

| 선택지 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| EC2 로컬 디스크 또는 EBS | 일반 파일 시스템처럼 사용하기 쉽다 | 특정 서버에 종속되고 여러 노드가 공유하기 어렵다 | 로컬 테스트에만 사용한다 |
| Amazon S3 | 확장성이 높고 Spark·Athena·EMR과 연동되며 Raw·Silver·Gold 계층을 구성하기 쉽다 | 작은 파일이 많으면 성능이 저하되고 객체 단위 덮어쓰기를 고려해야 한다 | **영구 저장소 권장안** |
| HDFS | 전통적인 분산 파일 시스템이며 Spark와 밀접하게 동작한다 | NameNode·DataNode 운영이 필요하고 저장소와 컴퓨팅이 결합된다 | 교육용 또는 임시 처리에는 가능하지만 AWS 영구 저장소로는 비효율적이다 |
| RDS | SQL과 트랜잭션을 사용할 수 있다 | 대용량 원본 파일과 스키마가 자주 바뀌는 데이터 저장에 부적합하다 | 원본 저장소가 아니라 서빙 계층에 사용한다 |

### 데이터 포맷

| 포맷 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| CSV | 사람이 읽기 쉽고 원천 데이터와 호환된다 | 데이터 타입 정보가 없고 용량이 크며 읽기 성능이 낮다 | Raw 원본 보존용 |
| JSON | API 원본과 중첩 구조를 그대로 보존하기 쉽다 | 용량이 크고 스키마가 일관되지 않을 수 있다 | Raw API 응답 보존용 |
| Parquet | 컬럼 기반 압축, Predicate Pushdown, 컬럼 가지치기를 지원한다 | 사람이 직접 읽기 어렵고 작은 파일 문제를 관리해야 한다 | **Silver·Gold 권장 포맷** |
| Avro | 스키마 진화와 행 단위 처리에 적합하다 | 현재 시스템의 분석 쿼리에는 Parquet보다 이점이 작다 | Kafka 도입 시 고려한다 |
| Iceberg 또는 Delta Lake | ACID, 스키마 진화, Time Travel, Upsert를 지원한다 | Catalog와 테이블 관리 복잡도가 증가한다 | 초기 MVP에는 과도하며 재처리와 Upsert 요구가 커질 때 확장한다 |

### Spark 실행 환경

| 선택지 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| 단일 EC2 `local[*]` | 구축과 실행이 가장 단순하고 비용이 적다 | 실제 분산 처리와 Executor 장애를 보여주기 어렵다 | 개발 및 기능 검증용 |
| Spark Docker Cluster on EC2 | Master·Worker·Executor 구조와 Spark UI를 직접 확인할 수 있다 | 클러스터와 장애 복구를 직접 운영해야 한다 | 로컬 및 교육용 분산 실험에 적합하다 |
| EMR on EC2 | AWS가 Spark·Hadoop 클러스터 구성을 지원하고 튜닝 실험이 쉽다 | 클러스터 실행 비용과 VPC·IAM 설정이 필요하다 | **AWS 분산 처리 권장안** |
| EMR Serverless | 클러스터를 직접 관리하지 않고 작업별로 자원을 사용할 수 있다 | 세부 클러스터 제어와 분산 구조 시연이 제한된다 | 운영 편의성이 중요할 때 대안이 된다 |
| AWS Glue | 서버리스 Spark ETL과 Data Catalog 연동이 쉽다 | Spark 환경 제어가 제한되고 Glue 방식에 종속될 수 있다 | 단순 ETL 중심 시스템이라면 대안이 된다 |
| EMR on EKS | Kubernetes 환경에서 Spark 자원을 통합 관리할 수 있다 | EKS 운영 복잡도가 매우 크다 | 현재 프로젝트 규모에는 과도하다 |

### Spark API

| 선택지 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| RDD API | Transformation, Action, Lineage를 직접 이해하고 제어하기 좋다 | 스키마 관리가 어렵고 Catalyst 최적화를 충분히 활용하기 어렵다 | 핵심 연산 비교와 교육용 실험에 제한적으로 사용한다 |
| DataFrame API | 스키마 관리, Catalyst 최적화, Parquet 조인과 집계에 적합하다 | 내부 RDD 흐름이 코드에서 직접 드러나지 않는다 | **실제 ETL 권장안** |
| Spark SQL | 조인과 집계를 표현하기 쉽고 SQL로 검증하기 좋다 | 복잡한 애플리케이션 로직은 관리가 어려울 수 있다 | DataFrame과 함께 사용한다 |
| Pandas API on Spark | Pandas와 유사한 문법으로 분산 처리가 가능하다 | Spark 고유 최적화와 실행 구조를 이해하기 어렵게 만들 수 있다 | 팀 숙련도에 따라 보조적으로 사용한다 |

### 데이터 Catalog 및 스키마 관리

| 선택지 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| 경로와 코드로 관리 | 가장 단순하다 | 테이블과 스키마를 중앙에서 확인하기 어렵다 | 초기 로컬 실험에 적합하다 |
| Glue Data Catalog | S3 데이터를 테이블로 관리하고 Athena·EMR과 공유할 수 있다 | IAM과 Catalog 설정이 추가되고 Crawler 오추론 가능성이 있다 | **Silver·Gold 메타데이터 관리 권장안** |
| Hive Metastore | Spark와 전통적으로 잘 연동된다 | Metastore 서버와 DB를 직접 운영해야 한다 | AWS에서는 Glue Data Catalog보다 운영 부담이 크다 |
| PostgreSQL 메타데이터 테이블 | 필요한 메타데이터를 자유롭게 설계할 수 있다 | Catalog 기능을 직접 구현해야 한다 | 특수한 메타데이터 요구가 있을 때만 고려한다 |

### 분석 및 예측 방식

| 선택지 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| 과거 평균·규칙 기반 | 결과 설명이 쉽고 데이터가 적어도 구현 가능하다 | 복합적인 날씨·인구 영향을 충분히 반영하기 어렵다 | **1차 Baseline 권장안** |
| Spark MLlib | Spark Feature 데이터와 바로 연결되고 분산 학습이 가능하다 | 모델 종류와 실험 편의성이 제한적이다 | 데이터 규모가 매우 커질 때 고려한다 |
| Scikit-learn 또는 XGBoost | 모델 종류가 다양하고 실험·평가가 쉽다 | Spark 결과를 별도로 변환해야 하며 메모리 한계를 고려해야 한다 | **2차 예측 모델 권장안** |
| SageMaker | 학습, 배포, 모델 버전 관리를 통합할 수 있다 | 비용과 복잡도가 증가하고 데이터 엔지니어링보다 ML 플랫폼 비중이 커질 수 있다 | 실시간 모델 서빙이 필요할 때 확장한다 |

### 서빙 데이터베이스

| 선택지 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| S3 Parquet 직접 조회 | 별도 DB가 필요 없고 구조가 단순하다 | 반복 조회와 작은 조건 검색이 느릴 수 있다 | 배치 분석과 검증용 |
| Athena | S3 데이터를 서버리스 SQL로 조회할 수 있다 | 반복적인 대시보드 조회에는 지연과 비용이 누적될 수 있다 | 임시 분석과 검증용 |
| RDS PostgreSQL | Streamlit 연동이 쉽고 최신 결과 조회와 Upsert에 적합하다 | 대규모 분석 쿼리에는 한계가 있고 인스턴스 운영이 필요하다 | **대시보드 서빙 권장안** |
| Redshift | 대규모 분석, Fact·Dimension 모델, 다중 사용자 쿼리에 적합하다 | 현재 데이터 규모에는 비용과 구성이 과할 수 있다 | 장기 이력과 사용자 수가 커질 때 확장한다 |
| DynamoDB | 대여소 ID 기반 단순 조회가 빠르고 서버리스 확장이 가능하다 | 복잡한 집계와 SQL 분석이 어렵다 | 조회 패턴이 단순하게 고정될 때 대안이 된다 |

### 대시보드 및 API

| 선택지 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| Streamlit | Python만으로 지도, 차트, 필터를 빠르게 구현할 수 있다 | 복잡한 권한과 대규모 트래픽, UI 자유도에 한계가 있다 | **MVP 대시보드 권장안** |
| Grafana | 실시간 시계열 지표와 알림에 강하다 | 비즈니스 분석과 재배치 화면에는 제약이 있다 | 운영 모니터링 화면에 적합하다 |
| Superset | SQL 기반 BI 대시보드를 쉽게 만들 수 있다 | 커스텀 사용자 로직과 서비스형 UI에는 한계가 있다 | 분석가용 BI 대시보드 대안 |
| React + FastAPI | 실제 서비스형 UI와 API를 독립적으로 확장할 수 있다 | 웹 개발 범위가 크게 증가한다 | 프로젝트 이후 서비스 확장안 |
| QuickSight | AWS 관리형 BI로 서버 운영이 필요 없다 | 비용과 권한 설정이 필요하고 화면 자유도가 낮다 | 조직 내부 BI 공유용 대안 |

### 컨테이너 실행 환경

| 선택지 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| EC2 + Docker Compose | 구현이 단순하고 여러 서비스를 한 서버에서 빠르게 구성할 수 있다 | 단일 장애 지점이며 확장과 재시작을 직접 관리해야 한다 | **초기 프로젝트 권장안** |
| ECS on EC2 | ECS가 컨테이너를 관리하면서 인스턴스를 직접 제어할 수 있다 | ECS와 EC2를 모두 운영해야 한다 | 컨테이너 관리 경험을 강조할 때 고려한다 |
| ECS Fargate | 서버를 직접 관리하지 않고 컨테이너별 자원을 분리할 수 있다 | VPC, Task Definition, Service 설정이 필요하다 | Streamlit이나 수집기 분리 배포 시 확장안 |
| EKS | Kubernetes 생태계와 높은 확장성을 제공한다 | 클러스터, 노드, Pod, Service, Ingress 운영이 복잡하다 | 현재 프로젝트에는 과도하다 |
| App Runner | 웹 컨테이너를 빠르게 배포하고 HTTPS를 쉽게 제공한다 | Batch와 Airflow 실행에는 적합하지 않다 | Streamlit 단독 배포 대안 |

### Docker 이미지 저장소

| 선택지 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| Docker Hub | 사용이 단순하고 접근성이 높다 | Private 저장소 제한과 외부 서비스 의존성이 있다 | 공개 이미지나 간단한 실험에 적합하다 |
| GitHub Container Registry | GitHub Actions와 연동하기 쉽다 | AWS IAM과 별도 권한 체계가 필요하다 | GitHub 중심 CI/CD를 사용할 때 대안이 된다 |
| Amazon ECR | ECS·EKS·IAM과 연동이 쉽고 이미지 버전 관리가 편하다 | AWS 계정과 리전에 종속된다 | **AWS 배포 권장안** |
| EC2에서 직접 Build | 별도 Registry가 필요 없다 | 배포 재현성과 속도가 낮고 서버마다 결과가 달라질 수 있다 | 임시 테스트에만 사용한다 |

### 모니터링 및 알림

| 선택지 | 장점 | 단점 | 우리 시스템에서의 판단 |
|---|---|---|---|
| Airflow UI | DAG와 Task 상태를 확인하기 쉽다 | 사용자가 직접 접속해야 장애를 알 수 있다 | 파이프라인 상태 확인용 |
| Spark UI | Job, Stage, Task, Shuffle과 성능 병목을 확인할 수 있다 | Spark 실행 중 또는 로그 보존 환경이 필요하다 | Spark 성능 분석용 |
| CloudWatch | EC2, EMR, ECS, Lambda 로그와 지표를 통합할 수 있다 | 로그 설계와 비용 관리가 필요하다 | **AWS 통합 모니터링 권장안** |
| Prometheus + Grafana | 세밀한 시스템 메트릭과 시각화가 가능하다 | 모니터링 시스템 자체를 운영해야 한다 | 규모가 커질 때 확장안 |
| SNS 또는 Slack | 실패와 이상 상황을 즉시 전달할 수 있다 | 탐지 기준과 알림 중복 제어가 필요하다 | **실패 및 데이터 이상 알림 권장안** |

## 3.3 최종 권장 기술 조합

| 영역 | 최종 선택 | 선택 이유 |
|---|---|---|
| 수집 | Python + Docker on EC2 | 구현이 단순하고 로컬과 AWS 환경을 통일할 수 있다 |
| 오케스트레이션 | Airflow on EC2 | DAG, 재시도, Backfill, 재처리를 직접 구현하고 설명할 수 있다 |
| 원본 저장 | S3 Raw | 원본 보존과 재처리에 적합하다 |
| 정제 저장 | S3 Silver Parquet | Spark 정제와 분석 성능에 적합하다 |
| 분석 저장 | S3 Gold Parquet | Feature와 예측 결과를 장기 보존하기 좋다 |
| Catalog | Glue Data Catalog | S3 테이블과 스키마를 중앙에서 관리할 수 있다 |
| 분산 처리 | EMR on EC2 Spark | 실제 Executor, Stage, Task 구조와 튜닝 결과를 보여줄 수 있다 |
| Spark API | DataFrame·Spark SQL 중심 | Catalyst 최적화와 유지보수성이 높다 |
| RDD 사용 | 비교 실험 및 핵심 로직 일부 | Transformation, Action, Lineage, Stage 개념을 설명할 수 있다 |
| 예측 | 규칙 기반 Baseline + XGBoost | 설명 가능한 기준 모델과 성능 개선 모델을 비교할 수 있다 |
| 서빙 DB | RDS PostgreSQL | 최신 대여소 상태와 위험 순위를 빠르게 조회할 수 있다 |
| 대시보드 | Streamlit | 지도와 분석 결과를 빠르게 구현할 수 있다 |
| 이미지 저장 | Amazon ECR | AWS 컨테이너 실행 환경과 IAM으로 연동할 수 있다 |
| 실행 환경 | EC2 + Docker Compose | 프로젝트 범위에서 직접 관리하고 시연하기 쉽다 |
| 모니터링 | CloudWatch + Airflow UI + Spark UI | 시스템, 파이프라인, Spark 성능을 각각 확인할 수 있다 |
| 알림 | SNS 또는 Slack | 실패와 데이터 이상을 즉시 전달할 수 있다 |

최종 권장 흐름은 다음과 같다.

```text
외부 데이터
→ Docker 수집기
→ Airflow on EC2
→ S3 Raw
→ EMR Spark
→ S3 Silver·Gold + Glue Data Catalog
→ 수요 예측
→ RDS PostgreSQL
→ Docker Streamlit on EC2
→ 사용자

운영 로그
→ CloudWatch
→ SNS 또는 Slack
```

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