# 과제 목표
- Spark Master 1개
- Spark Worker 2개
- Worker당 CPU Core 2개
- Worker당 Memory 1GB
- Spark Job: Monte Carlo 방식의 π 추정

# 과제 핵심

1. Spark Master 1개와 Worker 2개가 정상적으로 연결되는가
2. spark-submit으로 Job을 Master에 제출할 수 있는가
3. Master가 작업을 Worker들에게 분배하는가
4. 결과와 로그를 통해 Job 성공 여부를 확인할 수 있는가

## 요구 사항
- Master와 Worker의 연결
- Driver와 Executor의 역할
- Partition과 Task의 분산 처리
- Worker별 자원 제한
- Docker를 통한 실행 환경 재현
- 쉘 스크립트를 통한 Job 제출 자동화


## 과제 흐름
Dockerfile 작성
    ↓
Spark 실행 환경 이미지 생성
    ↓
docker-compose.yml 작성
    ↓
Master 1개 + Worker 2개 실행
    ↓
Spark Web UI에서 Worker 연결 확인
    ↓
Spark Job 준비
    ↓
submit-job.sh 실행
    ↓
spark-submit이 Master에 Job 제출
    ↓
Master가 Worker에 Task 분배
    ↓
Worker가 병렬로 계산
    ↓
결과 및 로그 확인


## 주의사항
1. Spark 버전을 하나로 명시
2. Master와 Worker가 같은 Docker 이미지를 사용
3. Python과 Java 버전도 이미지 안에서 통일
4. 로컬에 설치된 Spark나 PySpark에 의존하지 않음
5. latest 태그 사용을 피함
6. ARM64와 AMD64에서 사용할 수 있는 이미지 확인
7. README에 버전과 실행 방법 기록


## 주요 설정
* Spark       -  3.5.x
* Hadoop 빌드  - Hadoop 3
* Java        -  17
* Python      - 3
* Cluster     - Spark Standalone


## 구조

PiEstimation Application
├── Worker 1
│   └── Executor 1
│       ├── Core 1
│       └── Core 2
└── Worker 2
    └── Executor 0
        ├── Core 1
        └── Core 2

한 worker 당 코어 2개로 제한 -> 코어가 제대로 일을 할당 받기 위해서
pi.py 에서 파티션 8개 만들었으니까 총 4개의 코어가 두번에 나눠서 task 처리




## 추가 사항

- Hadoop MapReduce와 Spark 비교

### Hadoop
Hadoop MapReduce에서는 Mapper와 Reducer가 각각 실행된 후 종료된다.

### Spark
Driver가 전체 연산 흐름을 관리하고, 여러 연산 단계를 하나의 Application 안에서 실행한다.

Driver
├── 연산 흐름 관리
├── Partition 구성
├── Task 생성
├── Executor 상태 확인
└── 결과 집계

Spark는 여러 단계의 연산을 연속적으로 수행하므로, 중간에 장애가 발생했을 때 처음부터 모든 작업을 다시 수행하는 대신 RDD의 lineage를 이용해 필요한 Partition을 다시 계산할 수 있다.

## 배운점
이번 과제를 통해 Docker 컨테이너를 여러 개 실행하는 것만으로 분산 처리가 이루어지는 것은 아니라는 점을 확인했다.

Driver
→ 실행 계획과 Task 관리

Master
→ Worker 자원 할당

Worker
→ Executor 실행 환경 제공

Executor
→ 실제 Task 처리

Core
→ 동시에 실행 가능한 Task 수 결정

Partition
→ 데이터 분할 및 병렬 처리 단위





### 반대 상황
반대로 Spark를 쓰지 않고 컨테이너 두 개만 띄운다면:
두 컨테이너가 존재할 뿐, pi.py 작업을 자동으로 반씩 나누지는 않는다. 
직접 코드를 작성해서 데이터 범위를 나누고 결과를 합쳐야 한다.



# 학습 목표
The objective of this project is to build an Apache Spark standalone cluster using Docker, run a Spark job, and validate the results

## 기능요구사항
Cluster Setup:
The cluster should consist of one Spark master and two Spark worker nodes.

Each node should be able to communicate with the others, and the master should be able to schedule tasks on the workers.

The Spark Web UI should be accessible to monitor the job progress and cluster status.

## Spark Job Execution:
The Spark job should be able to read a dataset from a mounted volume or a specified input path.

The job should perform a data transformation (e.g., estimating π) and write the results to a specified output path.

The output should be correctly partitioned and saved in a format such as CSV or Parquet.

## Error Handling:
The setup should handle errors gracefully, including network issues between Docker containers and misconfigurations.

Logs should be accessible to debug any issues with the Spark job or cluster setup.

## Reproducibility:
The entire setup should be reproducible using the provided Dockerfile and docker-compose.yml.

Clear instructions should be provided on how to build the Docker image, start the cluster, submit the job, and verify the results.

# 프로그래밍 요구사항
## Dockerfile:
Create a Dockerfile to build a Docker image with Apache Spark.

The Docker image should include Java, Python, and Spark.

Ensure the image is configured to run in standalone mode.

## Docker Compose File:
Create a docker-compose.yml file to set up a Spark standalone cluster with one master and two worker nodes.

Configure the Docker Compose file to expose the necessary ports for the Spark Web UI and master.

## Spark Job:
Use one of the sample job scripts included in the Spark distribution file (e.g., examples/src/main/python/pi.py).

Ensure the script reads a dataset, performs a transformation (e.g., estimating π using the Monte Carlo method), and writes the results to a specified output location.

## Submission Script:
Create a shell script to submit the Spark job to the standalone cluster using spark-submit.

# 예상결과 및 동작예시
Check the Spark Web UI at http://localhost:8080 to monitor the job.

The output (estimated value of π) will be printed in the logs of the Spark job.