# W4M1 과제 핵심

1. Spark Master 1개와 Worker 2개가 정상적으로 연결되는가
2. spark-submit으로 Job을 Master에 제출할 수 있는가
3. Master가 작업을 Worker들에게 분배하는가
4. 결과와 로그를 통해 Job 성공 여부를 확인할 수 있는가


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


### 주요 설정
* Spark       -  3.5.x
* Hadoop 빌드  - Hadoop 3
* Java        -  17
* Python      - 3
* Cluster     - Spark Standalone






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