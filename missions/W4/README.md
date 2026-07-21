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