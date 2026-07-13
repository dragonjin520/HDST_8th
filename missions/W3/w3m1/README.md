# 과제 흐름
1. Dockerfile 작성
    - 컨테이너 실행시 NameNode, DataNode, 기타 Hadoop 서비스 자동 시작
2. Hadoop 설정 파일 작성
    - Single node cluster
    - 노드설정, 저장방식, 실행방식, 환경변수
3. Hadoop 시작 스크립트 작성
4. Docker 이미지 빌드
5. Docker Volume과 포트를 연결해 컨테이너 실행
6. NameNode와 DataNode 실행 상태 확인
7. HDFS 디렉터리 생성
8. 샘플 파일 생성 및 업로드
9. HDFS에서 파일 내용 확인
10. 파일을 다시 로컬로 가져오기
11. 브라우저에서 HDFS Web UI 확인
12. 컨테이너 재시작 후 데이터 유지 여부 확인



# 기능요구사항
## Docker Image:
The Docker image should encapsulate a fully functional single-node Hadoop cluster.

When the Docker container runs, it should automatically start all necessary Hadoop services.

The container should be able to connect to the host machine's network to facilitate access to HDFS.

## HDFS Operations:
Users should be able to interact with HDFS from within the Docker container.

Users should be able to create directories, upload files, and retrieve files from HDFS.

The HDFS web interface should be accessible from the host machine to monitor the file system.

## Persistence:
The Hadoop data directory within the Docker container should be configured to persist data between container restarts.

Ensure that the data stored in HDFS remains intact even if the container is stopped and restarted.

## Create and Write File in HDFS:
Users should create a directory in HDFS.

Users should write a text file into the created directory.

The content of the text file should be verifiable by retrieving it from HDFS.

## Documentation:
Provide clear instructions on how to build the Docker image and run the container.

Include steps for configuring Hadoop within the container and starting the services.

Document how to perform basic HDFS operations, such as creating directories and uploading files.

# 프로그래밍 요구사항
## Docker Setup:
Install Docker on your local machine if it is not already installed.

Create a Dockerfile to configure the Hadoop environment.

Build a Docker image from the Dockerfile that sets up a single-node Hadoop cluster.

Ensure the Docker image includes all necessary configurations and dependencies for Hadoop.

## Hadoop Configuration:
Configure Hadoop core-site.xml, hdfs-site.xml, and mapred-site.xml files for a single-node cluster.

Set up Hadoop environment variables.

Format the HDFS namenode within the Docker container.

## Start Hadoop Services:
Start the Hadoop namenode and datanode services within the Docker container.

Ensure that the Hadoop Distributed File System (HDFS) is running correctly.

## Data Operations:
Create a directory in HDFS.

Upload a sample file from the local file system to HDFS.

Retrieve the file from HDFS to ensure it was uploaded successfully.

# 예상결과 및 동작예시
## Running Container:
A running Docker container with a single-node Hadoop cluster.

All Hadoop services (namenode, datanode, etc.) should be up and running within the container.

## HDFS Operations:
Create a directory in HDFS

Successfully upload a file from the local file system to the directory in HDFS.

Retrieve the uploaded file from HDFS to the local file system.

## Accessibility:
Access the HDFS web interface from the host machine to verify the cluster's status and perform file system operations.

# Submission
Submit the Dockerfile and any other configuration files used for setting up the Hadoop cluster.

Provide a README file with step-by-step instructions for building the Docker image, running the container, and performing HDFS operations.