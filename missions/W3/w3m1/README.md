# 과제 흐름
1. Dockerfile 작성
    - 컨테이너 실행시 NameNode, DataNode, 기타 Hadoop 서비스 자동 시작
2. Hadoop 설정 파일 작성
    - Single node cluster
    - 노드설정, 저장방식, 실행방식, 환경변수
3. Hadoop 시작 스크립트 작성
4. Docker 이미지 빌드
   ```text
   docker build -t hadoop-single-node .
   ```
5. Docker Volume과 포트를 연결해 컨테이너 실행
    
    ```text
    docker run ...
    ```
6. NameNode와 DataNode 실행 상태 확인

    ```text
    jps
    ```
7. HDFS 디렉터리 생성

    ```text
    hdfs dfs -mkdir -p /user/hadoop/input
    ```
8. 샘플 파일 생성 및 업로드
    ```text
    hdfs dfs -put sample.txt /user/hadoop/input/
    hdfs dfs -ls /user/hadoop/input
    hdfs dfs -cat /user/hadoop/input/sample.txt
    ```
9. HDFS에서 파일 내용 확인
10. 파일을 다시 로컬로 가져오기
    ```text
    hdfs dfs -get /user/hadoop/input/sample.txt retrieved.txt
    ```
11. 브라우저에서 HDFS Web UI 확인
    ```text
    http://localhost:9870
    ```
12. 컨테이너 재시작 후 데이터 유지 여부 확인



# 과제 정리

## 1. Docker에서 Hadoop 실행 환경 구성

Dockerfile에는 다음 환경이 포함된다.

- Ubuntu
- Java 17
- Hadoop 3.5.0
- Hadoop 설정 파일
- Hadoop 시작 스크립트

Docker 이미지는 Hadoop 실행에 필요한 운영체제, Java, Hadoop, 설정 파일을 모두 포함한다.

따라서 다른 컴퓨터에서도 Docker만 설치되어 있다면 동일한 Hadoop 실행 환경을 다시 만들 수 있다.

> Docker의 역할은 Hadoop 실행 환경을 표준화하고 재현하는 것이다.

---

## 2. Hadoop 설정 파일 적용

Hadoop이 단일 노드 클러스터로 동작하도록 다음 설정 파일을 작성한다.

| 파일 | 역할 |
|---|---|
| `core-site.xml` | Hadoop이 사용할 기본 파일 시스템을 HDFS로 지정 |
| `hdfs-site.xml` | NameNode와 DataNode 저장 경로, 복제 개수 설정 |
| `mapred-site.xml` | MapReduce 실행 방식을 YARN으로 설정 |

작성한 설정 파일은 Docker 이미지 빌드 과정에서 Hadoop 설정 경로인 `/opt/hadoop/etc/hadoop`으로 복사한다.

---

## 3. Hadoop 서비스 자동 실행

`start-hadoop.sh`는 컨테이너가 시작될 때 다음 작업을 수행한다.

```text
HDFS 저장 디렉터리 생성
→ 최초 실행 여부 확인
→ 최초 실행 시에만 NameNode 포맷
→ NameNode 시작
→ DataNode 시작
→ 실행 상태 확인
```

NameNode 포맷은 HDFS 메타데이터 저장 공간을 초기화하는 작업이다.

매번 컨테이너를 실행할 때 NameNode를 다시 포맷하면 기존 HDFS 메타데이터가 사라질 수 있으므로, 최초 실행일 때만 포맷하도록 구성한다.

---

## 4. HDFS 디렉터리와 파일 작업

Hadoop 서비스가 실행된 뒤 HDFS에 디렉터리를 생성하고 파일을 업로드한다.

```text
로컬 파일 생성
→ HDFS 디렉터리 생성
→ 파일 업로드
→ 파일 목록 조회
→ 파일 내용 확인
→ 다시 로컬로 가져오기
```

이 과정은 단순히 컨테이너 폴더에 파일을 복사하는 것이 아니라, HDFS를 통해 파일을 저장하고 읽는 과정이다.

```bash
hdfs dfs -mkdir -p /user/hadoop/input
hdfs dfs -put sample.txt /user/hadoop/input/
hdfs dfs -ls /user/hadoop/input
hdfs dfs -cat /user/hadoop/input/sample.txt
hdfs dfs -get /user/hadoop/input/sample.txt retrieved.txt
```

---

## 5. NameNode Web UI 확인

NameNode Web UI는 브라우저에서 HDFS 상태를 확인할 수 있는 관리 화면이다.

```text
http://localhost:9870
```

Web UI에서는 다음 정보를 확인할 수 있다.

- NameNode 실행 상태
- 연결된 DataNode 수
- DFS 사용량과 남은 저장 공간
- HDFS 디렉터리와 파일
- 데이터 블록 상태

---

## 6. 컨테이너 재시작 후 데이터 유지 확인

같은 컨테이너를 중지했다가 다시 시작한 뒤에도 HDFS 파일이 남아 있는지 확인한다.

```text
컨테이너 stop
→ 컨테이너 start
→ 기존 NameNode 메타데이터 사용
→ HDFS 파일 조회
```

이 실험은 Hadoop 서비스가 재시작될 때 기존 NameNode 메타데이터와 DataNode 블록을 다시 읽어 이전 상태를 복구하는지 확인하기 위한 것이다.

---

## 7. Docker Volume을 통한 데이터 영속성 확인

컨테이너를 중지했다가 다시 시작하는 것만으로는 완전한 데이터 영속성을 검증했다고 보기 어렵다.

같은 컨테이너를 재시작하면 컨테이너 내부 파일 시스템이 그대로 남지만, 컨테이너 자체를 삭제하면 내부 데이터도 함께 사라질 수 있기 때문이다.

따라서 HDFS 저장 경로를 Docker Volume과 연결한다.

```bash
docker run -d \
  --name hadoop-node \
  -p 9000:9000 \
  -p 9870:9870 \
  -v hadoop-data:/hadoop/dfs \
  hadoop-single-node
```

구조는 다음과 같다.

```text
Docker Container
├── Java
├── Hadoop
└── Hadoop 실행 프로세스

Docker Volume
├── NameNode 메타데이터
└── DataNode 데이터 블록
```

컨테이너를 삭제하고 새 컨테이너를 만들어도 동일한 Volume을 연결하면 기존 HDFS 데이터를 다시 사용할 수 있다.

```text
컨테이너 A 생성
→ HDFS에 파일 저장
→ 컨테이너 A 삭제
→ 컨테이너 B 생성
→ 같은 Volume 연결
→ 기존 파일 조회
```

> 실행 환경인 컨테이너는 언제든 교체할 수 있지만, 데이터는 컨테이너와 독립적으로 유지되어야 한다.

---

## 8. 이 과제를 수행하는 이유

실제 데이터 시스템에서는 서버 장애, 재배포, 버전 업데이트, 설정 변경 등으로 실행 환경이 자주 교체될 수 있다.

그때마다 저장된 데이터까지 사라진다면 안정적인 데이터 시스템으로 사용할 수 없다.

이번 과제는 다음 세 가지를 이해하기 위한 실습이다.

### 환경 재현성

Docker 이미지로 동일한 Hadoop 실행 환경을 다시 만들 수 있다.

### HDFS 구조

NameNode가 메타데이터를 관리하고, DataNode가 실제 데이터 블록을 저장한다.

### 데이터 영속성

컨테이너가 중지되거나 삭제되어도 HDFS 데이터는 Docker Volume에 독립적으로 유지된다.

한 문장으로 정리하면 다음과 같다.

> Docker 컨테이너는 교체 가능한 Hadoop 실행 환경으로 사용하고, HDFS 데이터는 Docker Volume에 분리하여 안전하게 유지하는 구조를 직접 구현하고 검증하는 과제이다.



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