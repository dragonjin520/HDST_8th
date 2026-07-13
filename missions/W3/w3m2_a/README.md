# W3M2-A: Docker 기반 멀티 노드 Hadoop 클러스터

## 1. W3M1과 W3M2의 차이점

W3M1과 W3M2는 모두 Docker 환경에서 Hadoop을 실행하지만, 확인하려는 범위가 다르다.

### W3M2

W3M2의 중심은 **여러 Hadoop 노드를 연결해 분산 저장과 분산 처리를 검증하는 것**이다.

- Master 노드에서 NameNode와 ResourceManager를 실행한다.
- Worker 노드에서 DataNode와 NodeManager를 실행한다.
- HDFS를 이용해 데이터를 저장한다.
- YARN이 Worker 노드에 작업을 배정한다.
- MapReduce 작업을 실행해 분산 처리를 확인한다.

| 구분 | W3M1 | W3M2 |
|---|---|---|
| 중심 주제 | HDFS 저장 구조와 복구 | 멀티 노드 Hadoop 클러스터 구축 |
| 주요 서비스 | NameNode, DataNode | NameNode, DataNode, ResourceManager, NodeManager |
| 주요 기능 | 파일 저장, 조회, 재시작 후 복구 | 분산 저장, 작업 분배, MapReduce 실행 |
| 검증 포인트 | 데이터가 Volume에 유지되는가 | Worker가 클러스터에 참여하고 작업을 처리하는가 |

---

## 2. Docker에서 컨테이너를 사용하는 방식

Docker는 컨테이너마다 독립된 실행 환경을 제공한다. 이번 과제에서는 한 컨테이너 안에서 여러 Hadoop 노드를 실행하지 않고, **각 컨테이너를 하나의 Hadoop 노드처럼 사용한다.**

```text
Docker Host
├── hadoop-master 컨테이너
│   ├── NameNode
│   └── ResourceManager
│
└── hadoop-worker1 컨테이너
    ├── DataNode
    └── NodeManager
```

| 실제 Hadoop 환경 | Docker 실습 환경 |
|---|---|
| 물리 서버 1대 | 컨테이너 1개 |
| 서버 IP 주소 | 컨테이너 hostname |
| 서버 간 네트워크 | Docker Network |
| 서버 디스크 | Docker Volume |

Master와 Worker는 같은 Docker Network에 연결되며, `master`, `worker1`이라는 hostname으로 서로 통신한다.

또한 Master와 Worker는 별도의 이미지를 각각 빌드하지 않고, 공통 이미지인 `hadoop-base:3.3.6`을 함께 사용한다.

```text
hadoop-base:3.3.6
├── hadoop-master 컨테이너
└── hadoop-worker1 컨테이너
```

컨테이너의 역할은 `NODE_ROLE` 환경 변수로 구분한다.

```text
NODE_ROLE=master → NameNode, ResourceManager 실행
NODE_ROLE=worker → DataNode, NodeManager 실행
```

---

## 3. 프로젝트 구조

```text
w3m2_a/
├── config/
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── mapred-site.xml
│   └── yarn-site.xml
├── scripts/
│   └── start-hadoop.sh
├── Dockerfile.master
├── Dockerfile.worker
├── docker-compose.yml
└── README.md
```

- `config/`: Hadoop 설정 파일
- `scripts/`: 컨테이너 시작 시 Hadoop 서비스를 자동 실행하는 스크립트
- `Dockerfile.master`: Hadoop 공통 이미지 생성에 사용
- `docker-compose.yml`: Master, Worker, Network, Volume 구성

---

## 4. 주요 설정

### core-site.xml

기본 파일 시스템을 Master의 NameNode로 설정한다.

```xml
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://master:9000</value>
</property>
```

### hdfs-site.xml

NameNode와 DataNode의 저장 위치 및 복제 개수를 설정한다.

```xml
<property>
    <name>dfs.replication</name>
    <value>1</value>
</property>
```

현재 Worker가 1개이므로 복제 개수는 1로 설정했다.

### mapred-site.xml

MapReduce 작업이 YARN 위에서 실행되도록 설정한다.

```xml
<property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
</property>
```

### yarn-site.xml

ResourceManager의 위치와 NodeManager의 shuffle 서비스를 설정한다.

```xml
<property>
    <name>yarn.resourcemanager.hostname</name>
    <value>master</value>
</property>
```

---

## 5. 실행 방법

### 5.1 이미지 확인

```bash
docker images
```

공통 이미지가 없다면 빌드한다.

```bash
docker compose build master
```

이미 생성된 이미지가 있다면 다시 빌드하지 않고 실행할 수 있다.

### 5.2 컨테이너 실행

```bash
docker compose up -d --no-build
```

상태 확인:

```bash
docker compose ps
```

정상적으로 실행되면 다음 두 컨테이너가 `Up` 상태로 표시된다.

```text
hadoop-master
hadoop-worker1
```

---

## 6. 네트워크 및 Hadoop 설치 확인

Master에서 Worker 확인:

```bash
docker exec hadoop-master getent hosts worker1
```

Worker에서 Master 확인:

```bash
docker exec hadoop-worker1 getent hosts master
```

Hadoop 버전 확인:

```bash
docker exec hadoop-master hadoop version
docker exec hadoop-worker1 hadoop version
```

두 컨테이너 모두 `Hadoop 3.3.6`이 출력되어야 한다.

---

## 7. Hadoop 서비스 자동 시작 확인

컨테이너 시작 시 `scripts/start-hadoop.sh`가 자동으로 실행된다.

Master에서는 다음 서비스가 실행된다.

```text
NameNode
ResourceManager
```

Worker에서는 다음 서비스가 실행된다.

```text
DataNode
NodeManager
```

프로세스 확인:

```bash
docker exec hadoop-master jps
docker exec hadoop-worker1 jps
```

정상 출력 예시:

```text
Master
- NameNode
- ResourceManager
- Jps

Worker
- DataNode
- NodeManager
- Jps
```

---

## 8. HDFS 기본 동작 확인

샘플 파일 생성:

```bash
docker exec hadoop-master bash -c 'echo "hello hadoop hello docker" > /tmp/sample.txt'
```

HDFS 디렉터리 생성:

```bash
docker exec hadoop-master hdfs dfs -mkdir -p /input
```

파일 업로드:

```bash
docker exec hadoop-master hdfs dfs -put /tmp/sample.txt /input/
```

업로드 확인:

```bash
docker exec hadoop-master hdfs dfs -ls /input
docker exec hadoop-master hdfs dfs -cat /input/sample.txt
```

예상 출력:

```text
hello hadoop hello docker
```

---

## 9. MapReduce WordCount 실행

기존 출력 디렉터리가 있다면 삭제한다.

```bash
docker exec hadoop-master hdfs dfs -rm -r -f /output
```

WordCount 실행:

```bash
docker exec hadoop-master bash -c '
hadoop jar \
$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar \
wordcount \
/input \
/output
'
```

결과 확인:

```bash
docker exec hadoop-master hdfs dfs -cat /output/part-r-00000
```

예상 출력:

```text
docker  1
hadoop  1
hello   2
```

이 과정으로 다음 흐름을 확인했다.

```text
HDFS 입력 저장
→ ResourceManager가 작업 접수
→ Worker의 NodeManager가 작업 실행
→ 결과를 HDFS에 저장
```

---

## 10. Web UI 확인

### NameNode Web UI

```text
http://localhost:9870
```

확인 항목:

- Live DataNode 1개
- HDFS 저장 상태
- 파일 시스템 정보

### YARN ResourceManager Web UI

```text
http://localhost:8088
```

확인 항목:

- Active Node 1개
- Worker 상태 RUNNING
- MapReduce Application 실행 결과

---

## 11. 데이터 영속성 확인

컨테이너를 종료한다.

```bash
docker compose down
```

Volume은 삭제하지 않는다.

다시 실행한다.

```bash
docker compose up -d --no-build
```

기존 데이터 확인:

```bash
docker exec hadoop-master hdfs dfs -cat /input/sample.txt
docker exec hadoop-master hdfs dfs -cat /output/part-r-00000
```

기존 파일과 MapReduce 결과가 유지되면 Docker Volume을 통한 데이터 영속성이 정상적으로 동작한 것이다.

> `docker compose down -v`는 Volume까지 삭제하므로 HDFS 데이터를 유지해야 할 때는 사용하지 않는다.

---

## 12. 진행 결과

이번 과제를 통해 다음 항목을 확인했다.

- Master와 Worker 컨테이너 구성
- 공통 Hadoop 이미지 재사용
- Docker Network 기반 컨테이너 간 통신
- NameNode, DataNode 연결
- ResourceManager, NodeManager 연결
- HDFS 파일 업로드 및 조회
- MapReduce WordCount 실행
- Hadoop 서비스 자동 시작
- Docker Volume 기반 데이터 영속성

최종 구조는 다음과 같다.

```text
Docker Host
│
├── hadoop-master
│   ├── NameNode
│   └── ResourceManager
│
├── hadoop-worker1
│   ├── DataNode
│   └── NodeManager
│
├── hadoop-network
│   └── Master와 Worker 간 통신
│
└── Docker Volumes
    ├── NameNode 메타데이터 유지
    └── DataNode 블록 데이터 유지
```
