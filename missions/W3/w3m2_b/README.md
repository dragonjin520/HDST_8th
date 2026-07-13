# W3M2-B: Docker 기반 Hadoop 멀티 노드 클러스터 설정

## 1. 학습 목표

Docker 컨테이너를 이용해 Apache Hadoop 멀티 노드 클러스터를 구성하고, Hadoop의 주요 설정 파일을 수정·검증하는 과제이다.

이번 과제에서는 다음 내용을 수행한다.

- 최소 2개 이상의 Docker 컨테이너로 Hadoop 클러스터 구성
- Hadoop 설정 파일 4종 수정
- 기존 설정 파일 백업
- Hadoop 서비스 재시작
- 설정값 적용 여부 자동 검증
- HDFS 복제 계수와 YARN 기반 MapReduce 실행 확인

---

## 2. Hadoop 설정 파일

### `core-site.xml`

Hadoop 공통 설정과 기본 파일 시스템 정보를 관리한다.

| 설정 | 설명 | 적용 값 |
|---|---|---|
| `fs.defaultFS` | 기본 파일 시스템 URI | `hdfs://namenode:9000` |
| `hadoop.tmp.dir` | Hadoop 임시 디렉터리 | `/hadoop/tmp` |
| `io.file.buffer.size` | 파일 입출력 버퍼 크기 | `131072` |

### `hdfs-site.xml`

HDFS 저장 구조와 NameNode, DataNode 관련 설정을 관리한다.

| 설정 | 설명 | 적용 값 |
|---|---|---|
| `dfs.replication` | HDFS 기본 복제 계수 | `2` |
| `dfs.blocksize` | HDFS 기본 블록 크기 | `134217728` (128 MB) |
| `dfs.namenode.name.dir` | NameNode 메타데이터 저장 경로 | `/hadoop/dfs/name` |

### `mapred-site.xml`

MapReduce 실행 프레임워크와 작업 관련 설정을 관리한다.

| 설정 | 설명 | 적용 값 |
|---|---|---|
| `mapreduce.framework.name` | MapReduce 실행 프레임워크 | `yarn` |
| `mapreduce.jobhistory.address` | JobHistoryServer 주소 | `namenode:10020` |
| `mapreduce.task.io.sort.mb` | Map 출력 정렬에 사용하는 메모리 | `256` |

### `yarn-site.xml`

YARN의 ResourceManager, NodeManager, 컨테이너 자원 설정을 관리한다.

| 설정 | 설명 | 적용 값 |
|---|---|---|
| `yarn.resourcemanager.address` | ResourceManager IPC 주소 | `namenode:8032` |
| `yarn.nodemanager.resource.memory-mb` | NodeManager가 사용할 수 있는 메모리 | `8192` |
| `yarn.scheduler.minimum-allocation-mb` | YARN 컨테이너 최소 메모리 할당량 | `1024` |

---

## 3. 프로젝트 구성

```text
w3m2_b/
├── README.md
├── docker-compose.yml
├── scripts/
│   ├── modify_config.sh
│   ├── verify_config.sh
│   └── start-hadoop.sh
└── config/
    ├── original/
    │   ├── core-site.xml
    │   ├── hdfs-site.xml
    │   ├── mapred-site.xml
    │   └── yarn-site.xml
    └── modified/
        ├── core-site.xml
        ├── hdfs-site.xml
        ├── mapred-site.xml
        └── yarn-site.xml
```

> 실제 디렉터리 구조가 다르다면 현재 프로젝트 구조에 맞게 경로를 수정한다.

---

## 4. 설정 변경 스크립트

설정 변경 스크립트는 Hadoop 설정 디렉터리 경로를 인자로 전달받아 동작한다.

주요 동작은 다음과 같다.

1. 인자로 전달된 설정 디렉터리 확인
2. 설정 파일 존재 여부 확인
3. 기존 XML 파일 백업
4. 지정된 Hadoop 설정값 수정
5. XML 구조와 문법 유지
6. 파일별 성공 또는 실패 상태 출력
7. Hadoop DFS와 YARN 서비스 재시작

실행 예시:

```bash
chmod +x scripts/modify_config.sh
./scripts/modify_config.sh /opt/hadoop/etc/hadoop
```

Docker 컨테이너 내부에서 실행하는 경우:

```bash
docker exec hadoop-master bash /workspace/scripts/modify_config.sh /opt/hadoop/etc/hadoop
```

---

## 5. Hadoop 서비스 시작 및 재시작

클러스터를 실행한 뒤 NameNode, DataNode, ResourceManager, NodeManager를 시작한다.

```bash
docker compose up -d
chmod +x scripts/start-hadoop.sh
./scripts/start-hadoop.sh
```

서비스가 이미 실행 중이라면 설정 변경 후 재시작하여 변경 사항을 반영한다.

확인 예시:

```bash
docker exec hadoop-master jps
docker exec hadoop-worker1 jps
```

예상 프로세스:

- `hadoop-master`: `NameNode`, `ResourceManager`
- `hadoop-worker1`: `DataNode`, `NodeManager`

---

## 6. 설정 검증 스크립트

검증 스크립트는 Hadoop 명령어를 사용해 실제 적용된 설정값을 확인한다.

```bash
chmod +x scripts/verify_config.sh
./scripts/verify_config.sh
```

검증 항목은 다음과 같다.

### 설정값 검증

- 기본 파일 시스템 URI
- Hadoop 임시 디렉터리
- 파일 입출력 버퍼 크기
- HDFS 복제 계수
- HDFS 블록 크기
- NameNode 메타데이터 저장 경로
- MapReduce 실행 프레임워크
- JobHistoryServer 주소
- Map 출력 정렬 메모리
- ResourceManager 주소
- NodeManager 메모리
- YARN 컨테이너 최소 메모리 할당량

### 동작 검증

- HDFS에 테스트 파일 생성
- 테스트 파일의 실제 복제 계수 확인
- 간단한 MapReduce 작업 실행
- MapReduce 작업이 YARN 프레임워크에서 실행되는지 확인
- YARN ResourceManager와 NodeManager 상태 확인

---

## 7. 주요 검증 명령어

### Hadoop 설정값 확인

```bash
hdfs getconf -confKey fs.defaultFS
hdfs getconf -confKey hadoop.tmp.dir
hdfs getconf -confKey io.file.buffer.size
hdfs getconf -confKey dfs.replication
hdfs getconf -confKey dfs.blocksize
hdfs getconf -confKey dfs.namenode.name.dir

hadoop getconf -confKey mapreduce.framework.name
hadoop getconf -confKey mapreduce.jobhistory.address
hadoop getconf -confKey mapreduce.task.io.sort.mb

yarn getconf -confKey yarn.resourcemanager.address
yarn getconf -confKey yarn.nodemanager.resource.memory-mb
yarn getconf -confKey yarn.scheduler.minimum-allocation-mb
```

### HDFS 테스트 파일 생성

```bash
echo "hello hadoop" > /tmp/test.txt
hdfs dfs -mkdir -p /w3m2-test
hdfs dfs -put -f /tmp/test.txt /w3m2-test/test.txt
hdfs fsck /w3m2-test/test.txt -files -blocks -locations
```

복제 계수가 `2`인지 확인한다.

### MapReduce 작업 실행

```bash
hadoop jar "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-"*.jar \
  wordcount /w3m2-test /w3m2-output
```

작업 실행 후 YARN 애플리케이션 목록을 확인한다.

```bash
yarn application -list -appStates ALL
```

---

## 8. 예상 출력

설정값이 기대값과 일치하면 `PASS`, 일치하지 않으면 `FAIL`을 출력한다.

```text
PASS: fs.defaultFS -> hdfs://namenode:9000
PASS: hadoop.tmp.dir -> /hadoop/tmp
PASS: io.file.buffer.size -> 131072
PASS: dfs.replication -> 2
PASS: dfs.blocksize -> 134217728
PASS: dfs.namenode.name.dir -> /hadoop/dfs/name
PASS: mapreduce.framework.name -> yarn
PASS: mapreduce.jobhistory.address -> namenode:10020
PASS: mapreduce.task.io.sort.mb -> 256
PASS: yarn.resourcemanager.address -> namenode:8032
PASS: yarn.nodemanager.resource.memory-mb -> 8192
PASS: yarn.scheduler.minimum-allocation-mb -> 1024
PASS: HDFS replication factor -> 2
```

설정값이 일치하지 않는 경우:

```text
FAIL: fs.defaultFS -> hdfs://namenode:8020
Expected: hdfs://namenode:9000
```

---

## 9. 과제 수행 결과

이번 과제를 통해 다음 내용을 확인할 수 있다.

- Hadoop 클러스터는 여러 설정 파일이 역할별로 분리되어 있다.
- `core-site.xml`은 공통 파일 시스템 설정을 담당한다.
- `hdfs-site.xml`은 HDFS 저장 및 복제 정책을 담당한다.
- `mapred-site.xml`은 MapReduce 실행 환경을 담당한다.
- `yarn-site.xml`은 클러스터 자원 관리 정책을 담당한다.
- 설정 파일을 수정한 뒤에는 관련 Hadoop 서비스를 재시작해야 한다.
- 설정 파일의 값만 확인하는 것보다 실제 HDFS와 YARN 동작까지 검증해야 변경 사항이 정상적으로 적용되었는지 알 수 있다.

---

## 10. 제출 파일

다음 파일을 제출한다.

- 설정 변경 스크립트 1개
- 설정 검증 스크립트 1개
- 원본 Hadoop 설정 파일 4개
- 수정된 Hadoop 설정 파일 4개
- 클러스터 실행에 필요한 `docker-compose.yml`
- Hadoop 서비스 실행 스크립트
- 실행 방법과 검증 결과를 설명한 `README.md`

---

## 11. 팀 활동

다음 Hadoop 설정 파일을 팀원별로 나누어 살펴본다.

- `core-site.xml`
- `hdfs-site.xml`
- `mapred-site.xml`
- `yarn-site.xml`

각 파일에서 중요하거나 유용하다고 생각되는 설정을 선택해 다음 내용을 조사한다.

- 설정의 역할
- 설정값 변경에 따른 영향
- 실제 클러스터 운영에서 사용하는 상황
- 잘못 설정했을 때 발생할 수 있는 문제

조사한 내용을 팀원들과 공유하고, 논의한 결과를 팀 위키에 정리한다.