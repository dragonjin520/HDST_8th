# W3M1과 W3M2의 차이점

W3M1과 W3M2는 모두 Docker 환경에서 Hadoop을 실행하지만, 확인하려는 범위가 다르다.

## W3M1

W3M1의 중심은 **HDFS의 저장 구조와 데이터 영속성 확인**이다.

## W3M2

W3M2의 중심은 **여러 Hadoop 노드로 클러스터를 구성하고 분산 저장과 분산 처리를 검증하는 것**이다.

- Master 노드에서 NameNode와 ResourceManager를 실행한다.
- Worker 노드에서 DataNode와 NodeManager를 실행한다.
- HDFS를 이용해 데이터를 분산 저장한다.
- YARN이 Worker 노드에 작업을 배정한다.
- MapReduce 작업을 실행해 실제 분산 처리가 가능한지 확인한다.

```text
Master Node
├── NameNode
└── ResourceManager

Worker Node
├── DataNode
└── NodeManager
```

> 여러 노드가 하나의 Hadoop 클러스터로 연결되어 저장과 연산을 분산해서 수행할 수 있는가?

## 비교

| 구분 | W3M1 | W3M2 |
|---|---|---|
| 중심 주제 | HDFS 저장 구조와 복구 | 멀티 노드 Hadoop 클러스터 구축 |
| 주요 서비스 | NameNode, DataNode | NameNode, DataNode, ResourceManager, NodeManager |
| 주요 기능 | 파일 저장, 조회, 재시작 후 복구 | 파일 분산 저장, 작업 분배, MapReduce 실행 |
| Docker 사용 | HDFS 컨테이너와 Volume 확인 | 여러 컨테이너를 각각 하나의 노드처럼 구성 |
| 검증 포인트 | 데이터가 Volume에 유지되는가 | Worker가 클러스터에 참여하고 작업을 처리하는가 |

# Docker에서 컨테이너를 이용하는 방법

Docker는 컨테이너마다 독립된 실행 환경을 제공한다. 따라서 한 컨테이너 안에서 여러 Hadoop 노드를 실행하는 것이 아니라, **여러 컨테이너를 각각 하나의 Hadoop 노드처럼 사용한다.**

```text
Docker Host
├── master 컨테이너 = Hadoop Master 노드
├── worker1 컨테이너 = Hadoop Worker 노드 1
└── worker2 컨테이너 = Hadoop Worker 노드 2
```

실제 Hadoop 환경에서는 여러 대의 서버를 연결하지만, 이번 과제에서는 여러 컨테이너가 각각 서버의 역할을 대신한다.

| 실제 Hadoop 환경 | Docker 실습 환경 |
|---|---|
| 물리 서버 1대 | 컨테이너 1개 |
| 서버 운영체제 | 컨테이너 실행 환경 |
| 서버 IP 주소 | 컨테이너 이름 또는 내부 IP |
| 서버 간 네트워크 | Docker Network |
| 서버 디스크 | Docker Volume |


# 진행 흐름

```text
1. Hadoop Master와 Worker용 Docker 이미지 생성
2. Docker Compose로 Master와 Worker 컨테이너 실행
3. 모든 컨테이너를 같은 Docker Network에 연결
4. Master에서 NameNode와 ResourceManager 실행
5. Worker에서 DataNode와 NodeManager 실행
6. Worker가 Master에 자신을 등록했는지 확인
7. HDFS 디렉터리 생성 및 파일 업로드
8. YARN을 이용해 MapReduce 작업 실행
9. HDFS와 YARN Web UI에서 클러스터 상태 확인
10. 컨테이너 재시작 후 HDFS 데이터 유지 여부 확인
```

이번 과제의 전체 구조는 다음과 같다.

```text
Docker Host
│
├── master 컨테이너
│   ├── NameNode
│   └── ResourceManager
│
├── worker1 컨테이너
│   ├── DataNode
│   └── NodeManager
│
├── Docker Network
│   └── Master와 Worker 간 통신
│
└── Docker Volumes
    ├── NameNode 메타데이터 유지
    └── DataNode 블록 데이터 유지
```

