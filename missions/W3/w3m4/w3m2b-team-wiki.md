# W3M2b 팀 활동

**과제 내용**: `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml` 4개
파일을 살펴보고, 중요하거나 유용하다고 생각되는 설정을 각자 골라 용법을 파악한 뒤 팀
토의 내용을 정리.

---

## 1. 활동 개요

본 과제에서는 하둡의 주요 설정 파일인 `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`을 살펴보고, 중요하거나 유용한 설정을 선정하여 용법을 파악한 후 팀 토의를 통해 내용을 정리하였다.

---

## 2. Hadoop 설정 파일의 역할

하둡 시스템은 여러 설정 파일로 구성되어 있으며, 각 파일은 역할에 따라 분리되어 있다. 하지만 실행 시점에는 이들 설정이 하나의 통합 설정 풀로 합쳐져 하둡 프레임워크가 참조한다.

---

## 3. core-site.xml — 하둡 코어 및 공통 설정

하둡 시스템 전체(HDFS, MapReduce, YARN)가 공통으로 참조하는 기본 정보와 입출력 설정.

### 현재 설정 중 중요한 것

| 설정 | 값 | 설명 |
|---|---|---|
| `fs.defaultFS` | `hdfs://localhost:9000` | 하둡의 기본 파일시스템 이름과 NameNode 주소. 모든 HDFS 셸 명령어·API가 이 주소를 이정표 삼아 NameNode를 찾아감 |
| `hadoop.http.staticuser.user` | `root` | HDFS 웹 UI에서 파일 업로드·삭제 시 기본 사용자. 없으면 웹에서 파일 목록 볼 때 권한 문제(Permission Denied)로 브라우징이 안 되는 경우가 흔함 |

### 추가하면 유용한 설정 (추천)

| 설정 | 왜 유용한가 |
|---|---|
| `hadoop.tmp.dir` | 하둡 동작 중 쓰는 임시 파일 경로. 기본값 `/tmp/hadoop-${user.name}`은 리눅스 재부팅 시 `/tmp` 내용이 지워지면 하둡 메타데이터가 통째로 깨질 위험이 있음. 안정적인 경로로 수동 지정하는 것이 모범 설계(Best Practice) |
| `io.file.buffer.size` (131072, 128KB) | 하둡 파일 I/O 버퍼 크기. 기본값(4KB)은 대용량 파일 처리에 너무 작아, 보통 128KB로 늘리면 읽기/쓰기 성능이 눈에 띄게 개선됨 |

### 읽을거리 — `fs.trash.interval`

HDFS에서 삭제한 파일을 즉시 영구 삭제하지 않고 `.Trash` 디렉터리에 보관하는 시간(분 단위)
설정. 기본값 0이면 휴지통 기능 비활성화 상태. 운영 환경에서는 실수로 삭제한 파일을 복구할
수 있는 안전장치로 활용 가능. (`.Trash`는 HDFS 내부의 논리적인 디렉터리)

---

## 4. hdfs-site.xml — HDFS 저장소 설정

NameNode/DataNode 및 데이터 복제(Replication) 정책 등 저장 공간 관련 설정.

### 현재 설정 중 중요한 것

| 설정 | 값 | 설명 |
|---|---|---|
| `dfs.replication` | `1` | 복제본 개수. 기본값 3으로 두면 단일 노드 환경에서 "복제본 저장할 노드가 부족하다"는 경고(Under-replicated blocks)가 계속 뜨므로, 1로 설정해 이를 예방 |
| `dfs.namenode.name.dir` / `dfs.datanode.data.dir` | - | 메타데이터와 실제 데이터 블록이 저장될 경로. Docker 볼륨과 매핑되는 핵심 디렉터리 |

### 추가하면 유용한 설정 (추천)

| 설정 | 왜 유용한가 |
|---|---|
| `dfs.permissions.enabled` (`false`) | HDFS 내부 권한 검사 비활성화. 로컬 학습/개발 환경에서 외부 호스트나 자바 앱이 HDFS 접근 시 권한 에러(PermissionDeniedException)가 흔히 발생하는데, 이 옵션을 끄면 개발에만 집중 가능 (**운영 환경에서는 절대 끄면 안 됨**) |
| `dfs.blocksize` (134217728, 128MB) | HDFS 파일의 블록 단위 크기. 하둡 3.x 기본값은 128MB. 분석 데이터의 평균 크기가 매우 크면 256MB(268435456) 등으로 늘려 NameNode의 메타데이터 관리 메모리 부담을 덜 수 있음 |

### 읽을거리 — `dfs.blocksize`의 트레이드오프

블록 크기가 **너무 작으면** 블록 수와 NameNode의 메타데이터 관리 부담이 증가한다.
반대로 **너무 크면** 처리할 블록 수가 줄어들어 MapReduce 작업의 병렬성이 낮아질 수 있다.
따라서 데이터 크기와 처리 방식에 맞게 적절한 블록 크기를 설정해야 한다.

---

## 5. yarn-site.xml — 리소스 관리 및 스케줄러 설정

클러스터의 자원(CPU, Memory)을 효율적으로 분배·스케줄링하는 설정.

### 현재 설정 중 중요한 것

| 설정 | 설명 |
|---|---|
| `yarn.nodemanager.aux-services` (`mapreduce_shuffle`) | MapReduce 작업이 데이터를 정렬·교환(Shuffle)하는 서비스를 활성화. 맵리듀스 구동 필수 값 |

### 메모리 설정 3종 세트 (사무실 비유로 이해하기)

| 설정 | 값(예시) | 비유 | 역할 |
|---|---|---|---|
| `yarn.nodemanager.resource.memory-mb` | 2048 | 사무실의 **총 평수** | 이 노드에서 YARN이 쓸 수 있는 총 물리 메모리 한계선. 서버에 실제 RAM 16GB가 있어도 이 값이 2048(2GB)이면 YARN은 딱 2GB까지만 씀 |
| `yarn.scheduler.maximum-allocation-mb` | 2048 | **가장 큰 방의 최대 크기** | 컨테이너 하나가 요청할 수 있는 메모리 상한. 어떤 작업이 3GB짜리 방을 요청하면, 최대치가 2GB이므로 예외(`InvalidResourceRequestException`)를 던지고 거절함 |
| `yarn.scheduler.minimum-allocation-mb` | 256 | **가장 작은 방의 크기 (배수 단위)** | 컨테이너에 줄 수 있는 최소 메모리 단위이자 청크(chunk) 크기. 100MB만 필요해도 최소 단위인 256MB를 강제로 받고, 300MB를 요청하면 256MB의 배수인 512MB를 받음 |

YARN 메모리 설정 3종(`resource.memory-mb`, `scheduler.maximum-allocation-mb`, `scheduler.minimum-allocation-mb`)은 "사무실 총 평수 / 가장 큰 방 / 가장 작은 방 단위"로 이해하면 관계가 명확해진다.

### 추가하면 유용한 설정 (추천)

| 설정 | 왜 유용한가 |
|---|---|
| `yarn.nodemanager.vmem-check-enabled` (`false`) | 가상 메모리 검사 비활성화. JVM이 메모리를 과다 예약해서 컨테이너가 억울하게 강제 종료(Kill)당하는 걸 예방하려고, Docker·로컬 실습 환경에서는 보통 꺼둠 |
| `yarn.nodemanager.resource.cpu-vcores` | YARN이 쓸 가상 CPU 코어(vCore) 개수 한계. 기본값은 보통 8개인데, 내 PC 코어 수(2~4개)에 맞춰 제한해두면 하둡이 CPU 전체를 먹통으로 만드는 걸 방지 |

### 읽을거리 — `yarn.scheduler.minimum-allocation-mb`

ResourceManager가 컨테이너 하나에 할당할 수 있는 최소 메모리 단위. 작업이 이 값보다
적은 메모리를 요청해도 최소 설정값만큼 할당된다. 값이 너무 크면 작은 작업에서 메모리가
낭비될 수 있고, 너무 작으면 컨테이너 수가 많아져 스케줄링 부담이 증가할 수 있다.

---

## 6. mapred-site.xml — MapReduce 실행 설정

데이터 분석 작업을 수행하는 MapReduce 프레임워크의 상세 설정.

### 현재 설정 중 중요한 것

| 설정 | 값 | 설명 |
|---|---|---|
| `mapreduce.framework.name` | `yarn` | MapReduce 작업 실행을 로컬 CPU가 아니라 YARN ResourceManager에 맡기겠다는 선언. 이 값이 있어야 YARN 대시보드(8088)에서 작업 진행 상황 모니터링 가능 |

### 추가하면 유용한 설정 (추천)

| 설정 | 왜 유용한가 |
|---|---|
| `mapreduce.map.memory.mb` / `mapreduce.reduce.memory.mb` | Map/Reduce 각 컨테이너가 쓸 메모리. 기본값(보통 1024MB 이상)이 너무 크면, 개인 PC의 가벼운 YARN 환경(`resource.memory-mb` 2GB 수준)에서 맵 태스크 2개만 떠도 메모리가 꽉 차 다른 작업이 멈추는(Deadlock) 문제 발생. 단일 노드 실습 환경에서는 512MB 정도로 슬림하게 줄이는 게 병렬 처리에 유리 |
| JobHistory Server 관련 설정 | 작업 종료 후 YARN 웹 UI(8088)에서는 상세 로그 확인이 어려움. JobHistory Server를 켜두면 완료된 작업의 통계·실행시간·세부로그를 웹 UI(19888)에서 완벽히 추적 가능 (`entrypoint.sh`에 `mapred --daemon start historyserver` 추가 필요) |

### 읽을거리 — Map/Reduce 메모리 설정

- `mapreduce.map.memory.mb`: Map 태스크 하나가 YARN에 요청하는 컨테이너 메모리 크기
- `mapreduce.reduce.memory.mb`: Reduce 태스크 하나가 YARN에 요청하는 컨테이너 메모리 크기

Reducer는 여러 Mapper의 중간 결과를 가져와 정렬·병합하는 Shuffle 작업을 수행하므로,
작업 특성에 따라 Map 태스크보다 더 많은 메모리가 필요할 수 있다.


---

## 7. 팀 논의 종합

팀원들은 개별 설정값을 단순히 나열하기보다 저장 안정성, 데이터 복제, 자원 할당, 작업 실행, 운영 관찰의 관점에서 설정 간 관계를 함께 살펴보았다. 특히 Docker 기반 실습 환경에서는 데이터 디렉터리의 볼륨 연결, 클러스터 규모에 맞는 복제 수, 제한된 메모리 안에서의 컨테이너 할당, 실제 작업 실행을 통한 검증이 중요하다는 데 의견을 모았다.

---

## 8. 검증 과정에서 발생한 문제와 두 가지 의문점

### 의문점 1. YARN과 MapReduce 설정을 왜 `hdfs getconf`로 조회하는가?

하둡 프레임워크는 내부적으로 자바(Java)의 `Configuration` 클래스로 설정을 관리한다. 사용자 입장에선 `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`이 각각 분리된 파일처럼 보이지만, 하둡 시스템이 구동되거나 명령어가 실행될 때는 이 4개 파일 내용을 전부 긁어모아 하나의 거대한 메모리 공간(설정 풀)에 통째로 올린다. 따라서 하둡 명령어가 실행되면, 그 내부에서는 이게 HDFS 설정인지 YARN 설정인지 구별하지 않고 하나의 거대한 키-값(Key-Value) 딕셔너리에서 값을 찾는다.

"설정 풀에서 특정 키(`mapreduce.framework.name`)의 값을 꺼내와라"라는 기능을 하는 도구가 `getconf`다. 하둡 개발자들이 이 조회 도구(`GetConf` 클래스)를 처음 만들 때, 하둡의 가장 기본 뼈대인 `hdfs` 명령어의 하위 도구로 패키징해두었다.

- `yarn getconf`나 `mapred getconf`라는 명령어는 아예 존재하지 않는다.
- 오직 `hdfs getconf`만 존재한다.

간단히 비유하자면, 윈도우의 "작업 관리자"가 시스템 전체 정보를 통합해서 보여주듯, `hdfs getconf`는 하둡 설정 전체를 통합 조회하는 도구이다.

즉, `hdfs getconf`는 이름과 달리 HDFS 설정만 읽는 명령이 아니라, 현재 로드된 Hadoop 전체 설정에서 특정 Key의 값을 조회하는 도구로 이해할 수 있다.

---

### 의문점 2. YARN과 MapReduce의 자원 요청이 충돌하면 어떤 설정이 우선하는가?

`yarn-site.xml`은 클러스터 전체에서 허용할 수 있는 자원 범위를 정하고, `mapred-site.xml`은 개별 Map 또는 Reduce 태스크가 요청할 자원량을 정한다.

따라서 두 설정이 충돌할 경우에는 YARN의 스케줄러 정책이 상위 제약 조건으로 적용된다. MapReduce 태스크는 원하는 메모리를 요청할 수 있지만, 그 요청값은 반드시 YARN이 허용한 최소·최대 범위 안에 있어야 한다.

```text
yarn.scheduler.minimum-allocation-mb
    ≤ mapreduce.reduce.memory.mb
    ≤ yarn.scheduler.maximum-allocation-mb
```

Map 태스크도 동일한 관계를 만족해야 한다.

```text
yarn.scheduler.minimum-allocation-mb
    ≤ mapreduce.map.memory.mb
    ≤ yarn.scheduler.maximum-allocation-mb
```

예를 들어 `yarn.scheduler.maximum-allocation-mb`가 `2048`인데 Reduce 태스크가 `mapreduce.reduce.memory.mb=3072`를 요청하면, ResourceManager는 허용 범위를 초과한 요청으로 판단하여 작업을 거부한다.

반대로 MapReduce 태스크가 YARN의 최소 할당량보다 작은 메모리를 요청하면, 실제 할당은 YARN의 최소 단위에 맞춰 조정될 수 있다. 예를 들어 최소 할당량이 `256MB`인 환경에서 태스크가 `100MB`를 요청하더라도 실제 컨테이너는 최소 `256MB`를 할당받는다.

즉, MapReduce는 태스크별 희망 자원량을 요청하고, YARN은 클러스터 전체 정책에 따라 그 요청을 허용·조정·거부한다. 따라서 자원 설정의 최종 우선권은 YARN에 있다.

---

## 9. 종합 결론

- 4개 XML 파일은 역할별로 분리되어 있지만, 실행 시점에는 하나의 설정 풀로 합쳐진다.
- `hdfs getconf`는 HDFS 전용이 아니라 하둡 전체 설정을 조회하는 공용 도구이며, `yarn getconf`나 `mapred getconf`는 존재하지 않는다.
- YARN 메모리 설정 3종은 사무실 공간 비유를 통해 이해하면 관계가 명확해진다.
- MapReduce 태스크의 메모리 요청은 YARN이 정한 최소·최대 할당 범위 안에서만 허용되므로, 두 설정이 충돌할 경우 최종 자원 정책은 YARN이 결정한다.
