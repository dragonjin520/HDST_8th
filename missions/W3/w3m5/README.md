# W3M5 - MovieLens 평균 평점 계산

## 1. 학습 목표

Apache Hadoop과 MapReduce의 동작 원리를 이해하고, Python으로 Hadoop Streaming 작업을 작성하여 MovieLens 20M 데이터셋의 영화별 평균 평점을 계산한다.

이번 과제에서는 다음 과정을 직접 수행한다.

- MovieLens 20M 데이터셋 준비
- Mapper와 Reducer 구현
- 로컬 환경에서 동작 검증
- 입력 데이터를 HDFS에 업로드
- Hadoop Streaming 작업 실행
- HDFS에 저장된 결과 조회 및 검증

---

## 2. 과제 목표

`ratings.csv`의 각 평점 데이터를 읽어 영화별 평균 평점을 계산한다.

입력 데이터 형식은 다음과 같다.

```text
userId,movieId,rating,timestamp
1,2,3.5,1112486027
1,29,3.5,1112484676
```

Mapper는 `movieId`와 `rating`을 추출한다.

```text
2\t3.5
29\t3.5
```

Reducer는 같은 영화 ID의 평점을 모두 합산한 뒤 평균을 계산한다.

```text
1\t3.921240
2\t3.211977
3\t3.151040
```

---

## 3. 디렉터리 구조

Data, Code, Config, Output의 역할을 분리하여 다음과 같이 구성한다.

```text
w3m5/
├── README.md
├── config.env
├── data/
│   └── ml-20m/
│       └── ratings.csv
├── output/
│   └── local_average_ratings.txt
└── src/
    ├── mapper.py
    └── reducer.py
```

각 디렉터리와 파일의 역할은 다음과 같다.

| 구분 | 역할 |
|---|---|
| `data/` | MovieLens 원본 데이터 저장 |
| `src/` | Mapper와 Reducer 코드 저장 |
| `output/` | 로컬 테스트 결과 저장 |
| `config.env` | HDFS 입력 및 출력 경로 관리 |
| `README.md` | 실행 과정과 결과 기록 |

---

## 4. 전체 진행 흐름

```text
MovieLens 20M 데이터셋 다운로드
        ↓
ratings.csv 데이터 구조 확인
        ↓
Mapper 구현
        ↓
Reducer 구현
        ↓
로컬 환경에서 Mapper/Reducer 테스트
        ↓
ratings.csv를 HDFS에 업로드
        ↓
Hadoop Streaming 작업 제출
        ↓
YARN과 Hadoop Web UI에서 작업 상태 확인
        ↓
HDFS 출력 결과 조회
        ↓
로컬 계산 결과와 비교하여 검증
```

---

## 5. 단계별 진행 과정

### 5.1 작업 디렉터리 생성

`w3m5` 디렉터리에서 다음 명령어를 실행한다.

```bash
mkdir -p data src output
touch src/mapper.py
touch src/reducer.py
touch config.env
```

---

### 5.2 MovieLens 20M 데이터셋 다운로드

MovieLens 20M 데이터셋을 다운로드한다.

```bash
curl -L \
  https://files.grouplens.org/datasets/movielens/ml-20m.zip \
  -o data/ml-20m.zip
```

압축을 해제한다.

```bash
unzip data/ml-20m.zip -d data
```

압축 해제 결과를 확인한다.

```bash
ls -lh data/ml-20m
```

이번 과제에서 사용하는 파일은 다음과 같다.

```text
data/ml-20m/ratings.csv
```

---

### 5.3 입력 데이터 확인

파일의 앞부분을 확인한다.

```bash
head -5 data/ml-20m/ratings.csv
```

예상 결과:

```text
userId,movieId,rating,timestamp
1,2,3.5,1112486027
1,29,3.5,1112484676
1,32,3.5,1112484819
1,47,3.5,1112484727
```

각 컬럼의 의미는 다음과 같다.

| 컬럼 | 설명 |
|---|---|
| `userId` | 사용자 ID |
| `movieId` | 영화 ID |
| `rating` | 사용자가 부여한 평점 |
| `timestamp` | 평점 등록 시간 |

이번 작업에서는 `movieId`와 `rating`만 사용한다.

---

### 5.4 Mapper 구현

Mapper는 각 행을 읽고 영화 ID를 Key, 평점을 Value로 출력한다.

처리 흐름:

```text
CSV 한 줄 입력
→ 헤더 확인 및 제외
→ 컬럼 개수 검증
→ movieId와 rating 추출
→ movieId\trating 출력
```

예시 출력:

```text
2\t3.5
29\t3.5
32\t3.5
```

잘못된 입력 행은 표준 오류로 기록하고 처리 대상에서 제외한다.

---

### 5.5 Reducer 구현

Reducer는 같은 영화 ID의 평점을 모두 전달받아 평균을 계산한다.

처리 흐름:

```text
movieId별 평점 입력
→ 평점 합계 계산
→ 평점 개수 계산
→ 평균 = 합계 / 개수
→ movieId\t평균 평점 출력
```

평균 평점은 결과 비교가 쉽도록 일정한 소수점 자리로 출력한다.

---

### 5.6 로컬 테스트

Hadoop에서 실행하기 전에 Mapper와 Reducer가 정상적으로 동작하는지 로컬에서 검증한다.

```bash
python3 src/mapper.py \
  < data/ml-20m/ratings.csv \
  | sort -k1,1n \
  | python3 src/reducer.py \
  > output/local_average_ratings.txt
```

결과의 앞부분을 확인한다.

```bash
head output/local_average_ratings.txt
```

로컬 테스트를 먼저 수행하면 Hadoop 작업 제출 전에 데이터 파싱 오류와 평균 계산 오류를 확인할 수 있다.

---

### 5.7 HDFS 입력 디렉터리 생성

Hadoop 컨테이너가 실행 중인지 확인한 뒤 HDFS 입력 디렉터리를 생성한다.

```bash
docker exec hadoop-master \
  hdfs dfs -mkdir -p /w3m5/input
```

기존 출력 디렉터리가 있다면 삭제한다.

```bash
docker exec hadoop-master \
  hdfs dfs -rm -r -f /w3m5/output
```

---

### 5.8 ratings.csv를 HDFS에 업로드

로컬 파일을 먼저 Hadoop master 컨테이너로 복사한다.

```bash
docker cp \
  data/ml-20m/ratings.csv \
  hadoop-master:/tmp/ratings.csv
```

컨테이너 내부 파일을 HDFS에 업로드한다.

```bash
docker exec hadoop-master \
  hdfs dfs -put -f /tmp/ratings.csv /w3m5/input/
```

업로드 결과를 확인한다.

```bash
docker exec hadoop-master \
  hdfs dfs -ls /w3m5/input
```

---

### 5.9 Mapper와 Reducer를 컨테이너로 복사

```bash
docker cp src/mapper.py hadoop-master:/tmp/mapper.py
docker cp src/reducer.py hadoop-master:/tmp/reducer.py
```

실행 권한을 부여한다.

```bash
docker exec hadoop-master \
  chmod +x /tmp/mapper.py /tmp/reducer.py
```

---

### 5.10 Hadoop Streaming 작업 실행

Hadoop Streaming JAR의 위치를 확인한다.

```bash
docker exec hadoop-master \
  find /opt/hadoop -name 'hadoop-streaming*.jar'
```

확인한 JAR 파일을 사용해 작업을 제출한다.

```bash
docker exec hadoop-master \
  hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /tmp/mapper.py,/tmp/reducer.py \
  -mapper 'python3 mapper.py' \
  -reducer 'python3 reducer.py' \
  -input /w3m5/input/ratings.csv \
  -output /w3m5/output
```

Hadoop Streaming JAR는 Python 스크립트를 Hadoop MapReduce 작업의 Mapper와 Reducer로 실행할 수 있도록 연결하는 역할을 한다.

---

### 5.11 작업 상태 확인

명령행에서 YARN 애플리케이션 목록을 확인한다.

```bash
docker exec hadoop-master yarn application -list -appStates ALL
```

웹 인터페이스에서도 실행 상태를 확인할 수 있다.

- HDFS NameNode UI: `http://localhost:9870`
- YARN ResourceManager UI: `http://localhost:8088`

---

### 5.12 HDFS 결과 조회

출력 디렉터리를 확인한다.

```bash
docker exec hadoop-master \
  hdfs dfs -ls /w3m5/output
```

결과 앞부분을 출력한다.

```bash
docker exec hadoop-master \
  hdfs dfs -cat '/w3m5/output/part-*' \
  | head
```

전체 결과를 로컬 파일로 저장하려면 다음 명령어를 사용한다.

```bash
docker exec hadoop-master \
  hdfs dfs -cat '/w3m5/output/part-*' \
  > output/hdfs_average_ratings.txt
```

---

### 5.13 결과 검증

로컬 실행 결과와 Hadoop 실행 결과를 비교한다.

```bash
diff \
  output/local_average_ratings.txt \
  output/hdfs_average_ratings.txt
```

출력 순서나 소수점 형식이 다를 수 있으므로 필요하면 두 결과를 정렬한 뒤 비교한다.

```bash
sort -k1,1n output/local_average_ratings.txt \
  > output/local_average_ratings_sorted.txt

sort -k1,1n output/hdfs_average_ratings.txt \
  > output/hdfs_average_ratings_sorted.txt

 diff \
  output/local_average_ratings_sorted.txt \
  output/hdfs_average_ratings_sorted.txt
```

`diff` 결과가 없으면 두 파일의 내용이 동일하다는 의미이다.

---

## 6. 기능 요구사항

### Data Processing

- HDFS에서 입력 데이터를 읽어야 한다.
- Mapper는 영화 ID와 평점을 Key-Value 형태로 출력해야 한다.
- Reducer는 영화별 평점을 집계하고 평균을 계산해야 한다.

### Job Execution

- Hadoop 클러스터에서 오류 없이 실행되어야 한다.
- MovieLens 20M의 전체 `ratings.csv`를 처리할 수 있어야 한다.

### Result Output

- 결과는 HDFS에 텍스트 파일로 저장되어야 한다.
- 각 행은 영화 ID와 평균 평점으로 구성되어야 한다.
- 결과는 HDFS 명령어로 조회할 수 있어야 한다.

---

## 7. 예상 결과

출력 형식은 다음과 같다.

```text
1\t3.921240
2\t3.211977
3\t3.151040
...
```

각 행의 의미는 다음과 같다.

```text
movieId    averageRating
```

예를 들어 다음 결과는 영화 ID가 `1`인 영화의 평균 평점이 약 `3.921240`이라는 의미이다.

```text
1\t3.921240
```

---

## 8. 제출 파일

```text
w3m5/
├── README.md
├── config.env
├── src/
│   ├── mapper.py
│   └── reducer.py
└── output/
    ├── local_average_ratings.txt
    └── hdfs_average_ratings.txt
```

MovieLens 원본 데이터는 용량이 크므로 Git 저장소에 포함하지 않고 `.gitignore`에 등록한다.

```gitignore
data/ml-20m/
data/ml-20m.zip
```

---

## 9. 완료 기준

- [ ] MovieLens 20M 데이터셋을 다운로드했다.
- [ ] `ratings.csv`의 컬럼 구조를 확인했다.
- [ ] Mapper를 구현했다.
- [ ] Reducer를 구현했다.
- [ ] 로컬 테스트를 통과했다.
- [ ] 입력 데이터를 HDFS에 업로드했다.
- [ ] Hadoop Streaming 작업을 성공적으로 실행했다.
- [ ] HDFS 결과를 조회했다.
- [ ] 로컬 결과와 Hadoop 결과를 비교했다.
- [ ] 최종 실행 결과를 README에 기록했다.
