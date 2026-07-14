# W3M3 Hadoop MapReduce Word Count

## 1. 학습 목표

Python으로 Mapper와 Reducer를 구현하고, Hadoop Streaming을 이용해 HDFS에 저장된 영문 전자책의 단어 빈도를 계산한다.

이번 과제를 통해 다음 내용을 확인한다.

- HDFS에 입력 데이터를 업로드하고 조회하는 방법
- Mapper가 단어별 `(word, 1)` 형태의 key-value를 생성하는 과정
- Hadoop의 Shuffle/Sort 과정
- Reducer가 단어별 값을 합산하는 과정
- MapReduce 작업 실행 및 결과 검증 방법

---

## 2. 과제 처리 흐름

```text
영문 전자책 준비
    ↓
Project Gutenberg 안내문 제거
    ↓
HDFS 입력 디렉터리에 업로드
    ↓
Mapper 실행: 각 단어를 "word\t1" 형태로 출력
    ↓
Hadoop Shuffle/Sort: 동일한 단어끼리 정렬 및 그룹화
    ↓
Reducer 실행: 단어별 등장 횟수 합산
    ↓
HDFS 출력 디렉터리에 결과 저장
    ↓
로컬 결과와 Hadoop 결과 비교
```

---

## 3. 프로젝트 구조

```text
w3m3/
├── README.md
├── data/
│   ├── Frankenstein.txt
│   └── Frankenstein_body.txt
├── src/
│   ├── mapper.py
│   └── reducer.py
└── output/
    ├── local_wordcount.txt
    └── hadoop_wordcount.txt
```

| 경로 | 역할 |
|---|---|
| `data/Frankenstein.txt` | Project Gutenberg에서 내려받은 원문 |
| `data/Frankenstein_body.txt` | Gutenberg 안내문을 제거한 소설 본문 |
| `src/mapper.py` | 입력 문장을 단어 단위 key-value로 변환 |
| `src/reducer.py` | 같은 단어의 값을 합산 |
| `output/local_wordcount.txt` | 로컬 파이프라인 실행 결과 |
| `output/hadoop_wordcount.txt` | Hadoop Streaming 실행 결과 |

---

## 4. 사용한 전자책

- 제목: `Frankenstein; or, the Modern Prometheus`
- 저자: `Mary Wollstonecraft Shelley`
- 출처: `Project Gutenberg`
- 원본 URL: `https://www.gutenberg.org/ebooks/84`
- 원본 파일 크기: `448,885 bytes`
- 원본 줄 수: `7,741줄`
- 원본 단어 수: `78,106개`

원문에서 Project Gutenberg 시작·종료 안내문을 제외하고 소설 본문만 추출했다.

```bash
sed -n '29,7390p' data/Frankenstein.txt \
  > data/Frankenstein_body.txt
```

---

## 5. 실행 환경

본 과제는 W3M2-B에서 구축한 Hadoop 멀티 노드 클러스터를 재사용하여 실행했다.

- Hadoop 실행환경: `missions/W3/w3m2_b`
- MapReduce 코드·데이터·결과: `missions/W3/w3m3`
- 클러스터 실행: `w3m2_b/docker-compose.yml`
- Mapper, Reducer, 입력 파일은 `w3m3`에서 작성한 뒤 master 컨테이너와 HDFS로 전달

| 구분 | 구성 |
|---|---|
| Hadoop | `3.3.6` |
| Python | `3.10.12` |
| Master | NameNode, ResourceManager |
| Worker 1 | DataNode, NodeManager |
| Worker 2 | DataNode, NodeManager |

Hadoop Streaming에서 Python Mapper와 Reducer를 실행할 수 있도록 공통 base 이미지에 Python을 포함해 다시 빌드했다. W3M2-B는 Hadoop 인프라를 담당하고, W3M3는 Word Count 애플리케이션과 실행 결과를 담당한다.

---

## 6. Mapper 동작 원리

Mapper는 표준 입력으로 전달된 텍스트를 한 줄씩 읽고, 각 단어를 다음 형식으로 출력한다.

```text
word\t1
```

Mapper에서는 다음 전처리를 수행한다.

- 모든 문자를 소문자로 변환
- 문장부호 제거
- 영문 단어만 추출
- apostrophe가 포함된 단어는 하나의 단어로 처리

예시 입력:

```text
Hello, Hadoop! Don't stop. Hello.
```

예시 출력:

```text
hello\t1
hadoop\t1
don't\t1
stop\t1
hello\t1
```

---

## 7. Shuffle/Sort 동작 원리

Mapper가 출력한 key-value는 Hadoop의 Shuffle/Sort 단계를 거치며, 동일한 단어를 기준으로 정렬 및 그룹화된다.

```text
hello → [1, 1]
hadoop → [1]
```

이 과정은 별도의 Python 코드로 구현하지 않고 Hadoop이 자동으로 처리한다.

---

## 8. Reducer 동작 원리

Reducer는 정렬된 Mapper 결과를 입력받아 동일한 단어의 값을 합산한다.

```text
hello\t2
hadoop\t1
```

최종 출력의 각 행은 다음 형식을 가진다.

```text
단어\t등장 횟수
```

---

## 9. 로컬 단위 테스트

Hadoop 작업을 실행하기 전에 Mapper와 Reducer를 로컬에서 검증했다.

```bash
echo "Hello Hadoop hello Hadoop MapReduce" \
  | python3 src/mapper.py \
  | sort \
  | python3 src/reducer.py
```

결과:

```text
hadoop\t2
hello\t2
mapreduce\t1
```

로컬 테스트의 `sort` 명령은 Hadoop의 Shuffle/Sort 단계를 대신한다.

---

## 10. 프랑켄슈타인 전체 로컬 실행

```bash
mkdir -p output

python3 src/mapper.py < data/Frankenstein_body.txt \
  | sort \
  | python3 src/reducer.py \
  > output/local_wordcount.txt
```

고유 단어 수 확인:

```bash
wc -l output/local_wordcount.txt
```

---

## 11. HDFS 입력 파일 업로드

로컬 파일을 먼저 master 컨테이너로 복사했다.

```bash
docker cp data/Frankenstein_body.txt \
  hadoop-master:/tmp/Frankenstein_body.txt
```

HDFS 입력 디렉터리를 생성하고 파일을 업로드했다.

```bash
docker exec hadoop-master \
  hdfs dfs -mkdir -p /w3m3/input

docker exec hadoop-master \
  hdfs dfs -put -f /tmp/Frankenstein_body.txt /w3m3/input/
```

업로드 확인:

```bash
docker exec hadoop-master \
  hdfs dfs -ls -h /w3m3/input
```

HDFS 입력 경로:

```text
/w3m3/input/Frankenstein_body.txt
```

---

## 12. Mapper와 Reducer 전달

```bash
docker cp src/mapper.py hadoop-master:/tmp/mapper.py
docker cp src/reducer.py hadoop-master:/tmp/reducer.py

docker exec hadoop-master \
  chmod +x /tmp/mapper.py /tmp/reducer.py
```

컨테이너 내부 동작 확인:

```bash
docker exec hadoop-master bash -c \
  'echo "Hello Hadoop hello Hadoop MapReduce" \
  | python3 /tmp/mapper.py \
  | sort \
  | python3 /tmp/reducer.py'
```

---

## 13. Hadoop Streaming 작업 실행

사용한 Hadoop Streaming JAR 경로:

```text
/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
```

기존 출력 디렉터리를 삭제한 뒤 작업을 실행했다.

```bash
docker exec hadoop-master \
  hdfs dfs -rm -r -f /w3m3/output

docker exec hadoop-master \
  hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -files /tmp/mapper.py,/tmp/reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /w3m3/input/Frankenstein_body.txt \
  -output /w3m3/output
```

HDFS 출력 경로:

```text
/w3m3/output
```

---

## 14. 출력 결과 조회

```bash
docker exec hadoop-master \
  hdfs dfs -ls -h /w3m3/output
```

성공한 작업에서는 다음 파일이 생성된다.

```text
/w3m3/output/_SUCCESS
/w3m3/output/part-00000
```

전체 결과 조회:

```bash
docker exec hadoop-master \
  hdfs dfs -cat /w3m3/output/part-*
```

빈도 상위 20개 단어 조회:

```bash
docker exec hadoop-master bash -c \
  'hdfs dfs -cat /w3m3/output/part-* \
  | sort -k2,2nr \
  | head -20'
```

---

## 15. 실행 결과

- MapReduce 작업 성공 여부: `성공`
- File Input Bytes Read: `476,596 bytes`
- File Output Bytes Written: `72,804 bytes`
- Reducer 출력 레코드 수: `6,977개`
- 가장 많이 등장한 단어: `the`
- `the` 등장 횟수: `4,195회`
- HDFS 결과 파일: `/w3m3/output/part-00000`
- 로컬 저장 결과: `output/hadoop_wordcount.txt`
- 로컬 검증 결과: `output/local_wordcount.txt`와 완전히 동일

### 빈도 상위 20개 단어

| 순위 | 단어 | 빈도 |
|---:|---|---:|
| 1 | the | 4,195 |
| 2 | and | 2,976 |
| 3 | i | 2,850 |
| 4 | of | 2,642 |
| 5 | to | 2,094 |
| 6 | my | 1,776 |
| 7 | a | 1,391 |
| 8 | in | 1,129 |
| 9 | was | 1,021 |
| 10 | that | 1,018 |
| 11 | me | 868 |
| 12 | but | 687 |
| 13 | had | 686 |
| 14 | with | 667 |
| 15 | he | 608 |
| 16 | you | 574 |
| 17 | which | 558 |
| 18 | it | 547 |
| 19 | his | 535 |
| 20 | as | 528 |

---

## 16. 결과 검증

Hadoop 결과를 하나의 파일로 병합한 뒤 로컬 결과와 비교했다.

```bash
docker exec hadoop-master \
  hdfs dfs -getmerge /w3m3/output /tmp/hadoop_wordcount.txt

docker cp \
  hadoop-master:/tmp/hadoop_wordcount.txt \
  output/hadoop_wordcount.txt

diff output/local_wordcount.txt output/hadoop_wordcount.txt
```

`diff` 명령에서 아무 결과도 출력되지 않아 로컬 결과와 Hadoop 결과가 완전히 동일함을 확인했다.

최종 Hadoop 결과 파일은 `w3m3/output/hadoop_wordcount.txt`에 저장했다. 이 파일은 W3M2-B의 실행환경에서 생성했지만, W3M3 과제의 결과물이므로 W3M3 디렉터리에 포함했다.

---

## 17. 재실행 시 주의사항

Hadoop은 출력 디렉터리가 이미 존재하면 작업을 시작하지 않는다.

재실행 전 기존 출력 디렉터리를 삭제해야 한다.

```bash
docker exec hadoop-master \
  hdfs dfs -rm -r -f /w3m3/output
```

`NativeCodeLoader` 경고는 네이티브 Hadoop 라이브러리 대신 Java 구현을 사용한다는 의미이며, 본 과제 실행에는 영향을 주지 않았다.

---

## 18. 핵심 정리

이번 과제를 통해 다음과 같은 Hadoop MapReduce 처리 흐름을 확인했다.

```text
HDFS 입력
→ Mapper
→ Shuffle/Sort
→ Reducer
→ HDFS 출력
```

Mapper와 Reducer는 각각 단순한 역할을 수행하고, 데이터 전달, 정렬, 그룹화, 작업 관리와 같은 분산 처리 과정은 Hadoop이 담당한다.