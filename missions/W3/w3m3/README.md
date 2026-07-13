# W3M3 Hadoop MapReduce Word Count

## 1. 학습 목표

Python으로 Mapper와 Reducer를 구현하고, Hadoop Streaming을 이용해 HDFS에 저장된 대용량 영문 전자책의 단어 빈도를 계산한다.

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
결과 조회 및 정확성 검증
```

---

## 3. 프로젝트 구조

```text
w3m3/
├── README.md
├── data/
│   └── book.txt
├── src/
│   ├── mapper.py
│   └── reducer.py
├── scripts/
│   ├── upload-input.sh
│   ├── run-wordcount.sh
│   └── verify-result.sh
└── output/
    └── sample-result.txt
```

| 경로 | 역할 |
|---|---|
| `data/book.txt` | MapReduce 입력으로 사용할 영문 전자책 |
| `src/mapper.py` | 입력 문장을 단어 단위 key-value로 변환 |
| `src/reducer.py` | 같은 단어의 값을 합산 |
| `scripts/upload-input.sh` | 전자책을 HDFS에 업로드 |
| `scripts/run-wordcount.sh` | Hadoop Streaming 작업 실행 |
| `scripts/verify-result.sh` | 출력 결과 조회 및 검증 |
| `output/sample-result.txt` | 실행 결과 예시 또는 검증 결과 |

---

## 4. 사용한 전자책

- 제목: `프랑켄슈타인`
- 저자: `Mary Wollstonecraft Shelley (메리 셸리)`
- 출처: `Project Gutenberg (프로젝트 구텐베르크)`
- 원본 URL: `https://www.gutenberg.org/ebooks/84`
- 분량: `단어 수: 약 75,000 단어 (영어 원문 기준)`


---

## 5. Mapper 동작 원리

Mapper는 표준 입력으로 전달된 텍스트를 한 줄씩 읽고, 각 단어를 다음 형식으로 출력한다.

```text
word\t1
```

예를 들어 다음 문장이 입력되면,

```text
hello hadoop hello
```

Mapper는 다음 결과를 출력한다.

```text
hello\t1
hadoop\t1
hello\t1
```

Mapper에서는 필요에 따라 다음 전처리를 수행한다.

- 대소문자 통일
- 문장부호 제거
- 빈 문자열 제거

---

## 6. Shuffle/Sort 동작 원리

Mapper가 생성한 결과는 Hadoop의 Shuffle/Sort 단계를 거친다.

```text
hello\t1
hello\t1
hadoop\t1
```

Hadoop은 동일한 단어를 기준으로 데이터를 정렬하고 Reducer에 전달한다.

```text
hello → [1, 1]
hadoop → [1]
```

이 과정은 별도의 Python 코드로 구현하지 않고 Hadoop이 자동으로 처리한다.

---

## 7. Reducer 동작 원리

Reducer는 정렬된 Mapper 결과를 입력받아 동일한 단어의 값을 합산한다.

```text
hello\t2
hadoop\t1
```

따라서 최종 출력의 각 행은 다음 형식을 가진다.

```text
단어\t등장 횟수
```

---

## 8. 실행 전 준비

### 8.1 Hadoop 클러스터 상태 확인

```bash
jps
```

멀티 노드 환경에서는 각 컨테이너 또는 노드에서 NameNode, DataNode, ResourceManager, NodeManager가 정상 실행 중인지 확인한다.

### 8.2 Python 스크립트 실행 권한 설정

```bash
chmod +x src/mapper.py src/reducer.py
```

각 Python 파일의 첫 줄에는 다음 shebang을 작성한다.

```python
#!/usr/bin/env python3
```

---

## 9. 로컬 단위 테스트

Hadoop 작업을 실행하기 전에 작은 입력 파일로 Mapper와 Reducer의 정확성을 확인한다.

### 9.1 테스트 입력

```text
hello hadoop hello
hadoop mapreduce
```

### 9.2 테스트 실행

```bash
cat sample.txt \
  | python3 src/mapper.py \
  | sort \
  | python3 src/reducer.py
```

로컬 테스트에서 `sort` 명령은 Hadoop의 Shuffle/Sort 단계를 대신한다.

### 9.3 예상 결과

```text
hadoop\t2
hello\t2
mapreduce\t1
```

---

## 10. HDFS 입력 파일 업로드

### 10.1 입력 디렉터리 생성

```bash
hdfs dfs -mkdir -p /w3m3/input
```

### 10.2 전자책 업로드

```bash
hdfs dfs -put data/book.txt /w3m3/input/
```

기존 파일을 덮어써야 한다면 다음 명령을 사용한다.

```bash
hdfs dfs -put -f data/book.txt /w3m3/input/
```

### 10.3 업로드 결과 확인

```bash
hdfs dfs -ls /w3m3/input
```

```bash
hdfs dfs -head /w3m3/input/book.txt
```

---

## 11. Hadoop Streaming 작업 실행

Python Mapper와 Reducer는 Hadoop Streaming JAR를 통해 실행한다.

```bash
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar" \
  -files src/mapper.py,src/reducer.py \
  -mapper mapper.py \
  -reducer reducer.py \
  -input /w3m3/input/book.txt \
  -output /w3m3/output
```

Hadoop 설치 환경에 따라 Streaming JAR의 실제 경로가 다를 수 있으므로 다음 명령으로 확인할 수 있다.

```bash
find "$HADOOP_HOME" -name "hadoop-streaming-*.jar"
```

Docker 환경에서 실행하는 경우 `docker exec` 명령을 앞에 추가하거나, 스크립트와 입력 파일을 컨테이너 내부에서 접근할 수 있도록 구성해야 한다.

---

## 12. 출력 결과 조회

### 12.1 출력 디렉터리 확인

```bash
hdfs dfs -ls /w3m3/output
```

성공한 작업에는 일반적으로 다음 파일이 생성된다.

```text
/w3m3/output/_SUCCESS
/w3m3/output/part-00000
```

Reducer 수가 여러 개라면 `part-00001`, `part-00002`와 같이 여러 결과 파일이 생성될 수 있다.

### 12.2 전체 결과 조회

```bash
hdfs dfs -cat /w3m3/output/part-*
```

### 12.3 상위 결과 일부 조회

```bash
hdfs dfs -cat /w3m3/output/part-* | head -20
```

### 12.4 빈도순 정렬

```bash
hdfs dfs -cat /w3m3/output/part-* \
  | sort -k2,2nr \
  | head -20
```

---

## 13. 결과 검증

결과 검증은 다음 순서로 진행한다.

1. 작은 테스트 입력으로 Mapper와 Reducer의 예상 결과를 확인한다.
2. Hadoop 작업 종료 상태가 성공인지 확인한다.
3. HDFS 출력 디렉터리에 `_SUCCESS` 파일이 생성되었는지 확인한다.
4. `part-*` 파일이 `단어\t횟수` 형식인지 확인한다.
5. 빈도순 상위 단어를 조회해 비정상적인 토큰이나 전처리 오류가 없는지 확인한다.

필요하면 출력 결과를 로컬 파일로 내려받아 추가 검증할 수 있다.

```bash
hdfs dfs -getmerge /w3m3/output output/wordcount-result.txt
```

---

## 14. 재실행 시 주의사항

Hadoop은 출력 디렉터리가 이미 존재하면 작업을 시작하지 않는다.

재실행 전 기존 출력 디렉터리를 삭제한다.

```bash
hdfs dfs -rm -r /w3m3/output
```

그다음 Hadoop Streaming 작업을 다시 실행한다.

입력 디렉터리는 유지할 수 있으며, 전자책을 변경한 경우에만 파일을 다시 업로드한다.

---

## 15. 실행 결과

아래 항목은 실제 실행 후 작성한다.

- MapReduce 작업 성공 여부: `작성 예정`
- 입력 파일 크기: `작성 예정`
- Mapper 입력 레코드 수: `작성 예정`
- Reducer 출력 레코드 수: `작성 예정`
- 가장 많이 등장한 단어: `작성 예정`
- 실행 결과 파일: `작성 예정`

실행 화면, Hadoop Web UI 또는 CLI 결과를 함께 기록하면 작업의 정상 수행 여부를 더욱 명확하게 확인할 수 있다.

---

## 16. 핵심 정리

이번 과제에서는 Python으로 단어 수를 직접 계산하는 것보다, 다음과 같은 Hadoop MapReduce 처리 구조를 이해하는 것이 중요하다.

```text
HDFS 입력
→ Mapper
→ Shuffle/Sort
→ Reducer
→ HDFS 출력
```

Mapper와 Reducer는 각각 단순한 역할만 수행하며, 데이터 분할, 전송, 정렬, 장애 처리와 같은 분산 처리 과정은 Hadoop이 담당한다.
