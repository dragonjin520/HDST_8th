# W3M4 Hadoop MapReduce Sentiment Analysis

## 1. 과제 개요

Apache Hadoop과 MapReduce를 활용하여 Sentiment140 트윗 데이터를 감성별로 분류하고 집계한다.

Mapper는 각 트윗을 읽어 `positive`, `negative`, `neutral` 중 하나로 분류하고, Reducer는 감성별 개수를 합산한다.

최종 결과는 HDFS에 텍스트 파일로 저장한다.

---

## 2. 학습 목표

- HDFS에 대용량 입력 데이터를 업로드하고 조회한다.
- Hadoop Streaming을 이용해 Python Mapper와 Reducer를 실행한다.
- Mapper에서 key-value 형태의 중간 결과를 생성한다.
- Shuffle & Sort 이후 Reducer에서 감성별 개수를 집계한다.
- Hadoop 작업 결과를 HDFS에서 확인하고 로컬 파일로 저장한다.

---

## 3. 데이터셋

사용 데이터는 **Twitter Data from Sentiment140**이다.

원본 파일명:

```text
training.1600000.processed.noemoticon.csv
```

주요 컬럼은 다음과 같다.

| 인덱스 | 내용 |
|---:|---|
| 0 | 감성 라벨 |
| 1 | 트윗 ID |
| 2 | 작성 시간 |
| 3 | Query |
| 4 | 사용자명 |
| 5 | 트윗 본문 |

원본 데이터는 약 227MB로 크기가 크기 때문에 Git에 포함하지 않고 `.gitignore`에서 제외한다.

```gitignore
data/*.csv
```

---

## 4. 프로젝트 구조

```text
w3m4/
├── README.md
├── .gitignore
├── config/
│   └── sentiment_keywords.json
├── data/
│   ├── README.md
│   └── training.1600000.processed.noemoticon.csv
├── output/
│   └── keyword_result.txt
├── scripts/
│   ├── run_keyword_job.sh
│   └── run_label_job.sh
└── src/
    ├── keyword_mapper.py
    ├── label_mapper.py
    └── reducer.py
```

역할은 다음과 같이 분리한다.

| 구분 | 역할 |
|---|---|
| `data/` | 원본 입력 데이터 |
| `src/` | Mapper, Reducer 코드 |
| `config/` | 감성 분류 키워드 설정 |
| `output/` | 실행 결과 저장 |
| `scripts/` | Hadoop Streaming 실행 스크립트 |

---

## 5. 처리 흐름

```text
Sentiment140 CSV
        ↓
      HDFS
        ↓
keyword_mapper.py
        ↓
positive\t1
negative\t1
neutral\t1
        ↓
Shuffle & Sort
        ↓
reducer.py
        ↓
감성별 총개수 집계
        ↓
HDFS Output
```

---

## 6. 감성 분류 기준

`config/sentiment_keywords.json`에 긍정과 부정 키워드를 정의했다.

Mapper는 트윗을 소문자로 변환하고 단어 단위로 분리한 뒤 키워드 포함 여부를 확인한다.

분류 기준은 다음과 같다.

| 조건 | 분류 결과 |
|---|---|
| 긍정 키워드만 포함 | `positive` |
| 부정 키워드만 포함 | `negative` |
| 둘 다 포함 | `neutral` |
| 둘 다 포함하지 않음 | `neutral` |

예시:

```text
I love this awesome project
→ positive

This is a terrible problem
→ negative

I went to school today
→ neutral

The design is beautiful but the service is bad
→ neutral
```

---

## 7. 로컬 테스트

Hadoop에서 실행하기 전에 작은 샘플 데이터로 Mapper와 Reducer를 검증했다.

```bash
SENTIMENT_CONFIG=config/sentiment_keywords.json \
python3 src/keyword_mapper.py < data/sample_sentiment140.csv \
  | sort \
  | python3 src/reducer.py
```

예상 결과:

```text
negative\t1
neutral\t2
positive\t1
```

---

## 8. HDFS에 입력 데이터 업로드

먼저 원본 CSV를 Hadoop master 컨테이너로 복사한다.

```bash
docker cp \
  data/training.1600000.processed.noemoticon.csv \
  hadoop-master:/tmp/sentiment140.csv
```

HDFS 입력 디렉터리를 생성한다.

```bash
docker exec hadoop-master \
  hdfs dfs -mkdir -p /w3m4/input
```

기존 입력 파일이 있다면 삭제한다.

```bash
docker exec hadoop-master \
  hdfs dfs -rm -f /w3m4/input/sentiment140.csv
```

HDFS에 데이터를 업로드한다.

```bash
docker exec hadoop-master \
  hdfs dfs -put \
  /tmp/sentiment140.csv \
  /w3m4/input/sentiment140.csv
```

업로드 결과를 확인한다.

```bash
docker exec hadoop-master \
  hdfs dfs -ls -h /w3m4/input
```

확인 결과:

```text
-rw-r--r--   2 root supergroup  227.7 M  /w3m4/input/sentiment140.csv
```

---

## 9. Mapper, Reducer, Config 전달

Hadoop 컨테이너가 다른 브랜치의 설정 파일을 bind mount하고 있어 `docker cp` 과정에서 오류가 발생했다.

이를 우회하기 위해 표준입력을 이용해 파일을 컨테이너에 전달했다.

```bash
docker exec -i hadoop-master \
  sh -c 'cat > /tmp/keyword_mapper.py' \
  < /tmp/w3m4-streaming/keyword_mapper.py

docker exec -i hadoop-master \
  sh -c 'cat > /tmp/reducer.py' \
  < /tmp/w3m4-streaming/reducer.py

docker exec -i hadoop-master \
  sh -c 'cat > /tmp/sentiment_keywords.json' \
  < /tmp/w3m4-streaming/sentiment_keywords.json
```

실행 권한을 부여한다.

```bash
docker exec hadoop-master \
  chmod +x /tmp/keyword_mapper.py /tmp/reducer.py
```

파일 전달 결과를 확인한다.

```bash
docker exec hadoop-master ls -lh \
  /tmp/keyword_mapper.py \
  /tmp/reducer.py \
  /tmp/sentiment_keywords.json
```

---

## 10. Hadoop Streaming JAR 확인

Hadoop Streaming JAR 파일 경로를 확인했다.

```bash
docker exec hadoop-master \
  find /opt/hadoop -name "hadoop-streaming*.jar"
```

사용한 JAR 경로:

```text
/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
```

---

## 11. Hadoop Streaming 실행

Hadoop은 이미 존재하는 출력 디렉터리에 결과를 저장할 수 없으므로 기존 출력 경로를 먼저 삭제한다.

```bash
docker exec hadoop-master \
  hdfs dfs -rm -r -f /w3m4/output/keyword
```

Hadoop Streaming 작업을 실행한다.

```bash
docker exec hadoop-master \
  hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -files /tmp/keyword_mapper.py,/tmp/reducer.py,/tmp/sentiment_keywords.json \
  -cmdenv SENTIMENT_CONFIG=sentiment_keywords.json \
  -mapper "python3 keyword_mapper.py" \
  -reducer "python3 reducer.py" \
  -input /w3m4/input/sentiment140.csv \
  -output /w3m4/output/keyword
```

작업 성공 시 다음 메시지를 확인할 수 있다.

```text
INFO streaming.StreamJob: Output directory: /w3m4/output/keyword
```

---

## 12. 결과 확인

HDFS 출력 디렉터리를 확인한다.

```bash
docker exec hadoop-master \
  hdfs dfs -ls -h /w3m4/output/keyword
```

결과 파일을 조회한다.

```bash
docker exec hadoop-master \
  hdfs dfs -cat "/w3m4/output/keyword/part-*"
```

zsh에서는 `*`를 로컬 shell이 먼저 해석할 수 있으므로 HDFS 경로를 따옴표로 감싸야 한다.

결과 형식:

```text
negative\t<count>
neutral\t<count>
positive\t<count>
```

결과를 로컬 파일로 저장한다.

```bash
docker exec hadoop-master \
  hdfs dfs -cat "/w3m4/output/keyword/part-*" \
  > output/keyword_result.txt
```

---

## 13. Mapper와 Reducer 역할

### Mapper

Mapper는 CSV 각 행에서 트윗 본문을 추출하고 키워드를 기준으로 감성을 분류한다.

출력 예시:

```text
positive\t1
negative\t1
neutral\t1
```

### Reducer

Reducer는 Shuffle & Sort를 거쳐 같은 감성끼리 모인 값을 합산한다.

입력 예시:

```text
positive\t1
positive\t1
positive\t1
```

출력 예시:

```text
positive\t3
```

---

## 14. 팀 활동: predefined keywords가 아닌 방법

키워드 기반 분류는 구현이 단순하지만 문맥, 반어법, 복합 감정을 정확히 판단하기 어렵다.

대안으로 Sentiment140 데이터의 원본 라벨을 직접 활용할 수 있다.

| 원본 라벨 | 감성 |
|---:|---|
| `0` | negative |
| `2` | neutral |
| `4` | positive |

이 방식은 Mapper에서 첫 번째 컬럼의 라벨을 읽고 감성 문자열로 변환한 뒤 `1`을 출력한다.

```text
0 → negative\t1
2 → neutral\t1
4 → positive\t1
```

추가로 고려할 수 있는 방법은 다음과 같다.

- TF-IDF와 Logistic Regression
- Naive Bayes
- SVM
- LSTM
- BERT 기반 감성 분류
- VADER와 같은 사전 기반 감성 점수

이번 과제에서는 기본 요구사항 충족을 위해 키워드 기반 Mapper를 우선 구현하고, 팀 활동에서는 원본 라벨 기반 방법을 비교 대상으로 사용할 수 있다.

---

## 15. 실행 중 발생한 문제와 해결

### 15.1 파일명 공백 문제

원본 CSV 파일명에 공백이 포함되어 `docker cp`가 인자를 잘못 해석했다.

해결 방법:

```bash
docker cp \
  "data/training.1600000.processed.noemoticon 2.csv" \
  hadoop-master:/tmp/sentiment140.csv
```

또는 파일명에서 공백을 제거한다.

### 15.2 브랜치 전환과 bind mount 문제

Hadoop 컨테이너는 `w3m2_b` 브랜치의 Hadoop 설정 파일을 bind mount하고 있었다.

다른 브랜치로 전환하자 설정 파일 경로가 사라져 `docker cp` 오류가 발생했다.

해결 방법:

- Hadoop 실행 시 `w3m2_b` 브랜치를 유지한다.
- W3M4 소스 파일은 `/tmp/w3m4-streaming`에 임시 저장한다.
- `docker exec -i`와 표준입력을 사용해 컨테이너로 전달한다.

### 15.3 zsh wildcard 문제

다음 명령은 zsh가 `*`를 먼저 해석해 오류가 발생했다.

```bash
hdfs dfs -cat /w3m4/output/keyword/part-*
```

해결 방법:

```bash
hdfs dfs -cat "/w3m4/output/keyword/part-*"
```

---

## 16. 결과 해석

출력 결과는 각 감성 카테고리로 분류된 트윗의 총개수를 의미한다.

예시:

```text
negative\t1200
neutral\t1800
positive\t1500
```

위 결과는 부정 트윗 1,200개, 중립 트윗 1,800개, 긍정 트윗 1,500개가 분류되었다는 뜻이다.

키워드 기반 결과는 실제 감성 정답이 아니라 사전에 정의한 키워드 규칙에 따른 분류 결과이므로, 정확도보다는 Hadoop MapReduce 처리 흐름을 이해하는 데 목적이 있다.

---

## 17. 정리

이번 과제를 통해 다음 과정을 수행했다.

1. Sentiment140 원본 데이터를 HDFS에 업로드했다.
2. Python으로 Mapper와 Reducer를 구현했다.
3. 감성 키워드를 Config 파일로 분리했다.
4. 로컬 환경에서 Mapper와 Reducer를 먼저 검증했다.
5. Hadoop Streaming을 이용해 전체 데이터를 처리했다.
6. 감성별 집계 결과를 HDFS에서 확인했다.
7. 브랜치와 Docker bind mount 문제를 우회해 작업을 완료했다.

작은 로컬 데이터가 아닌 약 227MB 규모의 데이터를 Hadoop에서 처리하면서 HDFS 입력, Mapper, Shuffle & Sort, Reducer, HDFS 출력으로 이어지는 전체 MapReduce 흐름을 확인할 수 있었다.