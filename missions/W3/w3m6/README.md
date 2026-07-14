# W3M6 - Amazon Product Reviews MapReduce

## 1. 과제 개요

Amazon Reviews 2023 데이터셋을 Hadoop HDFS에 저장하고, Python으로 작성한 Mapper와 Reducer를 Hadoop Streaming으로 실행하여 상품별 리뷰 수와 평균 평점을 계산한다.

이번 과제의 핵심은 다음과 같다.

- HDFS에서 입력 데이터를 읽는다.
- Mapper가 상품 ID와 평점을 key-value 형태로 출력한다.
- Hadoop Shuffle/Sort가 같은 상품 ID를 기준으로 데이터를 그룹화한다.
- Reducer가 상품별 리뷰 수와 평균 평점을 계산한다.
- 최종 결과를 HDFS에 저장하고 로컬 파일로 내려받는다.

---

## 2. 학습 목표

- Amazon Reviews 2023 데이터셋의 구조를 이해한다.
- JSON Lines 형식의 데이터를 Mapper에서 파싱한다.
- Hadoop Streaming을 이용해 Python MapReduce 작업을 실행한다.
- 상품별 리뷰 수와 평균 평점을 집계한다.
- 로컬 테스트와 Hadoop 실행 결과를 비교하여 정확성을 검증한다.
- Data, Code, Config, Output을 분리하여 재실행 가능한 구조로 구성한다.

---

## 3. 데이터셋

### 3.1 데이터 출처

- 데이터셋: Amazon Reviews 2023
- 사용 카테고리: `All_Beauty`
- 형식: JSON Lines (`.jsonl`)

전체 Amazon Reviews 2023 데이터는 규모가 매우 크기 때문에, 과제 수행과 Hadoop 실습에 적합한 `All_Beauty` 카테고리를 사용한다.

### 3.2 사용 필드

각 리뷰 레코드에서 다음 필드만 사용한다.

| 필드 | 설명 |
|---|---|
| `parent_asin` | 상품을 식별하는 상품 ID |
| `rating` | 사용자가 남긴 평점 |

입력 데이터 예시:

```json
{"rating": 5.0, "title": "Good product", "parent_asin": "B00YQ6X8EO"}
{"rating": 4.0, "title": "Useful", "parent_asin": "B00YQ6X8EO"}
{"rating": 3.0, "title": "Normal", "parent_asin": "B001TEST01"}
```

---

## 4. 처리 구조

```text
Amazon review JSONL
        │
        ▼
Mapper
parent_asin    rating
        │
        ▼
Hadoop Shuffle / Sort
상품 ID별 데이터 그룹화
        │
        ▼
Reducer
상품 ID    리뷰 수    평균 평점
```

Mapper 출력 예시:

```text
B00YQ6X8EO    5.0
B00YQ6X8EO    4.0
B001TEST01    3.0
```

Reducer 출력 예시:

```text
B00YQ6X8EO    2    4.50
B001TEST01    1    3.00
```

---

## 5. 디렉터리 구조

```text
w3m6/
├── README.md
├── config/
│   └── job.env
├── data/
│   ├── raw/
│   │   └── All_Beauty.jsonl
│   └── sample/
│       └── All_Beauty_sample.jsonl
├── output/
│   ├── local_result.txt
│   └── hadoop_result.txt
├── scripts/
│   ├── download_data.sh
│   └── run_hadoop.sh
└── src/
    ├── mapper.py
    └── reducer.py
```

각 디렉터리의 역할은 다음과 같다.

| 디렉터리 | 역할 |
|---|---|
| `config/` | 데이터 경로와 HDFS 경로 등 실행 설정 관리 |
| `data/raw/` | 원본 데이터 저장 |
| `data/sample/` | 로컬 검증용 샘플 데이터 저장 |
| `src/` | Mapper와 Reducer 코드 저장 |
| `scripts/` | 다운로드와 Hadoop 실행 명령 자동화 |
| `output/` | 로컬 실행 결과와 Hadoop 결과 저장 |

---

## 6. 디렉터리 생성

`w3m6` 디렉터리에서 다음 명령을 실행한다.

```bash
cd ~/Documents/GitHub/HDST_8th/missions/W3/w3m6

mkdir -p config data/raw data/sample output scripts src

touch config/job.env
touch scripts/download_data.sh
touch scripts/run_hadoop.sh
touch src/mapper.py
touch src/reducer.py
```

구조 확인:

```bash
tree
```

---

## 7. Config 설정

`config/job.env` 파일에 다음 내용을 작성한다.

```bash
DATASET_CATEGORY=All_Beauty
LOCAL_DATA_PATH=data/raw/All_Beauty.jsonl
SAMPLE_DATA_PATH=data/sample/All_Beauty_sample.jsonl

HDFS_INPUT_DIR=/w3m6/input
HDFS_OUTPUT_DIR=/w3m6/output
```

경로와 실행 설정을 코드에서 분리하면 다른 데이터셋이나 HDFS 경로를 사용할 때 코드 수정 없이 설정만 변경할 수 있다.

---

## 8. 데이터 다운로드

Amazon Reviews 2023의 `All_Beauty` 리뷰 데이터를 다운로드한다.

```bash
curl -L \
  https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/review_categories/All_Beauty.jsonl.gz \
  -o data/raw/All_Beauty.jsonl.gz
```

다운로드 파일 확인:

```bash
ls -lh data/raw
```

압축 해제:

```bash
gunzip data/raw/All_Beauty.jsonl.gz
```

압축 해제 결과 확인:

```bash
ls -lh data/raw
```

데이터 형식 확인:

```bash
head -n 2 data/raw/All_Beauty.jsonl
```

각 줄이 하나의 JSON 객체로 출력되어야 한다.

---

## 9. 샘플 데이터 생성

전체 데이터를 Hadoop에서 실행하기 전에 1,000개 리뷰를 사용해 로컬 파이프라인을 먼저 검증한다.

```bash
head -n 1000 \
  data/raw/All_Beauty.jsonl \
  > data/sample/All_Beauty_sample.jsonl
```

샘플 데이터 개수 확인:

```bash
wc -l data/sample/All_Beauty_sample.jsonl
```

예상 결과:

```text
1000 data/sample/All_Beauty_sample.jsonl
```

---

## 10. Mapper 구현

`src/mapper.py`는 JSON Lines 데이터를 한 줄씩 읽고 `parent_asin`과 `rating`을 출력한다.

```python
#!/usr/bin/env python3

import json
import sys


def main():
    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        try:
            review = json.loads(line)

            product_id = review.get("parent_asin")
            rating = review.get("rating")

            if not product_id or rating is None:
                continue

            rating = float(rating)

            if not 0.0 <= rating <= 5.0:
                continue

            print(f"{product_id}\t{rating}")

        except (json.JSONDecodeError, TypeError, ValueError):
            continue


if __name__ == "__main__":
    main()
```

Mapper는 다음 데이터를 건너뛴다.

- 빈 줄
- JSON 형식이 잘못된 레코드
- `parent_asin`이 없는 레코드
- `rating`이 없는 레코드
- 숫자로 변환할 수 없는 평점
- 0점 미만 또는 5점 초과 평점

---

## 11. Reducer 구현

`src/reducer.py`는 상품 ID별로 평점을 합산하고 리뷰 수와 평균 평점을 계산한다.

```python
#!/usr/bin/env python3

import sys


def emit_result(product_id, rating_sum, review_count):
    if product_id is None or review_count == 0:
        return

    average_rating = rating_sum / review_count
    print(f"{product_id}\t{review_count}\t{average_rating:.2f}")


def main():
    current_product_id = None
    rating_sum = 0.0
    review_count = 0

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        try:
            product_id, rating_text = line.split("\t", 1)
            rating = float(rating_text)
        except (ValueError, TypeError):
            continue

        if product_id == current_product_id:
            rating_sum += rating
            review_count += 1
            continue

        emit_result(current_product_id, rating_sum, review_count)

        current_product_id = product_id
        rating_sum = rating
        review_count = 1

    emit_result(current_product_id, rating_sum, review_count)


if __name__ == "__main__":
    main()
```

평균 평점은 다음 식으로 계산한다.

```text
평균 평점 = 상품별 평점 합계 / 상품별 리뷰 수
```

---

## 12. 실행 권한 설정

Mapper와 Reducer를 Hadoop Streaming에서 직접 실행할 수 있도록 실행 권한을 부여한다.

```bash
chmod +x src/mapper.py
chmod +x src/reducer.py
```

---

## 13. 로컬 테스트

Hadoop에서 실행하기 전에 Unix 파이프로 Mapper와 Reducer를 연결하여 결과를 검증한다.

```bash
python3 src/mapper.py \
  < data/sample/All_Beauty_sample.jsonl \
  | sort \
  | python3 src/reducer.py \
  > output/local_result.txt
```

결과 확인:

```bash
head output/local_result.txt
```

결과 형식:

```text
상품_ID    리뷰_수    평균_평점
```

열 개수 확인:

```bash
awk -F '\t' 'NF != 3 {print NR, $0}' output/local_result.txt
```

아무 결과도 출력되지 않으면 모든 행이 세 개의 열로 구성된 것이다.

---

## 14. 리뷰 수 기준 정렬 확인

Reducer 결과는 기본적으로 상품 ID를 기준으로 정렬된다. 가장 많은 리뷰를 가진 상품을 확인하려면 두 번째 열을 숫자 기준 내림차순으로 정렬한다.

```bash
sort -t $'\t' -k2,2nr output/local_result.txt | head
```

평균 평점이 높은 상품을 확인하려면 세 번째 열을 기준으로 정렬한다.

```bash
sort -t $'\t' -k3,3nr output/local_result.txt | head
```

리뷰 수가 적은 상품의 높은 평균 평점은 대표성이 낮을 수 있으므로, 평균 평점을 해석할 때 리뷰 수도 함께 확인해야 한다.

---

## 15. Hadoop 클러스터 상태 확인

Docker 컨테이너가 실행 중인지 확인한다.

```bash
docker ps
```

HDFS 상태 확인:

```bash
docker exec hadoop-master hdfs dfsadmin -report
```

YARN 노드 확인:

```bash
docker exec hadoop-master yarn node -list
```

---

## 16. 입력 데이터를 Hadoop 컨테이너로 복사

로컬의 원본 데이터를 `hadoop-master` 컨테이너 내부로 복사한다.

```bash
docker cp \
  data/raw/All_Beauty.jsonl \
  hadoop-master:/tmp/All_Beauty.jsonl
```

Mapper와 Reducer도 컨테이너로 복사한다.

```bash
docker cp src/mapper.py hadoop-master:/tmp/mapper.py
docker cp src/reducer.py hadoop-master:/tmp/reducer.py
```

컨테이너 내부 파일 확인:

```bash
docker exec hadoop-master ls -lh /tmp/All_Beauty.jsonl /tmp/mapper.py /tmp/reducer.py
```

---

## 17. HDFS 입력 디렉터리 생성

기존 입력 디렉터리가 있을 경우 삭제한 뒤 새로 생성한다.

```bash
docker exec hadoop-master \
  hdfs dfs -rm -r -f /w3m6/input

docker exec hadoop-master \
  hdfs dfs -mkdir -p /w3m6/input
```

원본 데이터를 HDFS에 업로드한다.

```bash
docker exec hadoop-master \
  hdfs dfs -put /tmp/All_Beauty.jsonl /w3m6/input/
```

업로드 확인:

```bash
docker exec hadoop-master \
  hdfs dfs -ls -h /w3m6/input
```

---

## 18. Hadoop Streaming JAR 확인

Hadoop Streaming JAR의 실제 경로를 확인한다.

```bash
docker exec hadoop-master bash -c \
  'find /opt/hadoop -name "hadoop-streaming*.jar"'
```

일반적인 경로 예시:

```text
/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
```

설치된 Hadoop 버전에 따라 파일명은 달라질 수 있다.

---

## 19. 기존 출력 디렉터리 삭제

Hadoop은 이미 존재하는 출력 디렉터리를 덮어쓰지 않는다. 작업을 다시 실행하기 전에 기존 출력 디렉터리를 삭제한다.

```bash
docker exec hadoop-master \
  hdfs dfs -rm -r -f /w3m6/output
```

---

## 20. Hadoop Streaming 작업 실행

다음 명령으로 MapReduce 작업을 제출한다.

```bash
docker exec hadoop-master hadoop jar \
  /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -D mapreduce.job.name="w3m6-amazon-review-analysis" \
  -files /tmp/mapper.py,/tmp/reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /w3m6/input \
  -output /w3m6/output
```

Hadoop Streaming JAR의 버전이나 경로가 다르면 앞 단계에서 확인한 경로로 변경한다.

작업 실행 중에는 다음 항목을 확인한다.

- Map 작업 진행률
- Reduce 작업 진행률
- 실패한 Task 존재 여부
- 입력 레코드 수
- 출력 레코드 수

---

## 21. 작업 상태 확인

YARN 애플리케이션 목록 확인:

```bash
docker exec hadoop-master yarn application -list -appStates ALL
```

완료 상태가 `FINISHED`, 최종 상태가 `SUCCEEDED`인지 확인한다.

HDFS 출력 파일 확인:

```bash
docker exec hadoop-master \
  hdfs dfs -ls /w3m6/output
```

정상 완료 시 다음과 같은 파일이 생성된다.

```text
/w3m6/output/_SUCCESS
/w3m6/output/part-00000
```

Reducer 수에 따라 `part-00001`과 같은 파일이 추가로 생성될 수 있다.

---

## 22. HDFS 결과 확인

결과 일부 확인:

```bash
docker exec hadoop-master \
  hdfs dfs -cat '/w3m6/output/part-*' \
  | head
```

결과 형식:

```text
상품_ID    리뷰_수    평균_평점
```

전체 결과 행 수 확인:

```bash
docker exec hadoop-master \
  hdfs dfs -cat '/w3m6/output/part-*' \
  | wc -l
```

---

## 23. 가장 많은 리뷰를 받은 상품 확인

HDFS 결과를 리뷰 수 기준으로 내림차순 정렬한다.

```bash
docker exec hadoop-master \
  hdfs dfs -cat '/w3m6/output/part-*' \
  | sort -t $'\t' -k2,2nr \
  | head -n 10
```

출력 결과의 의미는 다음과 같다.

| 열 | 의미 |
|---|---|
| 첫 번째 열 | 상품 ID |
| 두 번째 열 | 해당 상품의 전체 리뷰 수 |
| 세 번째 열 | 해당 상품의 평균 평점 |

---

## 24. Hadoop 결과를 로컬로 저장

HDFS 출력 결과를 로컬 `output` 디렉터리에 저장한다.

```bash
docker exec hadoop-master \
  hdfs dfs -cat '/w3m6/output/part-*' \
  > output/hadoop_result.txt
```

결과 확인:

```bash
head output/hadoop_result.txt
```

가장 많은 리뷰를 받은 상품 확인:

```bash
sort -t $'\t' -k2,2nr output/hadoop_result.txt | head -n 10
```

---

## 25. 로컬 결과와 Hadoop 결과 검증

샘플 데이터를 HDFS에 업로드하여 동일한 데이터로 Hadoop 작업을 실행하면 로컬 결과와 직접 비교할 수 있다.

정렬 후 비교:

```bash
sort output/local_result.txt > /tmp/local_sorted.txt
sort output/hadoop_result.txt > /tmp/hadoop_sorted.txt

diff /tmp/local_sorted.txt /tmp/hadoop_sorted.txt
```

아무 내용도 출력되지 않으면 두 결과가 동일한 것이다.

전체 원본 데이터를 Hadoop에서 실행한 경우에는 로컬 샘플 결과와 행 수가 다르므로 직접 비교하지 않는다.

---

## 26. 자동 실행 스크립트 예시

`scripts/run_hadoop.sh`에 다음과 같이 실행 과정을 정리할 수 있다.

```bash
#!/usr/bin/env bash

set -euo pipefail

HDFS_INPUT_DIR="/w3m6/input"
HDFS_OUTPUT_DIR="/w3m6/output"
STREAMING_JAR="/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"

hdfs dfs -rm -r -f "${HDFS_INPUT_DIR}"
hdfs dfs -rm -r -f "${HDFS_OUTPUT_DIR}"
hdfs dfs -mkdir -p "${HDFS_INPUT_DIR}"
hdfs dfs -put /tmp/All_Beauty.jsonl "${HDFS_INPUT_DIR}/"

hadoop jar "${STREAMING_JAR}" \
  -D mapreduce.job.name="w3m6-amazon-review-analysis" \
  -files /tmp/mapper.py,/tmp/reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input "${HDFS_INPUT_DIR}" \
  -output "${HDFS_OUTPUT_DIR}"

hdfs dfs -cat "${HDFS_OUTPUT_DIR}/part-*" | head
```

실행 권한 설정:

```bash
chmod +x scripts/run_hadoop.sh
```

스크립트를 컨테이너에서 사용할 경우 경로와 파일 위치가 실제 환경과 일치하는지 확인해야 한다.

---

## 27. 오류 처리와 데이터 품질

Mapper에서 잘못된 데이터를 단순히 프로그램 오류로 종료하지 않고 건너뛰도록 구성하였다.

다만 이러한 방식은 잘못된 데이터가 존재해도 작업이 성공하는 Silent Failure로 이어질 수 있다. 실제 운영 환경에서는 다음과 같은 추가 개선이 필요하다.

- 잘못된 JSON 레코드 수를 Hadoop Counter로 기록한다.
- 상품 ID가 없는 레코드 수를 기록한다.
- 평점 누락 또는 범위 오류 개수를 기록한다.
- 오류 레코드를 별도 HDFS 경로에 저장한다.
- 입력 레코드 수와 정상 출력 레코드 수를 비교한다.

이번 구현에서는 파이프라인 중단을 방지하는 기본 예외 처리를 적용하고, 결과 검증 단계에서 입력과 출력 상태를 확인한다.

---

## 28. 결과 해석

최종 결과의 각 행은 하나의 상품에 대한 집계 결과이다.

```text
B001E4KFG0    150    4.30
```

해석:

- 상품 ID: `B001E4KFG0`
- 리뷰 수: `150`
- 평균 평점: `4.30`

리뷰 수가 많다는 것은 해당 상품이 데이터셋에서 많이 평가되었다는 뜻이다. 평균 평점은 리뷰 수와 함께 해석해야 하며, 리뷰 수가 매우 적은 상품은 평균 평점이 높더라도 대표성이 낮을 수 있다.

---

## 29. 예상 결과 형식

```text
B001E4KFG0    150    4.30
B00813GRG4    200    3.90
...
```

실제 상품 ID와 집계값은 사용한 데이터셋과 실행 시점의 입력 데이터에 따라 달라진다.

---

## 30. 트러블슈팅

### 출력 디렉터리가 이미 존재하는 경우

```text
FileAlreadyExistsException: Output directory ... already exists
```

해결:

```bash
docker exec hadoop-master \
  hdfs dfs -rm -r -f /w3m6/output
```

### Streaming JAR 경로가 다른 경우

```bash
docker exec hadoop-master bash -c \
  'find /opt/hadoop -name "hadoop-streaming*.jar"'
```

확인한 실제 경로를 실행 명령에 사용한다.

### Mapper 또는 Reducer가 실행되지 않는 경우

컨테이너 내부 파일 존재 여부를 확인한다.

```bash
docker exec hadoop-master ls -l /tmp/mapper.py /tmp/reducer.py
```

Python 설치 여부를 확인한다.

```bash
docker exec hadoop-master python3 --version
```

### 출력이 비어 있는 경우

입력 데이터에 필요한 필드가 존재하는지 확인한다.

```bash
head -n 1 data/raw/All_Beauty.jsonl
```

Mapper 단독 실행 결과를 확인한다.

```bash
python3 src/mapper.py \
  < data/sample/All_Beauty_sample.jsonl \
  | head
```

### Native Hadoop 경고가 출력되는 경우

```text
WARN util.NativeCodeLoader: Unable to load native-hadoop library...
```

macOS 또는 현재 컨테이너 환경에 맞는 Native Hadoop 라이브러리를 찾지 못해 Java 구현을 사용한다는 경고이다. Hadoop 작업이 정상 완료된다면 과제 수행에는 문제가 없다.

---

## 31. 제출 파일

```text
w3m6/
├── README.md
├── config/
│   └── job.env
├── output/
│   └── hadoop_result.txt
├── scripts/
│   ├── download_data.sh
│   └── run_hadoop.sh
└── src/
    ├── mapper.py
    └── reducer.py
```

제출 시 다음 항목을 포함한다.

- Mapper 소스 코드
- Reducer 소스 코드
- 실행 스크립트
- Config 파일
- Hadoop 실행 결과 파일
- 전체 실행 과정과 결과 해석이 포함된 README
- 과제에서 사용하는 Docker 관련 파일

대용량 원본 데이터는 Git 저장소에 직접 포함하지 않고 `.gitignore`로 제외하는 것이 적절하다.

예시:

```gitignore
data/raw/
data/sample/
```

Docker 이미지는 제출하지 않는다.

---

## 32. 과제를 통해 확인한 내용

- Mapper는 각 리뷰를 독립적으로 읽어 상품 ID와 평점을 출력한다.
- Hadoop은 Mapper의 출력을 상품 ID 기준으로 Shuffle/Sort한다.
- Reducer는 같은 상품에 대한 평점을 순차적으로 집계한다.
- 리뷰 수와 평균 평점은 대규모 데이터에서도 상품 단위로 병렬 계산할 수 있다.
- HDFS는 입력과 결과 데이터를 분산 저장하고, YARN은 MapReduce 작업에 필요한 실행 자원을 관리한다.
- 로컬 테스트를 먼저 수행하면 Hadoop 환경 문제와 Python 로직 문제를 분리하여 확인할 수 있다.
- Hadoop 출력 디렉터리는 덮어쓸 수 없으므로 재실행 전에 기존 경로를 삭제해야 한다.

---

## 33. 개선 가능 사항

현재 구현을 확장하면 다음 기능을 추가할 수 있다.

- Hadoop Counter를 이용한 오류 레코드 집계
- Combiner를 이용한 네트워크 전송량 감소
- 리뷰 수 기준 상위 상품만 출력하는 두 번째 MapReduce Job
- 카테고리별 결과 비교
- 리뷰 수가 일정 기준 이상인 상품만 평균 평점 순으로 정렬
- 결과를 CSV, Parquet 또는 데이터베이스에 적재
- Airflow를 이용한 다운로드, HDFS 업로드, MapReduce 실행 자동화
- Spark DataFrame을 이용한 동일 집계 결과 비교
```
