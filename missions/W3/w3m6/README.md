# W3M6 - Amazon Product Reviews MapReduce

## 1. 과제 개요

Amazon Reviews 2023 데이터셋을 HDFS에 저장하고, Python으로 작성한 Mapper와 Reducer를 Hadoop Streaming으로 실행하여 상품별 리뷰 수와 평균 평점을 계산했다.

처리 흐름은 다음과 같다.

```text
Amazon Reviews JSONL
        │
        ▼
Mapper
상품 ID(parent_asin)    평점(rating)
        │
        ▼
Hadoop Shuffle / Sort
상품 ID별 그룹화
        │
        ▼
Reducer
상품 ID    리뷰 수    평균 평점
        │
        ▼
HDFS /w3m6/output
```

---

## 2. 학습 목표

- Amazon Reviews 2023 데이터셋의 구조를 이해한다.
- JSON Lines 형식의 데이터를 Mapper에서 파싱한다.
- Hadoop Streaming으로 Python MapReduce 작업을 실행한다.
- 상품별 리뷰 수와 평균 평점을 집계한다.
- 로컬 테스트와 Hadoop 실행을 통해 처리 결과를 검증한다.
- 대용량 데이터와 실행 코드를 분리하여 Git 저장소를 관리한다.

---

## 3. 데이터셋

### 3.1 데이터 출처

- 데이터셋: Amazon Reviews 2023
- 사용 카테고리: `All_Beauty`
- 데이터 형식: JSON Lines (`.jsonl`)
- HDFS 입력 파일 크기: 약 `311.5MB`

전체 Amazon Reviews 2023 데이터는 매우 크기 때문에, Hadoop 실습에 충분한 크기를 가지면서도 로컬 환경에서 처리 가능한 `All_Beauty` 카테고리를 선택했다.

### 3.2 사용 필드

| 필드 | 설명 |
|---|---|
| `parent_asin` | 상품을 식별하는 상품 ID |
| `asin` | `parent_asin`이 없을 때 사용할 대체 상품 ID |
| `rating` | 사용자가 남긴 평점 |

입력 데이터 예시:

```json
{"rating": 5.0, "title": "Such a lovely scent", "asin": "B000HB6VLE", "parent_asin": "B000HB6VLE"}
```

---

## 4. 디렉터리 구조

```text
w3m6/
├── .gitignore
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

대용량 원본과 샘플 데이터는 `.gitignore`로 제외한다.

```gitignore
data/raw/
data/sample/
*.gz
*.zip
*.jsonl
*.csv
```

---

## 5. 데이터 다운로드 및 준비

데이터 다운로드:

```bash
curl -L \
  https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/review_categories/All_Beauty.jsonl.gz \
  -o data/raw/All_Beauty.jsonl.gz
```

압축 해제:

```bash
gunzip data/raw/All_Beauty.jsonl.gz
```

데이터 형식 확인:

```bash
head -n 2 data/raw/All_Beauty.jsonl
```

로컬 검증용 샘플 생성:

```bash
head -n 1000 \
  data/raw/All_Beauty.jsonl \
  > data/sample/All_Beauty_sample.jsonl
```

---

## 6. Mapper

`src/mapper.py`는 JSONL 데이터를 한 줄씩 읽고 상품 ID와 평점을 탭으로 구분하여 출력한다.

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

            product_id = review.get("parent_asin") or review.get("asin")
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

Mapper 출력 형식:

```text
상품_ID    평점
```

예시:

```text
B000HB6VLE    5.0
B0093MXHFG    4.0
```

---

## 7. Reducer

`src/reducer.py`는 Hadoop이 상품 ID 기준으로 정렬한 Mapper 출력을 읽고 상품별 리뷰 수와 평균 평점을 계산한다.

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

Reducer 출력 형식:

```text
상품_ID    리뷰_수    평균_평점
```

---

## 8. 로컬 테스트

Hadoop에서 전체 데이터를 실행하기 전에 샘플 데이터로 Mapper와 Reducer를 검증했다.

```bash
mkdir -p output

python3 src/mapper.py \
  < data/sample/All_Beauty_sample.jsonl \
  | sort \
  | python3 src/reducer.py \
  > output/local_result.txt
```

결과 확인:

```bash
head -n 10 output/local_result.txt
```

열 개수 검증:

```bash
awk -F '\t' 'NF != 3 {print "잘못된 행:", NR, $0}' \
  output/local_result.txt
```

아무것도 출력되지 않으면 모든 행이 세 개 열로 구성된 것이다.

---

## 9. Hadoop 실행 환경

Hadoop Docker 환경은 기존 `w3m2_b` 브랜치에 구성되어 있었다. `w3m6`의 과제 코드와 `w3m2_b`의 실행 환경을 서로 수정하지 않기 위해 Git 외부 임시 경로를 사용했다.

```text
w3m6 브랜치
  └─ Mapper / Reducer 작성
          │
          ▼
/tmp/w3m6에 임시 저장
          │
          ▼
w3m2_b 브랜치
  └─ 기존 Hadoop Docker 환경에서 실행
          │
          ▼
/tmp/w3m6에 결과 저장
          │
          ▼
w3m6 브랜치의 output/으로 결과 복사
```

Mapper와 Reducer 임시 저장:

```bash
mkdir -p /tmp/w3m6

cp src/mapper.py /tmp/w3m6/mapper.py
cp src/reducer.py /tmp/w3m6/reducer.py
```

`w3m2_b` 브랜치의 Hadoop 환경에서 컨테이너로 복사:

```bash
docker cp /tmp/w3m6/mapper.py hadoop-master:/tmp/mapper.py
docker cp /tmp/w3m6/reducer.py hadoop-master:/tmp/reducer.py
```

복사 확인:

```bash
docker exec hadoop-master grep -n "json.loads" /tmp/mapper.py

docker exec hadoop-master \
  ls -lh /tmp/mapper.py /tmp/reducer.py
```

---

## 10. HDFS 입력 데이터 업로드

로컬 데이터를 컨테이너로 복사:

```bash
docker cp \
  data/raw/All_Beauty.jsonl \
  hadoop-master:/tmp/All_Beauty.jsonl
```

HDFS 입력 디렉터리 생성:

```bash
docker exec hadoop-master \
  hdfs dfs -rm -r -f /w3m6/input

docker exec hadoop-master \
  hdfs dfs -mkdir -p /w3m6/input
```

HDFS 업로드:

```bash
docker exec hadoop-master \
  hdfs dfs -put /tmp/All_Beauty.jsonl /w3m6/input/
```

업로드 결과:

```text
Found 1 items
-rw-r--r--   2 root supergroup    311.5 M /w3m6/input/All_Beauty.jsonl
```

---

## 11. Mapper 단독 검증

전체 Hadoop 작업을 다시 실행하기 전에 HDFS 입력 중 10줄만 Mapper에 전달했다.

```bash
docker exec hadoop-master bash -c \
  'hdfs dfs -cat /w3m6/input/All_Beauty.jsonl \
  | head -n 10 \
  | python3 /tmp/mapper.py'
```

정상 출력 예시:

```text
B000HB6VLE    5.0
B0093MXHFG    4.0
```

`head -n 10`을 Mapper 앞에 배치하여 전체 311.5MB 파일을 테스트 과정에서 처리하지 않도록 했다.

---

## 12. Hadoop Streaming 실행

기존 출력 경로 삭제:

```bash
docker exec hadoop-master \
  hdfs dfs -rm -r -f /w3m6/output
```

Hadoop Streaming 작업 실행:

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

Hadoop은 이미 존재하는 출력 디렉터리를 덮어쓰지 않으므로 재실행 전에 `/w3m6/output`을 삭제해야 한다.

---

## 13. Hadoop 작업 결과

출력 디렉터리 확인:

```bash
docker exec hadoop-master \
  hdfs dfs -ls -h /w3m6/output
```

실제 결과:

```text
Found 2 items
-rw-r--r--   2 root supergroup          0 2026-07-14 09:57 /w3m6/output/_SUCCESS
-rw-r--r--   2 root supergroup      1.9 M 2026-07-14 09:57 /w3m6/output/part-00000
```

확인 결과:

- `_SUCCESS` 파일이 생성되었다.
- `part-00000` 파일이 약 `1.9MB`로 생성되었다.
- Hadoop Streaming 작업이 정상 완료되었다.

결과 일부:

```bash
docker exec hadoop-master \
  hdfs dfs -cat '/w3m6/output/part-*' \
  | head -n 10
```

```text
0005946468      1       5.00
0123034892      1       5.00
0124784577      3       4.33
0515059560      1       4.00
0692508988      1       5.00
069267599X      41      4.78
0764490117      2       5.00
0816091846      14      4.64
0963416391      1       5.00
0966068432      2       5.00
```

---

## 14. 집계된 상품 수

```bash
docker exec hadoop-master \
  hdfs dfs -cat '/w3m6/output/part-*' \
  | wc -l
```

실행 결과:

```text
112565
```

총 `112,565개` 상품에 대해 리뷰 수와 평균 평점이 집계되었다.

---

## 15. 리뷰 수 상위 10개 상품

```bash
docker exec hadoop-master bash -c \
  "hdfs dfs -cat '/w3m6/output/part-*' \
  | sort -t \$'\t' -k2,2nr \
  | head -n 10"
```

| 순위 | 상품 ID | 리뷰 수 | 평균 평점 |
|---:|---|---:|---:|
| 1 | `B085BB7B1M` | 1,962 | 4.62 |
| 2 | `B0BM4GX6TT` | 1,750 | 4.14 |
| 3 | `B07C533XCW` | 1,513 | 4.47 |
| 4 | `B09X9BG4FC` | 1,374 | 4.50 |
| 5 | `B00R1TAN7I` | 1,372 | 4.03 |
| 6 | `B08L5KN7X4` | 1,343 | 4.02 |
| 7 | `B019GBG0IE` | 1,328 | 3.53 |
| 8 | `B01M1OFZOG` | 1,243 | 3.96 |
| 9 | `B0C9CWKY9G` | 1,153 | 4.85 |
| 10 | `B0107QYW14` | 1,112 | 3.93 |

가장 많은 리뷰를 받은 상품은 `B085BB7B1M`이다.

- 리뷰 수: `1,962개`
- 평균 평점: `4.62점`

리뷰 수가 많다고 항상 평균 평점이 높은 것은 아니었다. `B019GBG0IE`는 1,328개의 리뷰를 보유했지만 평균 평점은 3.53점이었다. 반대로 `B0C9CWKY9G`는 1,153개의 리뷰와 4.85점의 높은 평균 평점을 함께 기록했다.

---

## 16. 결과 파일 저장

HDFS 결과를 브랜치와 무관한 임시 경로에 저장했다.

```bash
docker exec hadoop-master \
  hdfs dfs -cat '/w3m6/output/part-*' \
  > /tmp/w3m6/hadoop_result.txt
```

저장 확인:

```bash
ls -lh /tmp/w3m6/hadoop_result.txt
```

```text
-rw-r--r--  1 admin  wheel   1.9M Jul 14 18:57 /tmp/w3m6/hadoop_result.txt
```

`w3m6` 브랜치로 돌아온 후 최종 결과 디렉터리에 복사했다.

```bash
mkdir -p output
cp /tmp/w3m6/hadoop_result.txt output/hadoop_result.txt
```

최종 저장 결과:

```text
-rw-r--r--  1 admin  staff   1.9M Jul 14 18:59 output/hadoop_result.txt
```

최종 결과 파일:

```text
output/hadoop_result.txt
```

---

## 17. 최종 결과 요약

| 항목 | 결과 |
|---|---:|
| 사용 데이터 | Amazon Reviews 2023 `All_Beauty` |
| 입력 파일 형식 | JSON Lines |
| HDFS 입력 파일 크기 | 약 311.5MB |
| 출력 파일 크기 | 약 1.9MB |
| 집계된 상품 수 | 112,565개 |
| 최다 리뷰 상품 | `B085BB7B1M` |
| 최다 리뷰 수 | 1,962개 |
| 최다 리뷰 상품 평균 평점 | 4.62점 |
| Hadoop 작업 상태 | 성공 |

---

## 18. 문제와 해결 과정

### 18.1 `_SUCCESS`는 생성됐지만 결과가 0바이트인 문제

최초 실행에서는 Hadoop 작업이 성공했지만 `part-00000`이 0바이트였다.

```text
/w3m6/output/_SUCCESS       0 bytes
/w3m6/output/part-00000     0 bytes
```

원인은 기존 Mapper가 CSV 형식의 입력을 가정하고 쉼표로 데이터를 분리했기 때문이다. JSON 문자열의 제목과 본문에도 쉼표가 포함되어 있어 한 줄이 여러 열로 잘못 분리되었다.

```text
[WARN] invalid row: ['{"rating": 4.0', ' "title": ...']
```

Amazon Reviews 2023 데이터는 JSON Lines이므로 Mapper를 `json.loads()` 기반으로 수정했다.

```python
review = json.loads(line)
product_id = review.get("parent_asin") or review.get("asin")
rating = review.get("rating")
```

수정 후 `part-00000`이 약 1.9MB로 정상 생성되었다.

### 18.2 브랜치 전환으로 Docker bind mount가 깨진 문제

`w3m6` 브랜치에는 과제 코드가 있고, Hadoop Docker 설정은 `w3m2_b` 브랜치에 있었다. Docker 컨테이너가 `w3m2_b`의 설정 파일을 bind mount한 상태에서 브랜치를 바꾸면 해당 파일이 작업 트리에서 사라져 오류가 발생했다.

```text
error while creating mount source path .../w3m2_b/config/modified/hdfs-site.xml
```

Docker 설정을 수정하지 않고 다음 방식으로 해결했다.

```text
1. w3m6 코드를 /tmp/w3m6에 임시 저장
2. w3m2_b 브랜치로 전환
3. 기존 Hadoop 환경 실행
4. 임시 Mapper와 Reducer를 컨테이너로 복사
5. Hadoop 작업 실행
6. 결과를 /tmp/w3m6에 저장
7. w3m6 브랜치에서 결과를 회수
```

### 18.3 Native Hadoop 경고

다음 경고가 반복적으로 출력되었다.

```text
WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform...
```

현재 플랫폼에 맞는 Native Hadoop 라이브러리를 찾지 못해 Java 구현을 사용한다는 경고이다. MapReduce 작업은 정상적으로 완료되었으며 결과에도 영향을 주지 않았다.

---

## 19. 데이터 품질과 개선 사항

현재 Mapper는 잘못된 JSON, 누락된 상품 ID, 잘못된 평점을 건너뛴다. 파이프라인 중단은 방지하지만 오류 데이터가 조용히 제외되는 Silent Failure 가능성이 있다.

실제 운영 환경에서는 다음과 같은 개선이 필요하다.

- Hadoop Counter를 이용해 JSON 파싱 오류 개수 기록
- 상품 ID 누락 레코드 수 기록
- 잘못된 평점 레코드 수 기록
- 오류 레코드를 별도 HDFS 경로에 저장
- 입력 레코드 수와 정상 처리 레코드 수 비교
- Combiner를 도입해 Mapper와 Reducer 사이의 전송량 감소
- 리뷰 수 상위 상품을 구하는 두 번째 MapReduce Job 구성

---

## 20. 제출 파일

```text
w3m6/
├── .gitignore
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

제출 대상:

- Mapper 소스 코드
- Reducer 소스 코드
- Hadoop 실행 스크립트
- Config 파일
- Hadoop 실행 결과 파일
- 전체 실행 과정과 결과가 포함된 README
- 과제에서 사용하는 Docker 관련 파일

대용량 원본 데이터와 샘플 데이터는 Git에 업로드하지 않는다. Docker 이미지도 제출하지 않는다.

---

## 21. 과제를 통해 확인한 내용

- Mapper는 각 JSON 리뷰에서 상품 ID와 평점을 추출한다.
- Hadoop은 Mapper 출력을 상품 ID 기준으로 Shuffle/Sort한다.
- Reducer는 같은 상품의 평점을 합산해 리뷰 수와 평균 평점을 계산한다.
- Hadoop 작업이 성공했더라도 결과 파일이 0바이트인지 반드시 확인해야 한다.
- 입력 파일 형식과 Mapper의 파싱 방식이 일치해야 한다.
- HDFS는 입력과 결과 데이터를 저장하고 YARN은 MapReduce 작업 자원을 관리한다.
- 로컬 테스트와 Mapper 단독 테스트를 통해 Python 코드 문제와 Hadoop 환경 문제를 분리할 수 있다.
- 서로 다른 브랜치의 코드와 실행 환경은 Git 외부 `/tmp`를 이용해 안전하게 전달할 수 있다.
- 약 311.5MB의 입력 데이터를 처리해 112,565개 상품의 집계 결과를 생성했다.
```
