# Extra Assignment: Word Cloud 작동 방법

## 1. Word Cloud 생성 과정

이번 과제의 Word Cloud 생성 과정은 다음과 같다.

```text
텍스트 데이터 로드
→ 감성 라벨 기준으로 positive / negative 분리
→ 각 감성별 데이터 샘플링
→ 단어 빈도 계산
→ 빈도에 따라 단어 크기 결정
→ 이미지 공간에 단어 배치
→ matplotlib으로 시각화
```

---

## 2. Sampling을 사용하는 이유

전체 데이터셋은 1,600,000개의 트윗으로 구성되어 있어 전체 데이터를 사용하면 처리 시간이 길어진다.  
따라서 positive와 negative 데이터에서 각각 일부 데이터를 샘플링하여 Word Cloud를 생성했다.

```python
positive_sample = positive_df.sample(n=1000, random_state=42)
negative_sample = negative_df.sample(n=1000, random_state=42)
```

`random_state`는 실행할 때마다 같은 샘플이 선택되도록 하여 결과를 재현 가능하게 만든다.

---

## 3. 단어 크기 결정 방식

Word Cloud는 텍스트에서 단어의 등장 빈도를 계산한다.

```text
자주 등장한 단어 → 크게 표시
적게 등장한 단어 → 작게 표시
```

---

## 4. 단어 배치 방식

Word Cloud는 빈도가 높은 단어를 먼저 배치하고, 이후 작은 단어들을 남은 공간에 배치한다.  
단어들은 서로 겹치지 않도록 이미지 안에 배치된다.

---

## 5. matplotlib 시각화

WordCloud 객체는 이미지 형태로 생성된다.  
이를 Jupyter Notebook 화면에 출력하기 위해 `matplotlib`을 사용했다.

```python
fig, axes = plt.subplots(1, 2, figsize=(16, 7))
```

`plt.subplots(1, 2)`를 사용하여 Positive Word Cloud와 Negative Word Cloud를 하나의 plot에 함께 표시했다.

---
