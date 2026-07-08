# Extra Assignment: Word Cloud 작동 방법

## 1. Word Cloud란?

Word Cloud는 텍스트 데이터에서 자주 등장하는 단어를 크게 표시하는 시각화 방법이다.  
단어의 빈도가 높을수록 큰 글씨로 나타나고, 빈도가 낮을수록 작은 글씨로 나타난다.

---

## 2. Word Cloud 생성 과정

이번 과제의 Word Cloud 생성 과정은 다음과 같다.

```text
텍스트 데이터 로드
→ 감성 라벨 기준으로 positive / negative 분리
→ 각 감성별 데이터 샘플링
→ 텍스트를 하나의 문자열로 결합
→ 단어 빈도 계산
→ 빈도에 따라 단어 크기 결정
→ 이미지 공간에 단어 배치
→ matplotlib으로 시각화
```

---

## 3. Sampling을 사용하는 이유

전체 데이터셋은 1,600,000개의 트윗으로 구성되어 있어 전체 데이터를 사용하면 처리 시간이 길어진다.  
따라서 positive와 negative 데이터에서 각각 일부 데이터를 샘플링하여 Word Cloud를 생성했다.

```python
positive_sample = positive_df.sample(n=1000, random_state=42)
negative_sample = negative_df.sample(n=1000, random_state=42)
```

`random_state`는 실행할 때마다 같은 샘플이 선택되도록 하여 결과를 재현 가능하게 만든다.

---

## 4. 단어 크기 결정 방식

Word Cloud는 텍스트에서 단어의 등장 빈도를 계산한다.

```text
자주 등장한 단어 → 크게 표시
적게 등장한 단어 → 작게 표시
```

예를 들어 `good`이 100번, `happy`가 20번 등장했다면 `good`이 더 크게 표시된다.

이번 과제에서는 다음 옵션을 사용했다.

```python
WordCloud(max_words=200)
```

즉, 하나의 Word Cloud에는 빈도가 높은 단어를 기준으로 최대 200개까지만 표시된다.

---

## 5. 단어 배치 방식

Word Cloud는 빈도가 높은 단어를 먼저 배치하고, 이후 작은 단어들을 남은 공간에 배치한다.  
단어들은 서로 겹치지 않도록 이미지 안에 배치된다.

---

## 6. matplotlib 시각화

WordCloud 객체는 이미지 형태로 생성된다.  
이를 Jupyter Notebook 화면에 출력하기 위해 `matplotlib`을 사용했다.

```python
fig, axes = plt.subplots(1, 2, figsize=(16, 7))
```

`plt.subplots(1, 2)`를 사용하여 Positive Word Cloud와 Negative Word Cloud를 하나의 plot에 함께 표시했다.

---

## 7. 해석 시 주의점

Word Cloud는 어떤 단어가 자주 등장했는지 빠르게 파악하는 데 유용하다.  
하지만 문맥을 완전히 이해하지는 못한다.

예를 들어 `good`이라는 단어가 많이 등장해도 실제 문장이 `not good`이라면 의미는 부정적일 수 있다.  
따라서 Word Cloud는 감성 분석 결과를 보조적으로 이해하는 시각화 도구로 활용하는 것이 적절하다.
