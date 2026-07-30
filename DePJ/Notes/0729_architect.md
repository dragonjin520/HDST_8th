```text

[외부 데이터]
├─ bikeList
├─ citydata_ppltn
├─ 날씨 API
└─ citydata 행사
        │
        ▼
[Airflow]
스케줄·의존성·재시도 관리
        │
        ▼
[Lambda Collectors]
API 호출·검증·수집시각 추가
        │
        ▼
[S3 Raw]
원본 JSON/XML
        │
        ▼
[Spark DataFrame]
├─ 스키마 정제
├─ 시간 정렬
├─ 대여소-핫스팟 매핑
├─ 대여소-날씨 격자 매핑
├─ 행사 거리 계산
└─ 모델 Feature 생성
        │
        ▼
[S3 Silver / Feature]
        │
        ├─────────────────────┐
        ▼                     ▼
[EC2 모델 추론]         [Spark 규칙 기반 계산]
대여·반납량 예측          수요 위험 점수 계산
        │                     │
        └──────────┬──────────┘
                   ▼
        [운영 판단 후처리]
        예상 잔여량·공급량·회수량
                   │
                   ▼
       [S3 Gold + RDS PostgreSQL]
                   │
                   ▼
              [FastAPI]
                   │
                   ▼
        [React + Leaflet]

end
```