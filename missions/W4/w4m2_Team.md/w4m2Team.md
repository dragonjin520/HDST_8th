# 팀 활동 요구사항
만약 이 데이터가 사람이 운행하는 차량의 데이터가 아니라 '자율주행차' 데이터라면 어떤 Data Product를 만들면 좋을까요? 아이디어를 수립하고 Prototype을 만들어 보세요.

## 주어진 데이터

- VendorID
- tpep_pickup_datetime
- tpep_dropoff_datetime
- passenger_count
- trip_distance
- RatecodeID
- store_and_fwd_flag
- PULocationID
- DOLocationID
- payment_type
- fare_amount
- extra
- mta_tax
- tip_amount
- tolls_amount
- improvement_surcharge
- total_amount
컬럼    |	의미    |	예시·해석
VendorID    |	운행 기록을 TLC에 제공한 택시 결제·운행정보 시스템 업체 코드    |	1=Creative Mobile Technologies, 2=Curb Mobility, 6=Myle Technologies, 7=Helix
tpep_pickup_datetime    |	택시 미터기를 켠 날짜와 시각    |	승객 탑승 및 운행 시작 시각
tpep_dropoff_datetime   |	택시 미터기를 끈 날짜와 시각    |	승객 하차 및 운행 종료 시각
passenger_count |	탑승한 승객 수  |	운전자가 입력한 값이므로 누락·오입력 가능
trip_distance   |	미터기가 기록한 운행 거리   |	단위는 마일(mile)
RatecodeID  |	운행 종료 시 적용된 요금제 코드 |	1 일반요금, 2 JFK, 3 Newark, 4 Nassau/Westchester, 5 협의요금, 6 합승, 99 미상
store_and_fwd_flag  |	네트워크 연결 문제로 기록을 차량에 임시 저장했다가 나중에 전송했는지 여부   |	Y=저장 후 전송, N=즉시 전송
PULocationID    |	승차 지점의 TLC Taxi Zone ID    |	PU는 Pickup
DOLocationID    |	하차 지점의 TLC Taxi Zone ID    |	DO는 Drop-off
payment_type    |	결제 방법 코드  |	0 Flex Fare, 1 카드, 2 현금, 3 무료, 4 결제 분쟁, 5 미상, 6 취소 처리
fare_amount |	미터기가 계산한 기본 운임   |	시간과 거리를 기준으로 산정된 운임
extra   |	추가 요금 및 기타 할증액    |	야간·혼잡 시간대 할증 등이 포함될 수 있음
mta_tax |	적용 요금에 따라 자동 부과되는 MTA 세금 |	일반적으로 별도 항목으로 기록
tip_amount  |	팁 금액 |	카드로 결제된 팁만 자동 기록되며 현금 팁은 포함되지 않음
tolls_amount    |	운행 중 발생한 전체 통행료	 |   다리·터널 등의 통행료 합계
improvement_surcharge   |	택시 서비스·차량 개선을 위한 개선 부담금    |	승차 시점에 부과되는 surcharge
total_amount    |	승객에게 청구된 최종 금액   |	현금 팁은 포함되지 않음

## 문제 정의
주어진 데이터는 택시 운전기사가 택시를 운전해서 생성해낸 데이터이다. 즉 사람이 생성한 데이터이고 사람마다, 시간마다 특성이 다를 것이다.

그런데 만약 이 데이터를 자율주행차가 만들었다면?
