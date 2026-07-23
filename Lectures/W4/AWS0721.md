# AWS

## IAM
- Account vs User
    - 계정 로그인과 유저 로그인
    - Ac ID : codesquad edu -> 제일 큰 AWS용 네임 스페이스
    - userID : IAM 전용

## AuthN & AuthZ
- 인증 : user, Group, Role
- 인가 : Policy


## infra
- Region : 서비스 제공 도시
    - Availability Zone : Region 안의 가용 영역, 최소 2개 
    - 가용성 확보 이중화용 : 서버 하나가 죽어도 다른 하나가 살아있음
- Edge Location : Region 밖에 있지만 서비스하는 작은 지역
    - 빠르게 응답이 필요하면 가까운 Edge location 사용


## VPC
- region에 속한 가상 사설망
- 인터넷에서 접근 제한되어 어려움
    - 인터넷에서 접근하는 것이 목표


- 하나의 VPC는 여러 AZ에 걸쳐있음
- 여러개의 데이터 센터를 하나의 망으로 연결했다. 



### Subnet
- VPC와 AZ에 동시에 속함.
    - subnet 위치가 AZ도 선택
- NACL 방화벽 : All open
    - EC2 별로 공통적 방화벽 적용
#### private vs public
- private : 외부 접근 제한 (기본값) - 데이터베이스, 백엔드
    - NAT 이용 : private를 public와 연결해서 외부와 통신
- public : 외부 접근 허용

### VPC 생성
#### IP 대역 설정
- CIDR 설정
- VPC : 10.0.0.0/16 (10.0.0.0 - 10.0.255.255)
- 각 subnet에 할당 : 10.0.0.0/24 (public), 10.0.10/24 (private)


### Main Route Table
- 연결 경로 설정
- 10.0.0.0/16 : local -> VPC 내부 통신 가능
    - private : local route table 설정 
    - public : 공인 public IP 발급 (비쌈)
        - Internet Gateway 생성
        - route table 수정
            - 내부 local 이외의 ip는 모두 외부 접근이라 차단
            - 따라서 IGW 추가



### EC2 방화벽
- Security Group : All close

## 필수 작업
1. route table
2. public IP
3. IGW
4. Security Group

