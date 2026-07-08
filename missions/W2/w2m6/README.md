# 각 역할
AWS = 전체 클라우드 플랫폼
EC2 = Docker 컨테이너를 실행하는 서버
ECR = Docker 이미지를 보관하는 저장소
Docker = 실행 환경을 이미지로 만들고 컨테이너로 실행하는 도구

## 1. AWS
이번 과제에서는 AWS 안의 여러 서비스 중에서 EC2와 ECR을 사용.
내 컴퓨터가 아닌 외부 클라우드 환경에서 Data Product를 실행할 수 있게 해주는 기반

## 2. Docker
실행 환경을 포장하고 실행하는 도구
내 Mac에서 실행되던 환경을 EC2에서도 똑같이 실행되게 만든다
배포할 프로그램과 환경을 포장하는 도구

## 3. EC2
EC2는 AWS에서 빌려 쓰는 가상 서버
외부 사용자가 Public IP 또는 Public DNS로 접속할 수 있는 실행 서버

## 4. ECR
ECR은 AWS의 Docker 이미지 저장소
Mac에서 만든 Docker 이미지는 처음에는 Mac 안에만 있다.
EC2는 그 이미지를 바로 가져올 수 없기 때문에 중간 저장소가 필요
Mac에서 만든 Docker 이미지를 EC2가 가져갈 수 있도록 보관하는 창고




# Docker Image 생성하기
Local에서 Docker Desktop을 이용해서 AWS EC2에 배포할 docker container image를 생성하세요.

어떤 OS를 선택해야 할까요?

어떤 소프트웨어를 설치해야 할까요?

어떤 화일을 담아야 할까요?


# 팀 활동 요구사항
Docker를 사용하는 이유가 뭘까요?

어떤 점은 더 불편한가요?

이번 미션에서는 하나의 EC2에 하나의 Docker container를 배포했습니다. 만약에 여러대의 EC2에 여러 개의 컨테이너를 배포해야 한다면 어떻게 해야 할까요?

