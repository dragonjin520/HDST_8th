# Hadoop 이미지 생성

# Hadoop 이미지 생성
FROM ubuntu:22.04

# 패키지 설치 중 사용자 입력 방지
ENV DEBIAN_FRONTEND=noninteractive

# Hadoop 실행에 필요한 Java와 기본 도구 설치
RUN apt-get update && \
    apt-get install -y \
        openjdk-11-jdk \
        openssh-server \
        wget \
        curl \
        vim \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Java 환경변수 설정
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin

# 작업 디렉터리 설정
WORKDIR /opt