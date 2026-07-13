# Hadoop 이미지 생성
FROM ubuntu:22.04

# 패키지 설치 중 사용자 입력 방지
ENV DEBIAN_FRONTEND=noninteractive

# Hadoop 버전 설정
ARG HADOOP_VERSION=3.5.0

# Hadoop 실행에 필요한 Java와 기본 도구 설치
RUN apt-get update && \
    apt-get install -y \
        openjdk-17-jdk \
        openssh-server \
        wget \
        curl \
        vim \
        procps \
        ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 빌드 환경의 CPU 아키텍처에 맞춰 Java 경로 설정
ARG TARGETARCH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH}

# Hadoop 다운로드 및 설치
RUN wget -q \
        https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
        -O /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm /tmp/hadoop.tar.gz

# Hadoop 환경변수 설정
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/usr/lib/jvm/java-17-openjdk-${TARGETARCH}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Hadoop이 사용할 Java 경로 설정 및 명령어 경로 보강
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> \
        $HADOOP_CONF_DIR/hadoop-env.sh && \
    ln -s /opt/hadoop/bin/hadoop /usr/local/bin/hadoop && \
    ln -s /opt/hadoop/bin/hdfs /usr/local/bin/hdfs

# 로컬 Hadoop 설정 파일을 이미지 내부 설정 경로로 복사
COPY config/core-site.xml $HADOOP_CONF_DIR/core-site.xml
COPY config/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml
COPY config/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml

# Hadoop 시작 스크립트 복사, 줄바꿈 정리 및 실행 권한 부여
COPY scripts/start-hadoop.sh /usr/local/bin/start-hadoop.sh
RUN sed -i 's/\r$//' /usr/local/bin/start-hadoop.sh && \
    chmod +x /usr/local/bin/start-hadoop.sh

# HDFS 데이터 저장 디렉터리 생성
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data

# NameNode RPC와 Web UI 포트 공개
EXPOSE 9000 9870

# 작업 디렉터리 설정
WORKDIR /opt/hadoop

# 컨테이너 시작 시 Hadoop 자동 실행
CMD ["bash", "/usr/local/bin/start-hadoop.sh"]