# Spark, Python, Java가 포함된 버전을 명시적으로 고정한다.
FROM apache/spark:3.5.6-python3

USER root

# Spark 애플리케이션과 결과 파일을 저장할 경로
RUN mkdir -p \
    /opt/spark-apps \       
    /opt/spark-data \
    /opt/spark-output \
    /opt/spark-logs \
    && chown -R spark:spark \
    /opt/spark-apps \
    /opt/spark-data \
    /opt/spark-output \
    /opt/spark-logs

# 로컬 소스 코드를 이미지 안으로 복사한다.
COPY --chown=spark:spark src/ /opt/spark-apps/

# Spark 관련 공통 환경변수
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /opt/spark-apps

# 컨테이너를 비특권 사용자로 실행한다.
USER spark