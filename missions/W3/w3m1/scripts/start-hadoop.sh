#!/bin/bash
set -e

# HDFS 저장 디렉터리 생성
mkdir -p /hadoop/dfs/name
mkdir -p /hadoop/dfs/data

# 최초 실행일 때만 NameNode 포맷
if [ ! -d /hadoop/dfs/name/current ]; then
    echo "[INFO] NameNode를 최초 포맷합니다."
    hdfs namenode -format -force -nonInteractive
else
    echo "[INFO] 기존 NameNode 메타데이터를 사용합니다."
fi

# HDFS 데몬 시작
hdfs --daemon start namenode
hdfs --daemon start datanode

# 프로세스 상태 확인
echo "[INFO] 실행 중인 Hadoop 프로세스"
jps

# 컨테이너가 종료되지 않도록 NameNode 로그 출력 유지
exec tail -F "$HADOOP_HOME"/logs/hadoop-*-namenode-*.log