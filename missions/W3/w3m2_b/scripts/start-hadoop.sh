#!/bin/bash
set -e

mkdir -p /opt/hadoop/logs
mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/tmp

echo "Starting Hadoop node: ${NODE_ROLE}"

if [ "${NODE_ROLE}" = "master" ]; then
    if [ ! -d "/hadoop/dfs/name/current" ]; then
        echo "Formatting NameNode..."
        hdfs namenode -format -force -nonInteractive
    fi

    hdfs --daemon start namenode
    yarn --daemon start resourcemanager

elif [ "${NODE_ROLE}" = "worker" ]; then
    echo "Waiting for NameNode at namenode:9000..."
    until bash -c '</dev/tcp/namenode/9000' 2>/dev/null; do
        sleep 2
    done

    hdfs --daemon start datanode
    yarn --daemon start nodemanager

else
    echo "Unknown NODE_ROLE: ${NODE_ROLE}"
    exit 1
fi

echo "Hadoop services started."

exec tail -f /dev/null