#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose.yml"
MASTER_CONTAINER="hadoop-master"
TEST_INPUT_DIR="/w3m2-test/input"
TEST_OUTPUT_DIR="/w3m2-test/output"
TEST_LOCAL_FILE="/tmp/w3m2-test.txt"
TEST_HDFS_FILE="${TEST_INPUT_DIR}/sample.txt"

PASS_COUNT=0
FAIL_COUNT=0

info() {
  printf '[INFO] %s\n' "$1"
}

pass() {
  printf '[PASS] %s\n' "$1"
  PASS_COUNT=$((PASS_COUNT + 1))
}

fail() {
  printf '[FAIL] %s\n' "$1" >&2
  FAIL_COUNT=$((FAIL_COUNT + 1))
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    printf '[ERROR] 필요한 명령을 찾을 수 없습니다: %s\n' "$1" >&2
    exit 1
  }
}

require_command docker

[[ -f "${COMPOSE_FILE}" ]] || {
  printf '[ERROR] docker-compose.yml을 찾을 수 없습니다: %s\n' "${COMPOSE_FILE}" >&2
  exit 1
}

if ! docker ps --format '{{.Names}}' | grep -qx "${MASTER_CONTAINER}"; then
  printf '[ERROR] 실행 중인 NameNode 컨테이너를 찾을 수 없습니다: %s\n' "${MASTER_CONTAINER}" >&2
  printf '[INFO] 먼저 다음 명령을 실행하세요: docker compose up -d\n' >&2
  exit 1
fi

run_in_master() {
  docker exec "${MASTER_CONTAINER}" bash -lc "$1"
}

check_config() {
  local command_name="$1"
  local key="$2"
  local expected="$3"
  local actual

  if ! actual="$(run_in_master "${command_name} getconf -confKey '${key}'" 2>/dev/null | tail -n 1 | tr -d '\r')"; then
    fail "${key} 값을 조회하지 못했습니다."
    return
  fi

  if [[ "${actual}" == "${expected}" ]]; then
    pass "${key} -> ${actual}"
  else
    fail "${key} -> ${actual} (expected: ${expected})"
  fi
}

info "Hadoop 설정값 검증을 시작합니다."

check_config hdfs "mapreduce.framework.name" "yarn"
check_config hdfs "mapreduce.jobhistory.address" "namenode:10020"
check_config hdfs "mapreduce.task.io.sort.mb" "256"

check_config hdfs "yarn.resourcemanager.address" "namenode:8032"
check_config hdfs "yarn.nodemanager.resource.memory-mb" "8192"
check_config hdfs "yarn.scheduler.minimum-allocation-mb" "1024"
check_config hdfs "yarn.nodemanager.aux-services" "mapreduce_shuffle"

info "Hadoop 데몬 상태를 확인합니다."

MASTER_JPS="$(run_in_master 'jps' 2>/dev/null || true)"

if grep -q 'NameNode' <<<"${MASTER_JPS}"; then
  pass "NameNode 프로세스 실행 확인"
else
  fail "NameNode 프로세스를 찾지 못했습니다."
fi

if grep -q 'ResourceManager' <<<"${MASTER_JPS}"; then
  pass "ResourceManager 프로세스 실행 확인"
else
  fail "ResourceManager 프로세스를 찾지 못했습니다."
fi

for worker in hadoop-worker1 hadoop-worker2; do
  if docker ps --format '{{.Names}}' | grep -qx "${worker}"; then
    WORKER_JPS="$(docker exec "${worker}" bash -lc 'jps' 2>/dev/null || true)"

    if grep -q 'DataNode' <<<"${WORKER_JPS}"; then
      pass "${worker}: DataNode 실행 확인"
    else
      fail "${worker}: DataNode 프로세스를 찾지 못했습니다."
    fi

    if grep -q 'NodeManager' <<<"${WORKER_JPS}"; then
      pass "${worker}: NodeManager 실행 확인"
    else
      fail "${worker}: NodeManager 프로세스를 찾지 못했습니다."
    fi
  else
    fail "실행 중인 worker 컨테이너를 찾지 못했습니다: ${worker}"
  fi
done

info "HDFS 테스트 파일과 실제 복제 계수를 검증합니다."

if run_in_master "
  echo 'hello hadoop hello docker' > '${TEST_LOCAL_FILE}' &&
  hdfs dfs -mkdir -p '${TEST_INPUT_DIR}' &&
  hdfs dfs -rm -f '${TEST_HDFS_FILE}' >/dev/null 2>&1 || true
  hdfs dfs -put -f '${TEST_LOCAL_FILE}' '${TEST_HDFS_FILE}'
" >/dev/null 2>&1; then
  pass "HDFS 테스트 파일 생성 완료: ${TEST_HDFS_FILE}"
else
  fail "HDFS 테스트 파일 생성에 실패했습니다."
fi

FSCK_OUTPUT="$(run_in_master "hdfs fsck '${TEST_HDFS_FILE}' -files -blocks -locations" 2>/dev/null || true)"

if grep -Eq 'Average block replication:[[:space:]]*2([.]0+)?' <<<"${FSCK_OUTPUT}"; then
  pass "HDFS 실제 복제 계수 -> 2"
else
  fail "HDFS 실제 복제 계수가 2가 아닙니다."
  printf '%s\n' "${FSCK_OUTPUT}" | tail -n 20
fi

info "MapReduce WordCount 작업을 실행합니다."

run_in_master "hdfs dfs -rm -r -f '${TEST_OUTPUT_DIR}' >/dev/null 2>&1 || true"

MAPREDUCE_RESULT=0
if run_in_master "
  EXAMPLE_JAR=\$(find \"\${HADOOP_HOME}/share/hadoop/mapreduce\" -maxdepth 1 -name 'hadoop-mapreduce-examples-*.jar' | head -n 1)
  test -n \"\${EXAMPLE_JAR}\"
  hadoop jar \"\${EXAMPLE_JAR}\" wordcount '${TEST_INPUT_DIR}' '${TEST_OUTPUT_DIR}'
"; then
  MAPREDUCE_RESULT=1
  pass "MapReduce WordCount 작업 실행 완료"
else
  fail "MapReduce WordCount 작업 실행에 실패했습니다."
fi

if [[ "${MAPREDUCE_RESULT}" -eq 1 ]] \
  && run_in_master "hdfs dfs -test -e '${TEST_OUTPUT_DIR}/_SUCCESS'"; then
  pass "MapReduce 결과 파일 생성 확인"
else
  fail "MapReduce 성공 마커를 확인하지 못했습니다."
fi

APPLICATION_OUTPUT="$(run_in_master "yarn application -list -appStates ALL" 2>/dev/null || true)"

if grep -qi 'application_' <<<"${APPLICATION_OUTPUT}"; then
  pass "YARN 애플리케이션 기록 확인"
else
  fail "YARN 애플리케이션 기록을 확인하지 못했습니다."
fi

NODE_OUTPUT="$(run_in_master 'yarn node -list' 2>/dev/null || true)"
RUNNING_NODES="$(
  printf '%s\n' "${NODE_OUTPUT}" \
    | sed -nE 's/.*Total Nodes:[[:space:]]*([0-9]+).*/\1/p' \
    | tail -n 1
)"

if [[ "${RUNNING_NODES}" =~ ^[0-9]+$ ]] && (( RUNNING_NODES >= 2 )); then
  pass "YARN NodeManager 등록 수 -> ${RUNNING_NODES}"
else
  fail "YARN에 등록된 NodeManager가 2개 미만입니다."
  printf '%s\n' "${NODE_OUTPUT}"
fi

printf '\n========== 검증 결과 =========='"\n"
printf 'PASS: %d\n' "${PASS_COUNT}"
printf 'FAIL: %d\n' "${FAIL_COUNT}"

if (( FAIL_COUNT > 0 )); then
  exit 1
fi

printf '[PASS] 모든 Hadoop 설정 및 동작 검증을 통과했습니다.\n'