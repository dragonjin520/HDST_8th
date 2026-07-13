#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

CONFIG_DIR="${1:-${PROJECT_DIR}/config/modified}"
BACKUP_DIR="${2:-${PROJECT_DIR}/config/original}"

CONFIG_FILES=(
  "core-site.xml"
  "hdfs-site.xml"
  "mapred-site.xml"
  "yarn-site.xml"
)

log() {
  printf '[INFO] %s\n' "$1"
}

success() {
  printf '[PASS] %s\n' "$1"
}

fail() {
  printf '[FAIL] %s\n' "$1" >&2
  exit 1
}

command -v python3 >/dev/null 2>&1 \
  || fail "python3 명령을 찾을 수 없습니다."

[[ -d "${CONFIG_DIR}" ]] \
  || fail "Hadoop 설정 디렉터리가 존재하지 않습니다: ${CONFIG_DIR}"

mkdir -p "${BACKUP_DIR}"

log "설정 디렉터리: ${CONFIG_DIR}"
log "원본 백업 디렉터리: ${BACKUP_DIR}"

for file in "${CONFIG_FILES[@]}"; do
  source_path="${CONFIG_DIR}/${file}"
  backup_path="${BACKUP_DIR}/${file}"

  [[ -f "${source_path}" ]] \
    || fail "설정 파일을 찾을 수 없습니다: ${source_path}"

  if [[ -f "${backup_path}" ]]; then
    log "기존 원본 백업을 유지합니다: ${backup_path}"
  else
    cp "${source_path}" "${backup_path}"
    success "원본 설정 백업 완료: ${file}"
  fi
done

python3 - "${CONFIG_DIR}" <<'PY'
from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

config_dir = Path(sys.argv[1])

settings: dict[str, dict[str, str]] = {
    "core-site.xml": {
        "fs.defaultFS": "hdfs://namenode:9000",
        "hadoop.tmp.dir": "/hadoop/tmp",
        "io.file.buffer.size": "131072",
    },
    "hdfs-site.xml": {
        "dfs.replication": "2",
        "dfs.blocksize": "134217728",
        "dfs.namenode.name.dir": "/hadoop/dfs/name",
        "dfs.datanode.data.dir": "/hadoop/dfs/data",
    },
    "mapred-site.xml": {
        "mapreduce.framework.name": "yarn",
        "mapreduce.jobhistory.address": "namenode:10020",
        "mapreduce.task.io.sort.mb": "256",
    },
    "yarn-site.xml": {
        "yarn.resourcemanager.address": "namenode:8032",
        "yarn.nodemanager.resource.memory-mb": "8192",
        "yarn.scheduler.minimum-allocation-mb": "1024",
        "yarn.nodemanager.aux-services": "mapreduce_shuffle",
    },
}


def indent(element: ET.Element, level: int = 0) -> None:
    padding = "\n" + "  " * level
    child_padding = "\n" + "  " * (level + 1)

    if len(element):
        if not element.text or not element.text.strip():
            element.text = child_padding

        for child in element:
            indent(child, level + 1)

        if not element[-1].tail or not element[-1].tail.strip():
            element[-1].tail = padding

    if level and (not element.tail or not element.tail.strip()):
        element.tail = padding


def upsert_property(root: ET.Element, name: str, value: str) -> None:
    for property_element in root.findall("property"):
        name_element = property_element.find("name")
        if name_element is not None and name_element.text == name:
            value_element = property_element.find("value")
            if value_element is None:
                value_element = ET.SubElement(property_element, "value")
            value_element.text = value
            return

    property_element = ET.SubElement(root, "property")
    ET.SubElement(property_element, "name").text = name
    ET.SubElement(property_element, "value").text = value


for filename, file_settings in settings.items():
    path = config_dir / filename

    try:
        tree = ET.parse(path)
    except ET.ParseError as exc:
        raise SystemExit(f"[FAIL] XML 파싱 오류: {path} - {exc}") from exc

    root = tree.getroot()
    if root.tag != "configuration":
        raise SystemExit(
            f"[FAIL] 최상위 XML 태그가 configuration이 아닙니다: {path}"
        )

    for key, value in file_settings.items():
        upsert_property(root, key, value)

    indent(root)
    tree.write(path, encoding="UTF-8", xml_declaration=True)
    print(f"[PASS] 설정 변경 완료: {filename}")
PY

for file in "${CONFIG_FILES[@]}"; do
  python3 - "${CONFIG_DIR}/${file}" <<'PY'
import sys
import xml.etree.ElementTree as ET

path = sys.argv[1]
ET.parse(path)
print(f"[PASS] XML 문법 검증 완료: {path}")
PY
done

if command -v docker >/dev/null 2>&1 \
  && docker compose version >/dev/null 2>&1 \
  && [[ -f "${PROJECT_DIR}/docker-compose.yml" ]]; then

  if docker compose -f "${PROJECT_DIR}/docker-compose.yml" ps --services --status running \
    | grep -q .; then
    log "변경된 설정을 반영하기 위해 Hadoop 컨테이너를 재시작합니다."
    docker compose -f "${PROJECT_DIR}/docker-compose.yml" restart
    success "Hadoop 컨테이너 재시작 완료"
  else
    log "실행 중인 Hadoop 컨테이너가 없어 재시작을 생략합니다."
  fi
else
  log "Docker Compose를 사용할 수 없어 서비스 재시작을 생략합니다."
fi

success "Hadoop 설정 변경 작업이 완료되었습니다."