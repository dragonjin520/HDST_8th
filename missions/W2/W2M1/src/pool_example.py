import csv
import os
import sys
import time
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config import (  # noqa: E402
    CHUNKSIZE,
    TASK_RESULT_PATH,
    TASK_TIMELINE_PATH,
    WORK,
    WORKER_COUNT,
)


def work_log(task):
    """
    Simulate a task by printing a waiting message,
    sleeping for the specified duration, and returning execution metadata.
    """
    name, duration = task

    start_timestamp = time.time()
    start_time = datetime.now().isoformat()
    pid = os.getpid()

    print(f"Process {name} waiting {duration} seconds")

    time.sleep(duration)

    end_timestamp = time.time()
    end_time = datetime.now().isoformat()

    print(f"Process {name} Finished.")

    return {
        "task_name": name,
        "duration": duration,
        "pid": pid,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "start_time": start_time,
        "end_time": end_time,
        "elapsed_time": round(end_timestamp - start_timestamp, 3),
        "status": "success",
        "error_message": "",
    }


def assign_worker_labels(results):
    """
    Assign readable worker labels based on process ids.
    Example: pid 12345 -> Worker-1, pid 12346 -> Worker-2
    """
    unique_pids = []

    for result in results:
        pid = result["pid"]
        if pid not in unique_pids:
            unique_pids.append(pid)

    pid_to_worker = {
        pid: f"Worker-{index + 1}"
        for index, pid in enumerate(unique_pids)
    }

    for result in results:
        result["worker"] = pid_to_worker[result["pid"]]

    return results


def add_relative_time_offsets(results, run_start_time):
    """
    Add start_offset and end_offset based on the total program start time.
    """
    for result in results:
        result["start_offset"] = round(
            result["start_timestamp"] - run_start_time,
            3,
        )
        result["end_offset"] = round(
            result["end_timestamp"] - run_start_time,
            3,
        )

    return results


def save_task_results(results):
    """
    Save task-level execution results to CSV.
    """
    TASK_RESULT_PATH.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "task_name",
        "worker",
        "pid",
        "duration",
        "start_offset",
        "end_offset",
        "elapsed_time",
        "start_time",
        "end_time",
        "status",
        "error_message",
    ]

    with open(TASK_RESULT_PATH, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for result in results:
            writer.writerow({
                "task_name": result["task_name"],
                "worker": result["worker"],
                "pid": result["pid"],
                "duration": result["duration"],
                "start_offset": result["start_offset"],
                "end_offset": result["end_offset"],
                "elapsed_time": result["elapsed_time"],
                "start_time": result["start_time"],
                "end_time": result["end_time"],
                "status": result["status"],
                "error_message": result["error_message"],
            })


def build_task_timeline(results):
    """
    Build a timeline showing which worker is running which task
    at each important time point.
    """
    time_points = sorted({
        result["start_offset"]
        for result in results
    } | {
        result["end_offset"]
        for result in results
    })

    workers = sorted({
        result["worker"]
        for result in results
    })

    timeline = []

    for time_point in time_points:
        row = {
            "time": time_point,
            "started_tasks": [],
            "finished_tasks": [],
        }

        for worker in workers:
            row[worker] = "idle"

        for result in results:
            task_name = result["task_name"]
            worker = result["worker"]
            start_offset = result["start_offset"]
            end_offset = result["end_offset"]

            if start_offset == time_point:
                row["started_tasks"].append(task_name)

            if end_offset == time_point:
                row["finished_tasks"].append(task_name)

            if start_offset <= time_point < end_offset:
                row[worker] = f"running {task_name}"

        row["started_tasks"] = ",".join(row["started_tasks"])
        row["finished_tasks"] = ",".join(row["finished_tasks"])

        timeline.append(row)

    return timeline, workers


def save_task_timeline(timeline, workers):
    """
    Save time-based worker status timeline to CSV.
    """
    fieldnames = [
        "time",
        *workers,
        "started_tasks",
        "finished_tasks",
    ]

    with open(TASK_TIMELINE_PATH, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(timeline)


if __name__ == "__main__":
    run_start_time = time.time()

    with Pool(processes=WORKER_COUNT) as pool:
        results = pool.map(work_log, WORK, chunksize=CHUNKSIZE)

    results = assign_worker_labels(results)
    results = add_relative_time_offsets(results, run_start_time)

    save_task_results(results)

    timeline, workers = build_task_timeline(results)
    save_task_timeline(timeline, workers)

    print(f"Task results saved to {TASK_RESULT_PATH}")
    print(f"Task timeline saved to {TASK_TIMELINE_PATH}")