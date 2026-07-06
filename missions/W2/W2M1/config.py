from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

OUTPUT_DIR = BASE_DIR / "output"
TASK_RESULT_PATH = OUTPUT_DIR / "task_results.csv"
RUN_SUMMARY_PATH = OUTPUT_DIR / "run_summary.json"
WORKLOG_PATH = OUTPUT_DIR / "worklog.txt"

WORKER_COUNT = 2
CHUNKSIZE = 1

WORK = [
    ("A", 5),
    ("B", 2),
    ("C", 1),
    ("D", 3),
]