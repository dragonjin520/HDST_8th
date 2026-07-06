import sys
import time
from multiprocessing import Pool
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config import WORK, WORKER_COUNT  # noqa: E402


def work_log(task):
    """
    Simulate a task by printing a waiting message,
    sleeping for the specified duration, and printing a finished message.
    """
    name, duration = task

    print(f"Process {name} waiting {duration} seconds")
    time.sleep(duration)
    print(f"Process {name} Finished.")


if __name__ == "__main__":
    with Pool(processes=WORKER_COUNT) as pool:
        pool.map(work_log, WORK)