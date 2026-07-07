# multiprocessing_all_in_one.py

import time
from multiprocessing import Process, Queue, current_process
from queue import Empty


TASK_COUNT = 10
PROCESS_COUNT = 4
SLEEP_TIME = 0.5


def do_task(tasks_to_accomplish, tasks_that_are_done):
    """
    tasks_to_accomplish Queue에서 작업을 하나씩 꺼내 처리하고,
    완료 메시지를 tasks_that_are_done Queue에 저장한다.
    """
    while True:
        try:
            task = tasks_to_accomplish.get_nowait()
        except Empty:
            break

        print(task)
        time.sleep(SLEEP_TIME)

        process_name = current_process().name
        done_message = f"{task} is done by {process_name}"
        tasks_that_are_done.put(done_message)


def main():
    tasks_to_accomplish = Queue()
    tasks_that_are_done = Queue()

    for task_no in range(TASK_COUNT):
        tasks_to_accomplish.put(f"Task no {task_no}")

    processes = []

    for _ in range(PROCESS_COUNT):
        process = Process(
            target=do_task,
            args=(tasks_to_accomplish, tasks_that_are_done),
        )
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())


if __name__ == "__main__":
    main()