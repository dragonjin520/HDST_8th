from multiprocessing import Process, Queue


def push_items(queue, items):
    print("pushing items to queue:")

    for index, item in enumerate(items, start=1):
        queue.put(item)
        print(f"item no: {index} {item}")


def pop_items(queue, item_count):
    print()
    print("popping items from queue:")

    for index in range(item_count):
        item = queue.get()
        print(f"item no: {index} {item}")


def main():
    colors = ["red", "green", "blue", "black"]

    queue = Queue()

    push_process = Process(target=push_items, args=(queue, colors))
    pop_process = Process(target=pop_items, args=(queue, len(colors)))

    push_process.start()
    push_process.join()

    pop_process.start()
    pop_process.join()


if __name__ == "__main__":
    main()