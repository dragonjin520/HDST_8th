from multiprocessing import Queue


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

    push_items(queue, colors)
    pop_items(queue, len(colors))


if __name__ == "__main__":
    main()