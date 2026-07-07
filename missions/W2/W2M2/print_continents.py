from multiprocessing import Process


def print_continent(continent: str = "Asia") -> None:
    """Print a sentence including the name of a continent."""
    print(f"The name of continent is : {continent}")


def main() -> None:
    """Create and run multiple processes for printing continent names."""
    processes = [
        Process(target=print_continent, args=("America",)),
        Process(target=print_continent, args=("Europe",)),
        Process(target=print_continent),
        Process(target=print_continent, args=("Africa",)),
    ]

    for process in processes:
        process.start()

    for process in processes:
        process.join()


if __name__ == "__main__":
    main()