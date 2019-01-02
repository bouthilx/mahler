from ops import create_trial, run, mahler, tags


def main():
    if not any(True for _ in mahler.find(tags + [run.name])):
        create_trial()


if __name__ == "__main__":
    main()
