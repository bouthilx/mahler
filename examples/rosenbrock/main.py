import sys

from ops import create_trial, run, mahler, tags


def main(container=None):
    if not any(True for _ in mahler.find(tags + [run.name])):
        create_trial(container=container)


if __name__ == "__main__":
    main(container=sys.argv[1] if len(sys.argv) > 1 else None)
