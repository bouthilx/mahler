from mahler.core import status

from ops import mahler, tags, run


if __name__ == "__main__":
    min_objective = float('inf')
    i = 0
    print(mahler.registrar._db._db.tasks.count())
    print(mahler.registrar._db._db.tasks.status.count())
    print(mahler.registrar._db._db.tasks.tags.count())
    for task in mahler.find(tags + [run.name], status=status.Completed('')):
        objective = task.output['objective']
        min_objective = min(objective, min_objective)
        print("{: 12.3f} {: 12.3f}".format(objective, min_objective))
        i += 1
    print(i)
