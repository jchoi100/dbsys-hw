import io, math, os, os.path, random, shutil, time, timeit
from Utils.WorkloadGenerator import WorkloadGenerator
from Database              import Database
from time import time

wg = WorkloadGenerator()
db = Database()

wg.createRelations(db)
wg.loadDataset(db, 'test/datasets/tpch-tiny', 1.0)
db.close()
shutil.rmtree(db.fileManager().dataDir, ignore_errors=True)
del db


with open("ExperimentResults", 'w') as f:
    f.write("Query 1:\n")
    oldTime = time()
    wg.runWorkload('test/datasets/tpch-tiny', 1.0, 4096, 1)
    runTime = time() - oldTime
    f.write("Database took time: " + str(runTime) +"\n\n")
    f.close()
