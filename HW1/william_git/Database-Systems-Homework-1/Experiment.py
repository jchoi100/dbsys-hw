from Utils.WorkloadGenerator import WorkloadGenerator
from Storage.File            import StorageFile
from Storage.Page            import Page
from Storage.SlottedPage     import SlottedPage

# Path to the folder containing csvs (on ugrad cluster)
dataDir = '/home/cs416/datasets/tpch-sf0.01/'

StorageFile.defaultPageClass = Page
scaleFactorList = [0.2, 0.4, 0.6, 0.8, 1.0]
pageSizeList = [4096]
workloadModeList = [1, 2, 3]

wg = WorkloadGenerator()

for i in scaleFactorList:
	for j in pageSizeList:
		for k in workloadModeList:
                        print(str(i) + " " + str(j) + " " + str(k) +" ")
                        wg.runWorkload(dataDir, i, j, k)



# Pick a page class, page size, scale factor, and workload mode:
#StorageFile.defaultPageClass = Page   # Contiguous Page
#pageSize = 4096                       # 4Kb
#scaleFactor = 0.5                     # Half of the data
#workloadMode = 1                      # Sequential Reads

# Run! Throughput will be printed afterwards.
# Note that the reported throughput ignores the time
# spent loading the dataset.
#wg = WorkloadGenerator()
#wg.runWorkload(dataDir, scaleFactor, pageSize, workloadMode)
