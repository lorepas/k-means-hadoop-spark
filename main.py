import os
import random
import time
import sys

def produceDataset(n,d, filename):
    f = open(filename, "w")
    for i in range(n):
        for j in range(d):
            f.write(str(random.uniform(min, max)))
            f.write(";")
        f.write(str(i) + "\n")
    f.close()

def getDataset(d, filename):
    with open(filename) as fp:
        print("-----------Chosen starting points:\n")
        dataset = []
        for i, line in enumerate(fp):
            point = []
            line = str(line).replace("\n", "").split(";")
            for j in range(d):
                point.append(float(line[j]))
            point.append(int(line[d]))
            dataset.append(point)
        return dataset

def writeStartingPoints(k, n, d, dataset_filename, centroids_filename):
    randomIndex = []
    for i in range(k):
        randomIndex.append(random.randrange(n))

    centroid_count = 0
    with open(dataset_filename) as fp:
        f = open(centroids_filename, "w")
        for i, line in enumerate(fp):
            if i in randomIndex:
                tmp = str(line).split(";")
                line = ""
                for j in range(d):
                    line += tmp[j] + ";"
                line += str(centroid_count) + "\n"
                f.write(line)
                centroid_count += 1
        f.close()

def readStartingPoints(filename, d):
    startingPoints = []
    with open(filename) as fp:
        for i, line in enumerate(fp):
            centroid = []
            point = []
            line = str(line).replace("\n", "").split(";")
            for j in range(d):
                point.append(float(line[j]))
            centroid.append(int(line[d]))
            centroid.append(point)
            startingPoints.append(centroid)
    return startingPoints

if __name__ == "__main__":

    if len(sys.argv) < 3:
        """ info on usage """
        sys.exit(-1)

    k = int(sys.argv[1])
    n = int(sys.argv[2])
    d = int(sys.argv[3])

    # PARAMETERS
#    k = 3
#    n = 100
#    d = 2
    min = 0
    max = 100
    dataset_filename = "dataset.txt"
    centroids_filename = "initialCentroids.txt"
    output_filename = "output.txt"

    # BUILD DATASET
    produceDataset(n, d, dataset_filename)

    
    # PICKUP STARTING POINTS
    writeStartingPoints(k, n, d, dataset_filename, centroids_filename)

    # SPARK-SUBMIT
    #print("Calling spark-submit")
    #os.system("spark-submit kmeans.py " + dataset_filename + " " + centroids_filename + " " + output_filename + " " + str(d))


    end = time.time()
    print("File created!")