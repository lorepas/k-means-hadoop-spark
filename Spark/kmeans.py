import sys
from pyspark import SparkContext
import random
import time

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

def writeOutputPoints(filename, centroids, dim):
    f = open(filename, 'w')
    for c in centroids:
        format = ""
        for i in range(dim):
            format += str(c[1][i]) + ";"
        format += str(c[0]) + "\n"
        f.write(format)

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

if __name__ == "__main__":
    if len(sys.argv) < 1:
        """ info on usage """
        sys.exit(-1)

    master = "local"
    sc = SparkContext(master, "K-means")

    pointsFile = "dataset.txt"
    centroidsFile = "initialCentroids.txt"
    outputFile = "output.txt"
    dim = int(sys.argv[1])


    def kmeansmap(point, d):
        closest_centroid_index = 0
        min_norm = 100000
        for centroid in centroids_broad.value:
            norm = 0
            for i in range(d):
                norm += pow((point[i] - centroid[1][i]), 2)
            if norm <= min_norm:
                min_norm = norm
                closest_centroid_index = centroid[0]
        return closest_centroid_index, (point, 1)


    def kmeansreduce(x, y, d):
        updated_centroid = []
        tot_points = x[1] + y[1]
        for i in range(d):
            updated_centroid.append(x[0][i] + y[0][i])
        updated_centroid = tuple(updated_centroid)
        return updated_centroid, tot_points

    def createVector(line, d):
        vector=[]
        vector = str(line).replace("\n", "").split(";")
        for i in range(d):
            vector[i] = float(vector[i])

        return vector

    def euclideanDistance(a,b,d):
    	distance = 0
    	for i in range(d):
    		distance = distance + pow((a[i]-b[i]),2)
    	return pow(distance,1/2)

 
    start = time.time()
    """ generate an array of tuples from the file """
    initialCentroids = readStartingPoints(centroidsFile, dim)
    lines = sc.textFile(pointsFile)
    points = lines.map(lambda point: createVector(point,dim))
    points.cache()

    centroids = initialCentroids
    iteration_count = 0
    diff = []
    while 1:
        print("#"+str(iteration_count) + " ITERATION\n\n")
        count = 0	#counter for stop condition
        centroids_broad = sc.broadcast(centroids)
        old_centroids = centroids_broad.value #get centroids before updating
        map_result = points.map(lambda x: kmeansmap(x, dim))
        new_centroids = map_result.reduceByKey(lambda x, y: kmeansreduce(x, y, dim))
        new_centroids = new_centroids.collect()
        for new_centroid in new_centroids:
            for i in range(dim):
                centroids[new_centroid[0]][1][i] = new_centroid[1][0][i] / new_centroid[1][1]
        for i in range(len(centroids)):
        	diff = euclideanDistance(centroids[i][1],old_centroids[i][1],dim)
        	if(diff<0.1):
        		count += 1
        if(count == len(centroids) || iteration_count > 14):
        	break;
        iteration_count += 1

    """ save newCentroids in a file if the algorithm is finished"""
    writeOutputPoints(outputFile, centroids, dim)

    end = time.time()
    print("Done in " + str(end-start) + " seconds, with "+ str(iteration_count) + " iterantions.")



