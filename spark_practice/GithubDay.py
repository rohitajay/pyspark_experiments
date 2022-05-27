from __future__ import print_function
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
import sys

if __name__ == "__main__":
    sc = SparkContext(conf=SparkConf())
    sqlContext = SQLContext(sc)
    ghLog = sqlContext.read.json(sys.argv[1])
