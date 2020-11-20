import os
import sys
import json
import csv
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext

def getPassAccuracy(df, stream_data):

    print("############################# HERE IN UTILS ######################################")
    print((stream_data))

