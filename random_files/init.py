import os
import sys
import json
import csv
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, FloatType, StringType


#sc = spark.sparkContext
sc = SparkContext(master=f"local[{os.cpu_count()-1}]", appName="fpl")
sql_context = SQLContext(sc)
ssc = StreamingContext(sc, batchDuration=1)

p1 = "../data/players.csv"
p2 = "../data/teams.csv"

player_df_path, teams_df_path = p1, p2

player_df = sql_context.read.load(p1, format="com.databricks.spark.csv", header=True, inferSchema=True)
teams_df = sql_context.read.load(p2, format="com.databricks.spark.csv", header=True, inferSchema=True)

def getMetrics(rdd):
    r = [json.loads(x) for x in rdd]
    match = r[0]     
    events = r[1:]
    




#ssc.checkpoint("checkpoint")
lines = ssc.socketTextStream("localhost", 6100)
lines.foreachRDD(getMetrics)
ssc.start()
ssc.awaitTermination()
ssc.stop()
