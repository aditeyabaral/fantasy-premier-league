import sys
import json
import csv
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext

#sc = spark.sparkContext
sc = SparkContext(master="local[2]", appName="fpl")
sql_context = SQLContext(sc)
ssc = StreamingContext(sc, batchDuration=1)

p1 = "data/play.csv"
p2 = "data/teams.csv"

player_df_path, teams_df_path = p1, p2

#player_df = sqlContext.read.load(p1, format="com.databricks.spark.csv", header=True, inferSchema=True)
#teams_df = sqlContext.read.load(p2, format="com.databricks.spark.csv", header=True, inferSchema=True)

def func(x):
    data = json.loads(x)
    return data

lines = ssc.socketTextStream("localhost", 6100)
lines.pprint()
#data = lines.map(lambda x: func(x))

ssc.start()
ssc.awaitTermination()
#ssc.stop()