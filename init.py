import os
import sys
import json
import csv
import utils
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext

#sc = spark.sparkContext
sc = SparkContext(master=f"local[{os.cpu_count()-1}]", appName="fpl")
sql_context = SQLContext(sc)
ssc = StreamingContext(sc, batchDuration=1)

p1 = "data/players.csv"
p2 = "data/teams.csv"

player_df_path, teams_df_path = p1, p2

player_df = sql_context.read.load(p1, format="com.databricks.spark.csv", header=True, inferSchema=True)
teams_df = sql_context.read.load(p2, format="com.databricks.spark.csv", header=True, inferSchema=True)


def func(x):
    #print(type(x))
    stream_data = json.loads(x)
    #print(type(stream_data))
    #print("################# HERE FUNC BEGINNING #################")
    getPassAccuracy(player_df, stream_data)
    #print("############ HERE AFTER UTILS ####################")
    return stream_data

lines = ssc.socketTextStream("localhost", 6100)
data = lines.map(lambda x: func(x))
#getPassAccuracy(player_df, data)
data.pprint()
ssc.start()
ssc.awaitTermination()
ssc.stop()