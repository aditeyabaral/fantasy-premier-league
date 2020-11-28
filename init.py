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

def calculate_metrics(rdd):
    #Sample function prints match identifier and event ID for first 10 events
    rdds = [json.loads(i) for i in rdd.collect()]
    if rdds != []:
        match = rdds[0]
        print("MATCH ID:", match["wyId"])
        events = rdds[1:]
        for e in events[:10]:
            print("EVENT ID:", e["eventId"])

lines = ssc.socketTextStream("localhost", 6100)
lines.foreachRDD(calculate_metrics)
ssc.start()
ssc.awaitTermination()
ssc.stop()