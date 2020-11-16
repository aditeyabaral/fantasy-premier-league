import sys
import socket
import requests
import json
import time
import csv
import cryptography
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
#from quiet_logs import quiet_logs


sc = SparkContext("local", "fpl")
sql_context = SQLContext(sc)
ssc  = StreamingContext(sc, 1)

p1 = "BD_FPL/Data/play.csv"
p2 = "BD_FPL/Data/teams.csv"

player_df_path, teams_df_path = p1, p2

player_df = sqlContext.read.load(p1, format="com.databricks.spark.csv", header = True, inferSchema = True)
teams_df = sqlContext.read.load(p2, format="com.databricks.spark.csv", header = True, inferSchema = True)

lines = ssc.socketTextStream("localhost", 6100)


#quiet_logs(sc)

