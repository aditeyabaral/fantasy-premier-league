import os
import sys
import json
import csv
#import utils
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, FloatType, StringType


#sc = spark.sparkContext
sc = SparkContext(master="local[2]", appName="fpl")
sql_context = SQLContext(sc)
ssc = StreamingContext(sc, batchDuration=1)

p1 = "../data/players.csv"
p2 = "../data/teams.csv"

player_df_path, teams_df_path = p1, p2

player_df = sql_context.read.load(p1, format="com.databricks.spark.csv", header=True, inferSchema=True)
teams_df = sql_context.read.load(p2, format="com.databricks.spark.csv", header=True, inferSchema=True)

player_df = player_df.withColumn('Pass Accuracy', lit(0).cast(FloatType()))
player_df = player_df.withColumn('Accurate Passes', lit(0).cast(IntegerType()))
player_df = player_df.withColumn('Non Accurate Passes', lit(0).cast(IntegerType()))
player_df = player_df.withColumn('Key Passes', lit(0).cast(IntegerType()))

# player_df = player_df.withColumn('Duel Effectiveness', lit(0).cast(IntegerType()))
# player_df = player_df.withColumn('Free Kick Effectiveness', lit(0).cast(IntegerType()))
# player_df = player_df.withColumn('Shots on target', lit(0).cast(IntegerType()))
# player_df = player_df.withColumn('Fouls', lit(0).cast(IntegerType()))
# player_df = player_df.withColumn('Own Goals', lit(0).cast(IntegerType()))
# player_df = player_df.withColumn('Goals', lit(0).cast(IntegerType()))
# player_df = player_df.withColumn('Minutes played', lit(0).cast(IntegerType()))


print(player_df.printSchema())






prev_id = None
list_of_players_0 = dict()
players_0 = dict()


def func(x):
    # print(type(x))
    stream_data = json.loads(x)
    my_keys = stream_data.keys()
    global prev_id
    global players_0
    global list_of_players_0
    global player_df
    if 'status' in my_keys:  # If the record is a match record
        cur_match = stream_data
        prev_id = cur_match['wyId']
         

    elif 'eventId' in my_keys:
        print("############################# LOOK FOR THIS LINE #############################")
        cur_event = stream_data
        playerId = cur_event['playerId']
        # print(cur_event)

        if cur_event['eventId'] == 8 :
            #Pass Accuracy code here
            acc_pass = 0
            non_acc_pass = 0
            key_pass = 0
            for tag in cur_event['tags']:
                if tag['id'] == 1801:
                    #The following line is throwing an error
                    player_df = player_df.withColumn('Accurate Passes', when(player_df['Id'] == playerId, player_df['Accurate Passes'] + 1).otherwise(player_df['Accurate Passes']))
                    print(tag['id'], playerId)
                
                if tag['id'] == 1802:
                    #The following line is throwing an error
                    player_df = player_df.withColumn('Non Accurate Passes', when(player_df['Id'] == playerId, player_df['Non Accurate Passes'] + 1).otherwise(player_df['Non Accurate Passes']))
                    print(tag['id'], playerId)
                  
                if tag['id'] == 302:
                    #The following line is throwing an error
                    player_df = player_df.withColumn('Key Passes', when(player_df['Id'] == playerId, player_df['Key Passes'] + 1).otherwise(player_df['Key Passes']))
                    print(tag['id'], playerId)
                  
            
            acc_pass = player_df.filter(player_df.Id == playerId).select('Accurate Passes').first()[0]
            non_acc_pass = player_df.filter(player_df.Id == playerId).select('Non Accurate Passes').first()[0]
            key_pass = player_df.filter(player_df.Id == playerId).select('Key Passes').first()[0]
            pass_acc = (acc_pass + (key_pass * 2))/(acc_pass + non_acc_pass + (key_pass * 2))
            player_df = player_df.withColumn('Pass Accuracy', when(player_df['Id'] == playerId, pass_acc).otherwise(player_df['Pass Accuracy']))
            print(playerId, pass_acc)
    return stream_data


lines = ssc.socketTextStream("localhost", 6100)
#data = lines.map(lambda x: func(x))
#data = lines.map(lambda x: getPassAccuracy(x))
#data = lines.foreachRDD(lambda x: func(x))
#getPassAccuracy(player_df, data)
data.pprint()
ssc.start()
ssc.awaitTermination()
ssc.stop()
