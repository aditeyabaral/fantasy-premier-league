#!/usr/bin/python3

#import findspark
#findspark.init()
import time
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests
import json

def helper(event):
	c = [(('uselessid', 'useless'), 1)];
	id_ = event['playerId'];

	if event['eventId'] == 8:
		a = 0; k = 0;
		for x in event['tags']:
			if(x['id'] == 1801): a = 1;
			if(x['id'] == 302): k = 1;
		if k:
			if a:
				c.append(((id_, 'accurate_key'), 1));
			c.append(((id_, 'key'), 1));
		else: 
			if a:
				c.append(((id_, 'accurate_normal'), 1));
			c.append(((id_, 'normal'), 1));
		return c;

	elif event['eventId'] == 1:
		status = 0;
		for x in event['tags']:
			if(x['id'] == 702): status = 1;break;
			if(x['id'] == 703): status = 2;break;
		if status == 1: c.append(((id_, 'duel_neutral'), 1));
		if status == 2: c.append(((id_, 'duel_won'), 1));
		c.append(((id_, 'duel'), 1));

	elif event['eventId'] == 3:
		g = 0; eff = 0;
		for x in event['tags']:
			if(x['id'] == 1801): eff = 1;
			if(x['id'] == 101): g = 1;
		if event['subEventId'] == 35 and g: c.append(((id_, 'free_kick_goal'), 1));
		elif eff: c.append(((id_, 'free_kick_eff'), 1));
		c.append(((id_, 'free_kick'), 1));

	elif event['eventId'] == 3:
		g = 0; k = 0;
		for x in event['tags']:
			if(x['id'] == 1801): k = 1;
			if(x['id'] == 101): g = 1;
		if k and g: c.append(((id_, 'shot_on_goal'), 1))
		elif k: c.append(((id_, 'shot_not_on_goal'), 1))
		c.append(((id_, 'shot'), 1))

	elif event['eventId'] == 2: c.append(((id_, 'foul'), 1));

	for x in event['tags']:
		if x['id'] == 102: c.append(((id_, 'own_goal'), 1));

	return c;

def player_details(match, x):
	id_ = x[0];
	prop = x[1];
	pass_accuracy = (prop['accurate_normal'] + 2 * prop['accurate_key'])/(prop['normal'] + 2 * prop['key']);
	duel_eff = (prop['duel_won'] + 0.5 * prop['duel_neutral'])/prop['duel'];
	fk_eff = (prop['free_kick_eff'] + prop['free_kick_goal'])/prop['free_kick'];
	shot_eff = (prop['shot_not_on_goal'] + 0.5 * prop['shot_on_goal'])/prop['shot'];
	shot_on_target = prop['shot_not_on_goal'] + prop['shot_on_goal'];
	rate = 1.05;
	for team in match['teamsData']:
		for subs in match['teamsData'][team]['substitutions']:
			for s in subs:
				if s['playerIn'] == id_: 
					rate = (90-s['minute'])/90;
				if s['playerOut'] == id_: 
					rate = s['minute']/90;
	contrib = rate*(pass_accuracy + duel_eff + fk_eff + shot_eff)/4;


def split_operate(rdd):
	try:
		a = rdd.map(lambda x: json.loads(x));
		match = a.first();
		events = a.filter(lambda x: x!=match);

		data = events.flatMap(lambda e: helper(e)) \
					.reduceByKey(lambda x, y: x + y) \
					.map(lambda x: (x[0][0], {x[0][1]: x[1]})) \
					.reduceByKey(lambda x, y: {**d1, **d2});
		data.foreach();
		print("PASS ACCURACY ----->>>> ", data.take(2));

	except Exception as e:
		print(e);

conf = SparkConf();
conf.setAppName("BigData")

sc = SparkContext(conf = conf)

ssc = StreamingContext(sc, 2);
ssc.checkpoint('checkpoint');

dataStream = ssc.socketTextStream('localhost', 6100);
dataStream.foreachRDD(split_operate);
dataStream.pprint();

ssc.start();
ssc.awaitTermination(4);
ssc.stop();
# spark-submit test.py > out.txt
