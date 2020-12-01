import json
from pyspark import SparkConf, SparkContext 
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import lit
from pyspark.sql import SQLContext, SparkSession
import sys
from shutil import rmtree
import os

conf = SparkConf()
conf.setAppName("FPL")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

players_df = sqlContext.read.load("data/players.csv", format="csv", header="true", inferSchema="true")
teams_df = sqlContext.read.load("data/teams.csv", format="csv", header="true", inferSchema="true")


text = open(sys.argv[1]).read()
inp = json.loads(text)
r_type = inp["req_type"]

if r_type==1:
	team_strength = [0, 0]
	win_chance = [0, 0]
	chemistry = json.loads(open("sample_ip/chemistry.json").read())
	rating = json.loads(open("sample_ip/rating.json").read())
	team_keys = ['team1', 'team2']
	player_keys = ['player'+str(i) for i in range(1, 12)]
	for t in range(len(team_keys)):
		team_avg = 0
		teamData = inp[team_keys[t]]
		playerNames = [teamData[pk] for pk in player_keys]
		playerIds = []
		for name in playerNames:
			playerIds.append(players_df.filter(players_df.name == name).first().Id)
		team_sum = 0
		visited = []
		for p in playerIds:
			for p1 in playerIds:
				if p != p1:
					if (p1, p) in visited:
						continue
					else:
						team_sum += chemistry[str(p)][str(p1)]
		team_avg = team_sum/11
		total_sum = 0
		for p in playerIds:
			total_sum += rating[str(p)]*team_avg
		total_sum /= 11
		team_strength[t] = total_sum
	win_chance[0] = (0.5 + team_strength[0] - (sum(team_strength)/2))*100
	win_chance[1] = 100-win_chance[0]
	out = {"team1":{"name":inp["team1"]["name"], "winning chance": win_chance[0]}, \
			 "team2":{"name":inp["team2"]["name"], "winning chance": win_chance[1]}}
	j = json.dumps(out)
	f = open("predict-result.json", "w").write(j)
elif r_type==2:
	name = inp["name"]
	print("NAME:", name)
	r = players_df.filter(players_df.name == name).first()
	print(r)

	data = {"name": name, "birthArea":r.birthArea, "birthDate":r.birthDate, "foot":r.foot, "role": r.role, "height":r.height,	\
			"weight": r.weight}
	playerId = r.Id 
	#print(data)
	#print("player id is:", playerId)

	#[Row(name='Paul Pogba', birthArea='France', birthDate='1993-03-15', foot='right', role='MD', height=191, passportArea='Guinea', weight=84, Id=7936)]

	tt = None
	found = False
	files = [f for f in os.listdir("/home/hduser_/Desktop/fantasy-premier-league/player_profile_data") if f.startswith("part-")]
	for file in files:
		text = open("player_profile_data/"+file).read()
		items = [eval(x) for x in text.split("\n") if x != '']
		for i in items:
			if i[0] == playerId:
				tt = i
				found=True
				break
		if found:
			break

	fouls,goals,own_goals, pass_accu, shots_on_target = tt[1]
	data["fouls"]=fouls
	data["own goals"]=own_goals
	data["pass_acc"] = pass_accu
	data["shots on target"] = shots_on_target

	j = json.dumps(data)
	f = open("player-result.json", "w").write(j)

