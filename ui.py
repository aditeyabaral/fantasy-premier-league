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
if 'req_type' in inp.keys():
	r_type = inp["req_type"]
else:
	r_type = 3

if r_type==1:                   #Compare 2 teams
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
		playerRoles = []
		for name in playerNames:
			playerIds.append(players_df.filter(players_df.name == name).first().Id)
			playerRoles.append(players_df.filter(players_df.name == name).first().role)
		gk, dfn, mf, f = 0, 0, 0, 0
		for r in playerRoles:
			if r=='DF':
				dfn+=1
			elif r=='GK':
				gk+=1
			elif r=='MD':
				mf+=1
			elif r=='FW':
				f+=1
		if gk != 1 or dfn < 2 or mf < 2 or f < 1:
			out = {"status":"Invalid Team"}
		else:
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
	f = open("predict_result.json", "w").write(j)
elif r_type==2:                                     #Player Profile
	name = inp["name"]
	print("NAME:", name)
	r = players_df.filter(players_df.name == name).first()
	print(r)

	data = {"name": name, "birthArea":r.birthArea, "birthDate":r.birthDate, "foot":r.foot, "role": r.role, "height":r.height,	\
			"weight": r.weight}
	playerId = r.Id
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
	f = open("player_result.json", "w").write(j)

elif r_type==3:              #Match Info
	date = inp['date']
	label = inp['label']

	with open('Match.json', 'r') as f:
		# matches = json.loads(f.read())
		text = f.read()
		text = '['+text[:-1]+']'
		matches = json.loads(text)

	final_match = dict()

	for match in matches:
		if (date==match['dateutc'].split()[0]) and (label==match['label']):
			final_match['date'] = date
			final_match['duration'] = match['duration']
			winner = match['winner']
			if winner==0:
				team_name = 'draw'
			else:
				team_name = teams_df.filter(teams_df.Id == winner).first()['name']
			final_match['winner'] = team_name
			final_match['venue'] = match['venue']
			final_match['gameweek'] = match['gameweek']

			final_match['goals'] = list()
			final_match['own_goals'] = list()
			final_match['yellow_cards'] = list()
			final_match['red_cards'] = list()

			for team in match['teamsData']:
				team_id = team
				print("TEAM ID:", team_id)
				team_name = teams_df.filter(teams_df.Id==team).first().name
				print(team)
				for player in match['teamsData'][team]['formation']['bench']:
					goals_dict = dict()
					own_goals_dict = dict()
			
					playerId = player['playerId']
					name = players_df.filter(players_df.Id==playerId).first()['name']

					goals_dict['name'] = name
					own_goals_dict['name'] = name

					goals_dict['team'] = team_name
					own_goals_dict['team'] = team_name
					
					goals_dict['number_of_goals'] = player['goals']
					own_goals_dict['number_of_goals'] = player['ownGoals']

					final_match['goals'].append(goals_dict)
					final_match['own_goals'].append(own_goals_dict)
					final_match['yellow_cards'].append(name)
					final_match['red_cards'].append(name)

				for player in match['teamsData'][team]['formation']['lineup']:
					goals_dict = dict()
					own_goals_dict = dict()
			
					playerId = player['playerId']
					name = players_df.filter(players_df.Id==playerId).first()['name']

					goals_dict['name'] = name
					own_goals_dict['name'] = name

					goals_dict['team'] = team_name
					own_goals_dict['team'] = team_name
					
					goals_dict['number_of_goals'] = player['goals']
					own_goals_dict['number_of_goals'] = player['ownGoals']

					final_match['goals'].append(goals_dict)
					final_match['own_goals'].append(own_goals_dict)
					final_match['yellow_cards'].append(name)
					final_match['red_cards'].append(name)
			break

	j = json.dumps(final_match)
	if final_match == {}:
		f = open("match_details.json", "w").write('{"status":"Not Found"}')
	else:
		f = open('match_details.json', 'w').write(j)
