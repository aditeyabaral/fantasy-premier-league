#import findspark
#findspark.init()
'''Importing the libraries'''
import json
from pyspark import SparkConf, SparkContext 
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import lit
from pyspark.sql import SQLContext, SparkSession
import sys
from shutil import rmtree
import os

count = 0
####################################################################################################################
########################################### Spark Initialisation ###################################################
####################################################################################################################

conf = SparkConf()
conf.setAppName("FPL")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = SparkSession(sc)
ssc = StreamingContext(sc, 5)
ssc.checkpoint("checkpoint_FPL")


####################################################################################################################
###################################### Reading players.csv, teams.csv ##############################################
####################################################################################################################

# Reading players and teams csv files
players_df = sqlContext.read.load("data/players.csv", format="csv", header="true", inferSchema="true")
teams_df = sqlContext.read.load("data/teams.csv", format="csv", header="true", inferSchema="true")


####################################################################################################################
############################################ Required functions ####################################################
####################################################################################################################


def getSparkSessionInstance(sparkConf):
    if ('spark' not in globals()):
        globals()['spark'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['spark']

def checkMatchRecord(record):
	'''
	checks if its a match record
	'''
	record = json.loads(record)
	return 'wyId' in record.keys()

def checkEventRecord(record):
	'''
	checks if its an event record
	'''
	record = json.loads(record)
	return 'eventId' in record.keys() 

def getMetrics(record):
	'''
	metric values
	'''

	record = json.loads(record)

	'''
	TUPLE INFORMATION:
	(acc normal pass) anp,
	 (accurate key pass) akp, 
	 (normal pass) np, 
	 (key pass) kp
	(duel won) dw, 
	(neutral duel) nd,
	 (total duel) td,
	 shots,
	 shots on target and goals,
	 shots on target and not goals,
	 shots on target
	 fouls
	 own goals
	 free kicks
	 effective free kicks
	 penalties scored
	 goals
	'''

	event_id = record["eventId"]
	matchId = record["matchId"]
	pid = record["playerId"]
	tags = [i["id"] for i in record["tags"]]
	own_goals = 0
	goals = 0

	#Check for goal
	if 101 in tags:
		goals = 1

	#Check for own goal
	if 102 in tags:
		own_goals=1

	# pass event
	if event_id == 8:
		accurate_normal_passes = 0; accurate_key_passes = 0; normal_passes = 0; key_passes = 0;

		if 302 in tags:
			key_passes = 1
		else:
			normal_passes = 1
		if 1801 in tags and 302 in tags:
			accurate_key_passes = 1
		elif 1801 in tags:
			accurate_normal_passes = 1
		return (pid, (accurate_normal_passes, accurate_key_passes, normal_passes, key_passes, 0, 0, 0,0,0,0, 0, 0, own_goals,0, 0, 0, goals, matchId))

	# duel event
	elif event_id == 1:
		duels_won = 0; neutral_duels = 0; total_duels = 1;
		if 703 in tags:
			duels_won = 1
		if 702 in tags:
			neutral_duels = 1
		return (pid, (0, 0, 0, 0, duels_won, neutral_duels, total_duels,0,0,0, 0, 0, own_goals,0, 0, 0, goals, matchId))

	#shot event
	elif event_id==10:
		number_of_shots=1
		on_target_and_goal=0
		on_target_and_no_goal=0
		on_target=0
		if 1801 in tags:
			on_target += 1
		if 1801 in tags and 101 in tags:
			on_target_and_goal += 1
		if 1801 in tags and 101 not in tags:
			on_target_and_no_goal += 1
		return (pid, (0, 0, 0, 0, 0, 0, 0, number_of_shots, on_target_and_goal, on_target_and_no_goal,on_target, 0, own_goals,0, 0, 0, goals, matchId))

	#free kick 

	elif event_id==3:
		total_free_kicks = 1; effective_free_kicks = 0; penalty_goals = 0;
		if 1801 in tags:
			effective_free_kicks += 1
		if record["subEventId"]==35 and 101 in tags:
			penalty_goals += 1;
		return (pid, (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, own_goals,total_free_kicks,effective_free_kicks,penalty_goals, goals, matchId))

	#Foul loss
	elif event_id==2:
		return (pid, (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, own_goals,0, 0, 0, goals, matchId))

	# For now, return all zeros (because its neither pass nor duel event)
	return (pid, (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, own_goals,0, 0, 0, goals, matchId))

def metricsCounterCalc(new, old):
	'''
	updates new state of metric counts
	'''
	a, b, c, d, e, f, g,h,i,j,k,l,m,n,o,p,q = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	m_id = -1
	for metrics in new:
		a += metrics[0]
		b += metrics[1]
		c += metrics[2]
		d += metrics[3]
		e += metrics[4]
		f += metrics[5]
		g += metrics[6]
		h += metrics[7]
		i += metrics[8]
		j += metrics[9]
		k += metrics[10]
		l += metrics[11]
		m += metrics[12]
		n += metrics[13]
		o += metrics[14]
		p += metrics[15]
		q += metrics[16]
		m_id = max(m_id, metrics[17])
	if old is None:
		return (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, m_id)
	if old[-1] != m_id:
		return (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, m_id)
	return (a + old[0], b + old[1], c + old[2], d + old[3], e + old[4], f + old[5], g + old[6], h + old[7], i + old[8], j + old[9], k + old[10], l + old[11], m + old[12], n + old[13], o + old[14], p + old[15], q + old[16], old[-1])

def getFinalMetrics(new_state, old_state):
	'''
	calculates final metrics
	'''
	new_state = new_state[0]
	try:
		pass_accuracy = (new_state[0] + new_state[1] * 2) / (new_state[2] + new_state[3] * 2)
	except:
		pass_accuracy = 0
	try:
		duel_effectiveness = (new_state[4] + new_state[5] * 0.5) / (new_state[6])
	except:
		duel_effectiveness = 0
	try:
		shot_effectiveness = (new_state[8] + (new_state[9]*0.5))/new_state[7]
	except:
		shot_effectiveness = 0

	try:
		shots_on_target = new_state[10]
	except:
		shots_on_target = 0

	try:
		fouls, own_goals, goals=new_state[11],new_state[12],new_state[16]
	except:
		fouls,own_goals, goals = 0,0,0

	try:
		free_kick_effectiveness = (new_state[14]+new_state[15])/new_state[13]
	except:
		free_kick_effectiveness = 0


	return (pass_accuracy, duel_effectiveness, shot_effectiveness, fouls, own_goals, shots_on_target, free_kick_effectiveness, goals)

def updatePlayerRating(new_state, old_state):
	'''
	updates player rating after every match
	'''
	try:
		updated_state = new_state[0][0]
		time_on_pitch = new_state[0][1][1] - new_state[0][1][0]
		time_on_pitch = 90
		pass_accuracy = updated_state[0]
		duel_effectiveness = updated_state[1]
		shot_effectiveness = updated_state[2]
		fouls = updated_state[3]
		own_goals = updated_state[4]
		shots_on_target = updated_state[5]
		if old_state is None:
			old_state = 0.5
		player_contribution = (pass_accuracy + duel_effectiveness + shot_effectiveness + shots_on_target) / 4
		#Penalise
		player_contribution = player_contribution - ((0.005*fouls + 0.05*own_goals)*player_contribution)
		finalContrib = (player_contribution + old_state) / 2
		if time_on_pitch == 90:
			return (1.05*finalContrib, 1.05*finalContrib - old_state)
		else:
			return ((time_on_pitch/90)*finalContrib, (time_on_pitch/90)*finalContrib - old_state)
	except:
		return (old_state[0], 0)


def getPlayerProfile(new_state, old_state):
	new_state = new_state[0]
	if old_state is None:
		fouls = new_state[3]
		goals = new_state[-1]
		own_goals = new_state[4]
		pass_accuracy = new_state[0]
		shots_on_target = new_state[5]
	else:
		fouls = new_state[3] + old_state[0]
		goals = new_state[-1] + old_state[1]
		own_goals = new_state[4] + old_state[2]
		pass_accuracy = new_state[0] + old_state[3]
		shots_on_target = new_state[5] + old_state[4]
	return (fouls,goals,own_goals, pass_accuracy, shots_on_target)

def getPlayerListFromMatch(m):
	m = json.loads(m)
	players_subst_stats = []
	for t in m["teamsData"]:
		team_data = m["teamsData"][t]
		sub_data = m["teamsData"][t]["formation"]["substitutions"]

		inPlayers = [s["playerIn"] for s in sub_data]
		outPlayers = [s["playerOut"] for s in sub_data]
		subTime = [s["minute"] for s in sub_data]

		bench_players = [p["playerId"] for p in team_data["formation"]["bench"]]
		starting_xi = [p["playerId"] for p in team_data["formation"]["lineup"]]
		for pId in starting_xi:
			try:
				idx = outPlayers.index(pId)
				players_subst_stats.append((pId, (0, subTime[idx])))
			except ValueError:
				players_subst_stats.append((pId, (0, 90)))
		for pId in bench_players:
			try:
				idx = inPlayers.index(pId)
				players_subst_stats.append((pId, (subTime[idx], 90)))
			except ValueError:
				players_subst_stats.append((pId, (-1, -1)))
	return players_subst_stats

def getTeamIDforPlayer(m):
	m = json.loads(m)
	player_team_data = []
	for t in m["teamsData"]:
		team_data = m["teamsData"][t]
		bench_players = [p["playerId"] for p in team_data["formation"]["bench"]]
		starting_xi = [p["playerId"] for p in team_data["formation"]["lineup"]]
		all_players = bench_players+starting_xi
		for pId in all_players:
			player_team_data.append((pId, int(t)))
	return player_team_data
				

def getJoinedPlayers(p1, p2):
	p1_p2 = p1.cartesian(p2)
	return p1_p2

def mapPlayers(x):
	# x = rdd.collect()
	return [[x[0][0], x[1][0]], [[0, x[0][1][-1]], [0, x[1][1][-1]], 0.5]]



def json_write(match):
	m = json.loads(match)
	m = json.dumps(m, indent = 4) 
	with open('Match.json', 'a') as f:
		f.write(m)
	return match

def save(rdd):
	if os.path.exists("/home/hduser_/Desktop/fantasy-premier-league/player_profile_data"):
		rmtree("/home/hduser_/Desktop/fantasy-premier-league/player_profile_data")
	rdd.saveAsTextFile("/home/hduser_/Desktop/fantasy-premier-league/player_profile_data")

####################################################################################################################
############################################## Driver Function #####################################################
####################################################################################################################

dataStream = ssc.socketTextStream("localhost", 6100)


### Match information
match = dataStream.filter(checkMatchRecord)
match.pprint()

new_match = match.map(lambda x: json_write(x))
new_match.pprint()

playerSubs = match.flatMap(lambda x: getPlayerListFromMatch(x))
playerSubs.pprint()

playerTeam = match.flatMap(lambda x: getTeamIDforPlayer(x))
playerTeam.pprint()

### Events
events = dataStream.filter(checkEventRecord)
events.pprint()

### Metrics
playerEventMetrics = events.map(getMetrics)
playerEventMetrics.pprint()

### Metrics Counts
playerMetricsCounter = playerEventMetrics.updateStateByKey(metricsCounterCalc)
playerMetricsCounter.pprint(30)

### Final Metrics
finalPlayerMetrics = playerMetricsCounter.updateStateByKey(getFinalMetrics)
finalPlayerMetrics.pprint()

player_profile = finalPlayerMetrics.updateStateByKey(getPlayerProfile)
player_profile.foreachRDD(save)


playerStats = finalPlayerMetrics.join(playerTeam)
playerStats.pprint()


pairPlayers = playerStats.transformWith(getJoinedPlayers, playerStats)
pairPlayers.pprint()


pairPlayersUpdated = pairPlayers.map(mapPlayers)
pairPlayersUpdated.pprint()


playerData = finalPlayerMetrics.join(playerSubs)
playerData.pprint()

### Player Rating
player_rating = playerData.updateStateByKey(updatePlayerRating)
finalPlayerRating = player_rating.join(playerTeam)
finalPlayerRating.saveAsTextFiles("rating-info")

'''
####################################################################################################################
############################################## Begin Streaming #####################################################
####################################################################################################################
'''
ssc.start()
ssc.awaitTermination(100)	
ssc.stop()
