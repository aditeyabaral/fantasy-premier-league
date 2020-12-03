'''
Importing relevant libraries
'''
import json
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import lit
from pyspark.sql import SQLContext, SparkSession
import sys
from shutil import rmtree
import os

'''
Spark Driver Settings
'''
conf = SparkConf()  # Initialize Spark Context configuration
conf.setAppName("FPL")
sc = SparkContext(conf=conf)  # Create Spark context
sqlContext = SQLContext(sc)  # Crate SQL context to access dataframe objects
spark = SparkSession(sc)  # SparkSession creation
ssc = StreamingContext(sc, 5)  # Start streaming context
# Set checkpoint directory for all updateStateByKey operations
ssc.checkpoint("checkpoint_FPL")


# Reading players and teams csv files
players_df = sqlContext.read.load(
    "data/players.csv", format="csv", header="true", inferSchema="true")
teams_df = sqlContext.read.load(
    "data/teams.csv", format="csv", header="true", inferSchema="true")


def getSparkSessionInstance(sparkConf):
	'''
	Helper function to retrieve SparkSession instance at the worker nodes
	'''
	if ('spark' not in globals()):
		globals()['spark'] = SparkSession\
			.builder\
			.config(conf=sparkConf)\
			.getOrCreate()
	return globals()['spark']

def checkMatchRecord(record):
	'''
	Check if an input record is a match record
	Return True if the input is a match record, else return False
	'''
	record = json.loads(record)
	return 'wyId' in record.keys()

def checkEventRecord(record):
	'''
	Check if an input record is a event record
	Return True if the input is a event record, else return False
	'''
	record = json.loads(record)
	return 'eventId' in record.keys() 

def getMetrics(record):
	'''
	Retrieve metric values for a single event record

	Return Value Format:
	Accurate normal passes,
	Accurate key passes, 
	Normal passes, 
	Key passes,
	Duels won, 
	Neutral duels,
	Total duels,
	Shots,
	Shots on target and goals,
	Shots on target and not goals,
	Shots on target,
	Fouls,
	Own goals,
	Free kicks,
	Effective free kicks.
	Penalties scored,
	Goals
	'''
	record = json.loads(record)
	event_id = record["eventId"]
	matchId = record["matchId"]
	pid = record["playerId"]
	tags = [i["id"] for i in record["tags"]]
	own_goals = 0
	goals = 0

	# Check for Goal
	if 101 in tags:
		goals = 1

	# Check for Own Goal
	if 102 in tags:
		own_goals=1

	# Pass Event
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

	# Duel Event
	elif event_id == 1:
		duels_won = 0; neutral_duels = 0; total_duels = 1;
		if 703 in tags:
			duels_won = 1
		if 702 in tags:
			neutral_duels = 1
		return (pid, (0, 0, 0, 0, duels_won, neutral_duels, total_duels,0,0,0, 0, 0, own_goals,0, 0, 0, goals, matchId))

	# Shot Event
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

	# Free Kick 
	elif event_id==3:
		total_free_kicks = 1; effective_free_kicks = 0; penalty_goals = 0;
		if 1801 in tags:
			effective_free_kicks += 1
		if record["subEventId"]==35 and 101 in tags:
			penalty_goals += 1;
		return (pid, (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, own_goals,total_free_kicks,effective_free_kicks,penalty_goals, goals, matchId))

	# Foul Loss
	elif event_id==2:
		return (pid, (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, own_goals,0, 0, 0, goals, matchId))

	# For now, return all zeros (because its neither pass nor duel event)
	return (pid, (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, own_goals,0, 0, 0, goals, matchId))

def metricsCounterCalc(new, old):
    '''
    Aggregate outputs of getMetrics for each key (each playerId)
    Outputs the sum of all parameters for a match for a player
    '''
    anp_ctr, akp_ctr, np_ctr, kp_ctr, dw_ctr, nd_ctr, td_ctr, sh_ctr, stg_ctr, stng_ctr,\
		 st_ctr, fl_ctr, og_ctr, fk_ctr, efk_ctr, pen_ctr, gl_ctr = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    m_id = -1
    for metrics in new:
        anp_ctr += metrics[0]
        akp_ctr += metrics[1]
        np_ctr += metrics[2]
        kp_ctr += metrics[3]
        dw_ctr += metrics[4]
        nd_ctr += metrics[5]
        td_ctr += metrics[6]
        sh_ctr += metrics[7]
        stg_ctr += metrics[8]
        stng_ctr += metrics[9]
        st_ctr += metrics[10]
        fl_ctr += metrics[11]
        og_ctr += metrics[12]
        fk_ctr += metrics[13]
        efk_ctr += metrics[14]
        pen_ctr += metrics[15]
        gl_ctr += metrics[16]
        m_id = max(m_id, metrics[17])
    if old is None:
        return (anp_ctr, akp_ctr, np_ctr, kp_ctr, dw_ctr, nd_ctr, td_ctr, sh_ctr, stg_ctr, stng_ctr, st_ctr, \
			 fl_ctr, og_ctr, fk_ctr, efk_ctr, pen_ctr, gl_ctr, m_id)
    if old[-1] != m_id:
        return (anp_ctr, akp_ctr, np_ctr, kp_ctr, dw_ctr, nd_ctr, td_ctr, sh_ctr, stg_ctr, stng_ctr, st_ctr, \
			 fl_ctr, og_ctr, fk_ctr, efk_ctr, pen_ctr, gl_ctr, m_id)
    return (anp_ctr + old[0], akp_ctr + old[1], np_ctr + old[2], kp_ctr + old[3], dw_ctr + old[4], nd_ctr + old[5], td_ctr + old[6], \
		  	 sh_ctr + old[7], stg_ctr + old[8], stng_ctr + old[9], st_ctr + old[10], fl_ctr + old[11], og_ctr + old[12], \
				   fk_ctr + old[13], efk_ctr + old[14], pen_ctr + old[15], gl_ctr + old[16], old[-1])

def getFinalMetrics(new_state, old_state):
	'''
	Calculates the final metrics for each player for each match, 
	Return the pass accuracy, duel effectiveness, shot effectiveness, number of free kicks, goals and fouls
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
	Update the player rating given the parameters from the finalMetrics function
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
		# Penalise
		player_contribution = player_contribution - ((0.005*fouls + 0.05*own_goals)*player_contribution)
		finalContrib = (player_contribution + old_state) / 2
		if time_on_pitch == 90:
			return (1.05*finalContrib, 1.05*finalContrib - old_state)
		else:
			return ((time_on_pitch/90)*finalContrib, (time_on_pitch/90)*finalContrib - old_state)
	except:
		return (old_state[0], 0)


def getPlayerProfile(new_state, old_state):
	'''
	Update the player profile for each new match that is streamed in
	'''
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
		pass_accuracy = (new_state[0] + old_state[3])/2
		shots_on_target = new_state[5] + old_state[4]
	return (fouls,goals,own_goals, pass_accuracy, shots_on_target)

def getPlayerListFromMatch(m):
	'''
	Return the playerId and a tuple with the inTime and outTime at which that player was substituted
	in or out

	This will be used for player rating computation
	'''
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
	'''
	For each player in a team that is either on the bench or in the starting lineup, 
	returns tuple of (playerId, teamId)

	This is used for chemistry computation
	'''
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
	'''
	Cartesian product of player metrics objects to be used for chemistry computation
	'''
	p1_p2 = p1.cartesian(p2)
	return p1_p2

def mapPlayers(x):
	# x = rdd.collect()
	return [[x[0][0], x[1][0]], [[0, x[0][1][-1]], [0, x[1][1][-1]], 0.5]]



def json_write(match):
	'''
	Save match records to persistent storage for servicing UI requests (type 3)
	'''
	m = json.loads(match)
	m = json.dumps(m, indent = 4) 
	with open('Match.json', 'a') as f:
		f.write(m)
	return match

def save(rdd):
	'''
	Save player profile data to persistent storage for servicing UI requests (type 2)
	'''
	if os.path.exists("/home/hduser_/Desktop/fantasy-premier-league/player_profile_data"):
		rmtree("/home/hduser_/Desktop/fantasy-premier-league/player_profile_data")
	rdd.saveAsTextFile("/home/hduser_/Desktop/fantasy-premier-league/player_profile_data")

dataStream = ssc.socketTextStream("localhost", 6100)            #Text input 

match = dataStream.filter(checkMatchRecord)						# Match Information
match.pprint()

new_match = match.map(lambda x: json_write(x)) 					#Write each match record to persistent storage
new_match.pprint()

playerSubs = match.flatMap(lambda x: getPlayerListFromMatch(x)) #Get playerId, (intime, outtime) data
playerSubs.pprint() 

playerTeam = match.flatMap(lambda x: getTeamIDforPlayer(x))		#Get (playerId, teamId) information
playerTeam.pprint()

# Events
events = dataStream.filter(checkEventRecord)					#Filter event records
events.pprint()

# Metrics
playerEventMetrics = events.map(getMetrics)						#Per event, per player, per match record
playerEventMetrics.pprint()

# Metrics Counts
playerMetricsCounter = playerEventMetrics.updateStateByKey(metricsCounterCalc) #Aggregate count of events per player per match
playerMetricsCounter.pprint(30)

# Final Metrics
finalPlayerMetrics = playerMetricsCounter.updateStateByKey(getFinalMetrics)    #Get metrics per player per match 
finalPlayerMetrics.pprint()

player_profile = finalPlayerMetrics.updateStateByKey(getPlayerProfile)			#Save metrics in player profile and write to persistent storage
player_profile.foreachRDD(save)


playerStats = finalPlayerMetrics.join(playerTeam)								#Get metrics data and team data for each player (used for chemistry)
playerStats.pprint()


pairPlayers = playerStats.transformWith(getJoinedPlayers, playerStats)			
pairPlayers.pprint()


pairPlayersUpdated = pairPlayers.map(mapPlayers)				#Initialize chemistry of all players
pairPlayersUpdated.pprint()


playerData = finalPlayerMetrics.join(playerSubs)				#Player Metric data and substitution data for computing the rating
playerData.pprint()

# Player Rating
player_rating = playerData.updateStateByKey(updatePlayerRating) #Player Rating computation
finalPlayerRating = player_rating.join(playerTeam)				#Plaer rating and team information used for chemistry
finalPlayerRating.saveAsTextFiles("rating-info")				#Save rating information for chemistry computation

'''
Start streaming job
'''
ssc.start()
ssc.awaitTermination(100)	
ssc.stop()
