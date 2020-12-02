import os
import json

metrics_dirs = sorted([d for d in os.listdir(".") if d.startswith("rating-info-")])

players = open("data/players.csv").read().split("\n")
playerIds = list(map(lambda x: x.split(",")[-1], players))
playerIds = list(filter(lambda x: x != "Id" and x != '', playerIds))
playerIds = list(map(int, playerIds))
rate_dict = {pId:0.5 for pId in playerIds}
chemistry = dict()
for p in playerIds:
    chemistry[p] = dict()
    for p1 in playerIds:
        if p != p1:
            chemistry[p][p1] = 0.5

for d in metrics_dirs:
    text = ''
    partFiles = [f for f in os.listdir(d) if f.startswith("part-")]
    for part in partFiles:
        text += open(d+"/"+part).read()
    text = [line for line in text.split("\n") if line != '']
    rate = list(map(eval, text))
    #print(rate)
    for t in rate:
        id1 = t[0]
        team1 = t[-1][-1]
        d1 = t[1][0][1]
        for t1 in rate:
            id2 = t1[0]
            team2 = t1[-1][1]
            d2 = t1[1][0][1]
            mag = abs((d2 + d1)/2)
            if id1 != id2:
                if team1 != team2:
                    if (d1>0 and d2>0) or (d1<0 and d2 <0): 
                        chemistry[id1][id2] -= mag
                        chemistry[id2][id1] -= mag
                    else:
                        chemistry[id1][id2] += mag
                        chemistry[id2][id1] += mag
                elif team1 == team2:
                    if (d1>0 and d2>0) or (d1<0 and d2 <0): 
                        chemistry[id1][id2] += mag
                        chemistry[id2][id1] += mag
                    else:
                        chemistry[id1][id2] -= mag
                        chemistry[id2][id1] -= mag
x = json.dumps(chemistry)
f = open("sample_ip/chemistry.json", "w").write(x)

last_dir = metrics_dirs[-1]
text = ''
partFiles = [f for f in os.listdir(d) if f.startswith("part-")]
for part in partFiles:
    text += open(d+"/"+part).read()
text = [line for line in text.split("\n") if line != '']
rate = list(map(eval, text))
for t in rate:
    playerId = t[0]
    rating = t[1][0][0]
    rate_dict[playerId] = rating

y = json.dumps(rate_dict)
f = open("sample_ip/rating.json", "w").write(y)