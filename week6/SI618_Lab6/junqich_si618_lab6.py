import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import *

lol = sqlContext.read.csv("hdfs:///var/umsi618f21/lab6/na_ranked_team.csv", header=True)
lol.registerTempTable("lol")


# Question 1
sql1 = [
    "SELECT *,",
    "(CAST(kill as int) + CAST(assist as int)) / (CAST(death as int) + 1) as kda",
    "FROM lol"
] # add a column called kda
q1 = sqlContext.sql(" ".join(sql1))
q1.registerTempTable("lol_KDA")

sql2 = [
    "SELECT summonerId, COUNT(*) as matches, AVG(kda) as avgkda",
    "FROM lol_KDA",
    "GROUP BY summonerId",
    "HAVING matches >= 10",
    "ORDER BY avgkda DESC, matches DESC"
]
q2 = sqlContext.sql(" ".join(sql2))
q2.rdd.map(lambda i: '\t'.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_lab6_output_1')


# Question 2 (FROM q1)
sql3 = [
    "SELECT matchId, winner, AVG(kda) as avgteamkda",
    "FROM lol_KDA",
    "GROUP BY matchId, winner"
] # count the average team kda
q3 = sqlContext.sql(" ".join(sql3))
q3.registerTempTable("lol_avgTeamKDA")

sql4 = [
    "SELECT a.*, b.avgteamkda FROM lol_KDA a",
    "JOIN lol_avgTeamKDA b",
    "ON a.matchId = b.matchId AND a.winner = b.winner"
] # add a column of average team KDA
q4 = sqlContext.sql(" ".join(sql4))
q4.registerTempTable("lol_KDA")

sql5 = [
    "SELECT summonerId, COUNT(*) as matches,",
    "AVG((kda / (avgteamkda + 1))) as normkda",
    "FROM lol_KDA",
    "GROUP BY summonerId",
    "HAVING matches >= 10",
    "ORDER BY normkda DESC, matches DESC"
]
q5 = sqlContext.sql(" ".join(sql5))
q5.rdd.map(lambda i: '\t'.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_lab6_output_2')


# Question 3 (FROM q1)
sql6 = [
    "SELECT *",
    "FROM lol_KDA",
    "WHERE winner like '1'"
] # count the average team kda
q6 = sqlContext.sql(" ".join(sql6))
q6.registerTempTable("lol_win")

sql7 = [
    "SELECT *",
    "FROM lol_KDA",
    "WHERE winner like '0'"
] # count the average team kda
q7 = sqlContext.sql(" ".join(sql7))
q7.registerTempTable("lol_lose")

sql8 = [
    "SELECT a.matchId, a.predictedRole,",
    "a.championName as champion1, b.championName as champion2,"
    "a.kda as kda1, b.kda as kda2",
    "FROM lol_win a",
    "JOIN lol_lose b", 
    "ON a.matchId = b.matchId AND a.predictedRole = b.predictedRole",
    "ORDER BY champion1, champion2"
] # find the pairs of same role from opposite team
q8 = sqlContext.sql(" ".join(sql8))
q8.registerTempTable("lol_pair")

q9_1 = sqlContext.sql("SELECT * FROM lol_pair WHERE champion1 < champion2")
q9_1.registerTempTable("lol_pair1")
q9_2 = sqlContext.sql("SELECT matchId, predictedRole, champion2 as champion1, champion1 as champion2, kda2 as kda1, kda1 as kda2 FROM lol_pair WHERE champion1 > champion2")
q9_2.registerTempTable("lol_pair2")
q9 = sqlContext.sql("SELECT * FROM lol_pair1 UNION ALL SELECT * FROM lol_pair2")
q9.registerTempTable("lol_pair_sorted") # sorted the pair by alphabetical order

sql10 = [
    "SELECT champion1, champion2, predictedRole,",
    "COUNT(*) as matches, AVG(kda1 / kda2) as avgkdaratio",
    "FROM lol_pair_sorted",
    "GROUP BY champion1, champion2, predictedRole",
    "HAVING matches >= 10",
    "ORDER BY champion1, predictedRole, matches DESC, champion2"
] # count the average team kda
q10 = sqlContext.sql(" ".join(sql10))
q10.rdd.map(lambda i: '\t'.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_lab6_output_3')