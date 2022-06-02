import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import *



#Data from https://www1.nyc.gov/site/nypd/stats/reports-analysis/stopfrisk.page
saf = sqlContext.read.csv("hdfs:///var/umsi618f21/stopAndFrisk2009.csv", header=True)
saf.printSchema()

saf.select("frisked").show()
saf.select("frisked").distinct().show()

saf.registerTempTable("saf")

q1 = sqlContext.sql("SELECT year, pct, crimsusp, arstmade, sumissue, frisked, searched, contrabn, sex, race, age FROM saf")
q1.show()


#Let's get some summary stats by race
#Here are the race categories:
#race	 	NOT LISTED
#	A	ASIAN/PACIFIC ISLANDER
#	B	BLACK
#	I	AMERICAN INDIAN/ALASKAN NATIVE
#	P	BLACK-HISPANIC
#	Q	WHITE-HISPANIC
#	W	WHITE
#	X	UNKNOWN
#	Z	OTHER
# Info about the data at: https://www1.nyc.gov/assets/nypd/downloads/zip/analysis_and_planning/stop-question-frisk/SQF-File-Documentation.zip


# Let's see how many stops there are for each race and what is the likelihood of being frisked conditional oon being stopped
q2 = sqlContext.sql("SELECT AVG(cast((frisked == 'Y') as int)) as friskedRatio, count(*) as stops, race from saf GROUP BY race ORDER BY stops")
q2.show()


# The goal  of stop and frisk was to stop crime. A good heuristic is whether they find a contraband at the end. Let's see how that success varies by race
q3 = sqlContext.sql('''SELECT CAST(AVG(cast((contrabn == 'Y') as int)) as decimal (10,4)) as contraBnRatio, 
					CAST(AVG(cast((arstmade == 'Y') as int)) as decimal (10,4)) as arrestRatio, 
					CAST(AVG(cast((frisked == 'Y') as int)) as decimal (10,4)) as friskedRatio, count(*) as stops, race from saf GROUP BY race ORDER BY stops''')
q3.show()

q3_5 = sqlContext.sql('''SELECT avg(cast((contrabn == 'Y') as int))
                        from saf GROUP BY race''')
q3_5.show()


# You can collect the results to create a non-distributed object
q3.collect()
#you can also write to file
q3.rdd.map(lambda i: '\t'.join(str(j) for j in i)) \
	.saveAsTextFile('stopAndFriskSimpleStats')

#use .rdd to convert DF to rdd
q3rdd = q3.rdd
#use toDF to convert rdd to df
q3again = q3rdd.toDF() 
#you actually do not need the schema here since the rdd object converted 
#from DF already has the column names


# Ok, let's build up even more. You might say maybe the police stops are driven by overall population?
# Let's account for that

# Population Data - 2010 Census
pop = sqlContext.read.csv("hdfs:///var/umsi618f21/nyc_2010pop_2020precincts.csv", header=True)
pop.registerTempTable('pop')

q4 = sqlContext.sql('''SELECT precinct_2020 as pct, P0020001 as Total_Pop, CAST(P0020005*100/P0020001 as decimal(10,2)) as White,
                    CAST(P0020006*100/P0020001 as decimal(10,2)) as Black, CAST(P0020002*100/P0020001 as decimal(10,2)) as Hispanic,
                    CAST(P0020008*100/P0020001 as decimal(10,2)) as Asian
                    FROM pop
                    ''')
q4 = q4.withColumn('pct', trim(q4.pct))
q4.registerTempTable('pop')

# Stop Counts by precinct in 2010
q5 = sqlContext.sql('''SELECT pct, COUNT(*) as stops, 
					CAST(AVG(cast((contrabn == 'Y') as int)) as decimal (10,2)) as contraBnRatio, 
					CAST(AVG(cast((arstmade == 'Y') as int)) as decimal (10,2)) as arrestRatio, 
					CAST(AVG(cast((frisked == 'Y') as int)) as decimal (10,2)) as friskedRatio, 
                    CAST(SUM(cast((race == 'B') as int))*100/COUNT(*) as decimal (10,2)) as percentstop_black, 
                    CAST(SUM(cast((race == 'W') as int))*100/COUNT(*) as decimal (10,2)) as percentstop_white,
                    CAST(SUM(cast((race == 'Q') as int))*100/COUNT(*) as decimal (10,2)) as percentstop_hispanic,
                    CAST(SUM(cast((race == 'A') as int))*100/COUNT(*) as decimal (10,2)) as percentstop_asian
                    FROM saf
                    WHERE pct IS NOT NULL
                    GROUP BY pct
                    ORDER BY pct ASC
                    ''')
q5 = q5.withColumn('pct', trim(q5.pct))
q5.registerTempTable('precintStops')


#Now lets merge the two
q6 = sqlContext.sql('''SELECT pop.pct, precintStops.stops, precintStops.arrestRatio, 
					precintStops.friskedRatio, precintStops.contraBnRatio,
					precintStops.percentstop_black, pop.Black, precintStops.percentstop_white, pop.White, 
					precintStops.percentstop_hispanic, pop.Hispanic, precintStops.percentstop_asian, pop.Asian
                    FROM precintStops
                    JOIN pop 
                    ON precintStops.pct  = pop.pct 
                    ORDER BY pop.pct ASC
                    ''')
q6.registerTempTable('precintSummary')
q6.show()
q6.rdd.map(lambda i: '\t'.join(str(j) for j in i)) \
	.saveAsTextFile('stopAndFriskAndDemoStats')



# There is so much mooreo you can do. Maybe consider computing the success scores per racial group. Or gender. Or age. Or determine how things changed over time. 
# NYC had various cahnges to their stop and frisk program. What were their effects?


