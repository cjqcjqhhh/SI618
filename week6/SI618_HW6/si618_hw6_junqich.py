import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import *

# pyspark --master yarn --conf spark.ui.port="$(shuf -i 10000-60000 -n 1)"

bus = sqlContext.read.json("hdfs:///var/umsi618f21/hw6/yelp_academic_dataset_business.json")
bus.registerTempTable("bus")
rev = sqlContext.read.json("hdfs:///var/umsi618f21/hw6/yelp_academic_dataset_review.json")
rev.registerTempTable("rev")

# Question 1
q1 = sqlContext.sql("SELECT user_id, AVG(stars) as mean, STD(stars) as sd FROM rev GROUP BY user_id")
q1.registerTempTable("rev_sd")

q2 = sqlContext.sql("SELECT user_id, mean, IF(sd = 'NaN', 0, sd) as sd FROM rev_sd")
q2.registerTempTable("rev_sd")

q3 = sqlContext.sql("SELECT r1.*, (CASE WHEN r2.sd = 0 THEN 0 ELSE (r1.stars - r2.mean) / r2.sd END) as normUserRating FROM rev r1 JOIN rev_sd r2 ON r1.user_id = r2.user_id")
q3.registerTempTable("rev_normUser")

q4 = sqlContext.sql("SELECT business_id, AVG(normUserRating) as normBusRating FROM rev_normUser GROUP BY business_id ORDER BY normBusRating DESC")
q4.registerTempTable("rev_normBusiness")

q5 = sqlContext.sql("SELECT * FROM rev_normBusiness LIMIT 100")
q5.rdd.map(lambda i: '\t'.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_hw6_output_1')


# Question 2
q6 = sqlContext.sql("SELECT b.*, r.normBusRating FROM bus b JOIN rev_normBusiness r ON b.business_id = r.business_id")
q6.registerTempTable("bus_normBusiness")

q7 = sqlContext.sql("SELECT city, AVG(normBusRating) as avgBusRating FROM bus_normBusiness GROUP BY city ORDER BY avgBusRating DESC")
q7.rdd.map(lambda i: '\t'.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_hw6_output_2')


# Question 3
q8 = sqlContext.sql("SELECT business_id, AVG(normUserRating) as normBusRating FROM rev_normUser WHERE useful >= 1 GROUP BY business_id ORDER BY normBusRating DESC")
q8.registerTempTable("rev_normBusinessUseful")

q9 = sqlContext.sql("SELECT b.*, r.normBusRating FROM bus b JOIN rev_normBusinessUseful r ON b.business_id = r.business_id")
q9.registerTempTable("bus_normBusinessUseful")

q10 = sqlContext.sql("SELECT city, AVG(normBusRating) as avgBusRating FROM bus_normBusinessUseful GROUP BY city ORDER BY avgBusRating DESC")
q10.rdd.map(lambda i: '\t'.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_hw6_output_3')