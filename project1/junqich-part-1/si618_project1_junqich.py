import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

# --- Data Pre-processing --- #

# read in data
covid_raw = sqlContext.read.csv("covid.csv", header=True)
vacc_raw = sqlContext.read.csv("country_vaccinations.csv", header=True)

# select and clean data
covid = covid_raw.select("location", "iso_code", "date", "total_cases", "new_cases", "total_cases_per_million", "new_cases_per_million", "reproduction_rate", "stringency_index")
covid = covid.withColumnRenamed("location", "country")
covid = covid.withColumn("year", year(covid.date))
covid = covid.withColumn("month", month(covid.date))
covid = covid.withColumn("date",to_timestamp(col("date"))).withColumn("day", date_format(col("date"), "d"))
covid = covid.na.drop(subset=["stringency_index", "reproduction_rate"])
covid.registerTempTable("covid")

vacc = vacc_raw.select("country", "iso_code", "date", "daily_vaccinations", "daily_vaccinations_per_million")
vacc = vacc.withColumn("year", year(vacc.date))
vacc = vacc.withColumn("month", month(vacc.date))
vacc = vacc.withColumn("date",to_timestamp(col("date"))).withColumn("day", date_format(col("date"), "d"))
vacc = vacc.na.drop(subset=["daily_vaccinations", "daily_vaccinations_per_million"])
vacc.registerTempTable("vacc")

# covid.filter(covid.total_cases.isNull()).collect() # check if null

# --- Analysis --- #

# Question 1: Vaccination rate and reproduction_rate

q1 = sqlContext.sql('''SELECT country, year, AVG(reproduction_rate) as avg_reproduction_rate
                    FROM covid
                    GROUP BY country, year
                    ORDER BY country
                    ''')
q1.registerTempTable('covid_R')

q2 = sqlContext.sql('''SELECT country, year, SUM(daily_vaccinations_per_million) as tot_vacc_per_million
                    FROM vacc
                    WHERE year = 2021
                    GROUP BY country, year
                    ORDER BY country
                    ''')
q2.registerTempTable('vacc_tot')

q3 = sqlContext.sql('''SELECT c.country, c.year, avg_reproduction_rate, tot_vacc_per_million
                    FROM covid_R c
                    LEFT JOIN vacc_tot v 
                    ON c.country = v.country and c.year = v.year
                    ORDER BY c.country, c.year
                    ''')
q3.registerTempTable('covid_R_vacc')
q3.rdd.map(lambda i: ','.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_project_q1-1')

q3_1 = sqlContext.sql("SELECT * FROM covid_R_vacc WHERE year = 2021 ORDER BY tot_vacc_per_million DESC")
q3_1.rdd.map(lambda i: ','.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_project_q1-2')

q3_2 = sqlContext.sql("SELECT country, year, avg_reproduction_rate FROM covid_R_vacc")
q3_2.rdd.map(lambda i: ','.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_project_q1-3')

# can also perfrom monthly analysis here

# Question 2: Vaccination rate and stringency_index
q4 = sqlContext.sql('''SELECT country, year, AVG(stringency_index) as avg_stringency_index
                    FROM covid
                    GROUP BY country, year
                    ORDER BY country, year
                    ''')
q4.registerTempTable('covid_S')

q5 = sqlContext.sql('''SELECT c.country, c.year, avg_stringency_index, tot_vacc_per_million
                    FROM covid_S c
                    LEFT JOIN vacc_tot v 
                    ON c.country = v.country and c.year = v.year
                    ORDER BY c.country, c.year
                    ''')
q5.registerTempTable('covid_S_vacc')
q5.rdd.map(lambda i: ','.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_project_q2-1')

q5_1 = sqlContext.sql("SELECT * FROM covid_S_vacc WHERE year = 2021 ORDER BY tot_vacc_per_million DESC")
q5_1.rdd.map(lambda i: ','.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_project_q2-2')

q5_2 = sqlContext.sql("SELECT country, year, avg_stringency_index FROM covid_S_vacc")
q5_2.rdd.map(lambda i: ','.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_project_q2-3')


# Question 3: New cases rate and Vaccination rate

q6 = sqlContext.sql('''SELECT country, year, month, SUM(new_cases_per_million) as tot_new_cases_ppm
                    FROM covid
                    WHERE year = 2021
                    GROUP BY country, year, month
                    ORDER BY country, year, month
                    ''')
q6.registerTempTable('covid_case')

q7 = sqlContext.sql('''SELECT country, year, month, SUM(daily_vaccinations_per_million) as tot_vacc_ppm
                    FROM vacc
                    WHERE year = 2021
                    GROUP BY country, year, month
                    ORDER BY country, year, month
                    ''')
q7.registerTempTable('vacc_tot2')

q8 = sqlContext.sql('''SELECT c.country, c.year, c.month, tot_new_cases_ppm, tot_vacc_ppm
                    FROM covid_case c
                    JOIN vacc_tot2 v 
                    ON c.country = v.country and c.year = v.year and c.month = v.month
                    ORDER BY c.country, c.year, c.month
                    ''')
q8.registerTempTable('covid_case_vacc')
q8.rdd.map(lambda i: ','.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_project_q3-1')

q9 = sqlContext.sql('''SELECT country, year, SUM(tot_new_cases_ppm) as tot_new_cases_ppm, SUM(tot_vacc_ppm) as tot_vacc_ppm
                    FROM covid_case_vacc c
                    GROUP BY country, year
                    ORDER BY tot_new_cases_ppm DESC, tot_vacc_ppm DESC
                    ''')
q9.rdd.map(lambda i: ','.join(str(j) for j in i)) \
	.saveAsTextFile('junqich_si618_project_q3-2')



