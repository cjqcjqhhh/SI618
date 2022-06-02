# Word count example in Pyspark
'''
To run on Fladoop cluster

spark-submit --master yarn  --num-executors 2 --executor-memory 1g --executor-cores 2 spark_wordcount.py
'''
import re
from pyspark import SparkContext
sc = SparkContext(appName="WordCount")

WORD_RE = re.compile(r"\b[\w']+\b")
input_file = sc.textFile("hdfs:///user/cbudak/books")

word_counts = input_file.flatMap(lambda line: WORD_RE.findall(line)) \
                        .map(lambda word: (word, 1)) \
                        .reduceByKey(lambda a, b: a + b)

word_counts_sorted = word_counts.sortBy(lambda x: x[1], ascending = False)
word_counts_sorted.take(5)
word_counts_sorted.saveAsTextFile("hdfs:///user/cbudak/spark_wordcount_output") #replace cbudak with your uniqname
word_counts_sorted.map(lambda t : t[0] + '\t' + str(t[1])).saveAsTextFile('spark_wordcount_output2')

#afterwards on the login node, do:
#hdfs dfs -get spark_wordcount_output spark_wordcount_output
#hdfs dfs -get spark_wordcount_output2 spark_wordcount_output2
#you should have both the folders in your home directory now
#if you will rerun the code, don't forget to first remove the old output
#hdfs dfs -rm -r spark_wordcount_output
#hdfs dfs -rm -r spark_wordcount_output2
#you can check status of job: yarn application -status application_1596490614397_4945 (replace with application id referred to as <id> from now on)
#yarn logs -applicationId <id> to get  the logs
#yarn application -kill <id> to kill the job

