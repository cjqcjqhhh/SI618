# Calculate the total stars for each business category
# Original by Dr. Yuhang Wang and Josh Gardner
# Updated by Danaja Maldeniya
'''
To run on Cavium cluster:
spark-submit --master yarn --queue umsi618f21 --num-executors 16 --executor-memory 1g --executor-cores 2 spark_avg_stars_per_category.py

To get results:
hadoop fs -getmerge total_reviews_per_category_output total_reviews_per_category_output.txt
'''

import json
from pyspark import SparkContext
sc = SparkContext(appName="PySparksi618f19_total_reviews_per_category")

input_file = sc.textFile("/var/umsi618f21/hw5/yelp_academic_dataset_business.json")

def cat_reviews(data):
    cat_review_list = []
    stars = data.get('stars', 0)
    categories_raw = data.get('categories', None)
    city = data.get('city', None)
    review_count = data.get('review_count', 0)
    try:
        wheelchair = int(bool(data.get("attributes").get("WheelchairAccessible", False)))
    except:
        wheelchair = 0
    try:
        business_parking = json.loads(
                        data.get("attributes").get("BusinessParking", {None: False}).replace("'", '"') \
                            .replace("True", "true").replace("False", "false")
                    )
        parking = int(
            business_parking["garage"] or business_parking["street"] or business_parking["lot"]
            )
    except:
        parking = 0
        
    wheelchair = wheelchair if wheelchair else 0
    parking = parking if parking else 0
    
    if categories_raw:
        categories = categories_raw.split(', ')
        for c in categories:
            cat_review_list.append(
                (
                    (city, c), 
                    (1, stars, review_count, wheelchair, parking)
                    )
                )
    else:
        cat_review_list.append(
            (
                (city, "Unknown"), 
                (1, stars, review_count, wheelchair, parking)
                )
            )
    
    return cat_review_list

cat_stars = input_file.map(
    lambda line: json.loads(line)
    ).flatMap(
        cat_reviews
        ).reduceByKey(
            lambda x, y: (
                x[0] + y[0],
                0 if (x[2] + y[2] == 0) else (x[1]*x[2] + y[1]*y[2])/(x[2] + y[2]),
                x[2] + y[2],
                x[3] + y[3],
                x[4] + y[4]
            )
        )

cat_stars.collect()
cat_stars.saveAsTextFile("total_reviews_per_category_output")