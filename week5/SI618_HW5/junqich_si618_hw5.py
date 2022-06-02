# Calculate the total stars for each business category
# Original by Dr. Yuhang Wang and Josh Gardner
# Updated by Danaja Maldeniya
'''
To run on Cavium cluster:
spark-submit --master yarn --queue umsi618f21 --num-executors 16 --executor-memory 1g --executor-cores 2 spark_total_reviews_per_category.py

To get results:
hadoop fs -getmerge total_reviews_per_category_output total_reviews_per_category_output.tsv
'''

import json
from pyspark import SparkContext
sc = SparkContext(appName="PySparksi618f19_total_reviews_per_category")

input_file = sc.textFile("/var/umsi618f21/hw5/yelp_academic_dataset_business.json")

def cat_reviews(data):
    # data is a dict here
    cat_review_list = []
    # ---- KEYS ---- #
    city = data.get('city', None) # city
    categories_raw = data.get('categories', None) # catrgory
    # ---- VALUES ---- #
    business = 1 # business
    stars = data.get('stars', None) # stars
    reviews = data.get('review_count', None) # review number
    # avg rating = stars / review number

    # wheelchair
    try:
        wheelchair = (data.get("attributes").get("WheelchairAccessible") == "True")
    except:
        wheelchair = False
    
    # parking
    try:
        businessparking_raw = data.get("attributes").get("BusinessParking")
        parking_raw = json.loads(
            businessparking_raw.replace("'", '"') \
                .replace("True", "true") \
                .replace("False", "false") \
                .replace("None", "false")
        )
        parking = any([parking_raw.get("garage"), parking_raw.get("lot"), parking_raw.get("street")])
    except:
        parking = False


    if categories_raw:
        categories = categories_raw.split(', ')
        for c in categories:
            if reviews != None:
                cat_review_list.append(
                    ((city, c),
                    (business, stars, int(wheelchair), int(parking)))
                )
    else:
        if reviews != None:
            cat_review_list.append(
                ((city, "Unknown"),
                (business, stars, int(wheelchair), int(parking)))
            )
    
    return cat_review_list


cat_stars = input_file.map(lambda line: json.loads(line)) \
                      .flatMap(cat_reviews) \
                      .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3])) \
                      .map(lambda kv: (kv[0], (str(kv[1][0]), str(kv[1][1] / kv[1][0]), str(kv[1][2]), str(kv[1][3]))))

cat_stars_sorted = cat_stars.sortBy(lambda kv: (kv[0][0], 10e8-int(kv[1][0]), kv[0][1]), ascending = True) \
                            .map(lambda kv: '\t'.join(kv[0]) + '\t' + '\t'.join(kv[1]))

cat_stars_sorted.collect()
cat_stars_sorted.saveAsTextFile("total_reviews_per_category_output")
