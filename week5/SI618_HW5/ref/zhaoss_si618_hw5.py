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

'''
def cat_reviews(data):
    cat_review_list = []
    reviews = data.get('review_count', None)
    categories_raw = data.get('categories', None)
    if categories_raw:
        categories = categories_raw.split(', ')
        for c in categories:
            if reviews != None:
                cat_review_list.append((c, reviews))
    return cat_review_list

    cat_stars = input_file.map(lambda line: json.loads(line)) \
                      .flatMap(cat_reviews) \
                      .reduceByKey(lambda x, y: x + y)
'''


def business(data):
    business_list = []
    city = data.get('city', None)
    stars = data.get('stars', None)
    categories_raw = data.get('categories', None)

    if categories_raw:
        categories = categories_raw.split(', ')
        for c in categories:
            if city != None and stars != None:
                try:
                    wheel_chair = data.get('attributes')['WheelchairAccessible']
                    if 'True' in wheel_chair:
                        business_list.append(((city, c), 1, stars, 1))
                    else:
                        business_list.append(((city, c), 1, stars, 0))
                except:
                    business_list.append(((city, c), 1, stars, 0))

    else:
        if city != None and stars != None:
            try:
                wheel_chair = data.get('attributes')['WheelchairAccessible']
                if 'True' in wheel_chair:
                    business_list.append(((city, 'Unknown'), 1, stars, 1))
                else:
                    business_list.append(((city, 'Unknown'), 1, stars, 0))
            except:
                business_list.append(((city, 'Unknown'), 1, stars, 0))

    return business_list

def parking(data):
    business_list = []
    city = data.get('city', None)
    categories_raw = data.get('categories', None)

    if categories_raw:
        categories = categories_raw.split(', ')
        for c in categories:
            if city != None:
                try:
                    parking = data.get('attributes')['BusinessParking']
                    if "'garage': True" in parking or "'street': True" in parking or "'lot': True" in parking:
                        business_list.append(((city, c), 1))
                    else:
                        business_list.append(((city, c), 0))
                except:
                    business_list.append(((city, c), 0))
    else:
        if city != None:
            try:
                parking = data.get('attributes')['BusinessParking']
                if "'garage': True" in parking or "'street': True" in parking or "'lot': True" in parking:
                    business_list.append(((city, 'Unknown'), 1))
                else:
                    business_list.append(((city, 'Unknown'), 0))
            except:
                business_list.append(((city, 'Unknown'), 0))

    return business_list

step1 = input_file.map(lambda line: json.loads(line)).flatMap(business)
step1_1 = step1.map(lambda x: (x[0], x[1])).reduceByKey(lambda a, b: a + b)
step1_2 = step1.map(lambda x: (x[0], x[2])).reduceByKey(lambda a, b: a + b)
step1_3 = step1.map(lambda x: (x[0], x[3])).reduceByKey(lambda a, b: a + b)

step2 = input_file.map(lambda line: json.loads(line)).flatMap(parking)
step2_1 = step2.map(lambda x: (x[0], x[1])).reduceByKey(lambda a, b: a + b)


final_step1 = step1_1.join(step1_2).join(step1_3).join(step2_1) \
    .map(lambda x: (x[0][0], x[0][1], x[1][0][0][0], x[1][0][0][1] / x[1][0][0][0], x[1][0][1], x[1][1])) \
    .sortBy(lambda x: x[1], ascending=True) \
    .sortBy(lambda x: x[2], ascending=False) \
    .sortBy(lambda x: x[0], ascending=True)

final_step1.map(lambda x: str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]) + "\t" + str(x[3]) + "\t" + str(x[4]) + "\t" + str(x[5])).saveAsTextFile("zhaoss_si618_hw5_output")

'''
step1.collect()
step1.saveAsTextFile("zhaoss_si618_hw5_output")
# final_step = final_step.map(lambda x: (x[0], x[1], x[2] / x[1], x[3]))
# final_step.saveAsTextFile("zhaoss_si618_hw5_output")

# cat_stars.collect()
# cat_stars.saveAsTextFile("total_reviews_per_category_output")

# step1_2.collect()
'''