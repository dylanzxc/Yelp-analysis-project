import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import datetime
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, functions, types
from init import spark
from schema import get_popular_business_model_schema

def find_top_businesses(keyspace, top, model_file):
    # read business from cassandra
    business = spark.read.format("org.apache.spark.sql.cassandra").options(table='business', keyspace=keyspace).load()
    business = business.select('business_id', 'stars', 'review_count', 'is_open')
    # read review from cassandra
    review = spark.read.format("org.apache.spark.sql.cassandra").options(table='review', keyspace=keyspace).load()
    review = review.select('business_id', 'review_id', 'useful', 'funny', 'cool')
    # read checkin from cassandra
    checkin = spark.read.format("org.apache.spark.sql.cassandra").options(table='checkin', keyspace=keyspace).load()
    # read comment analysis from cassandra
    comment_analysis = spark.read.format("org.apache.spark.sql.cassandra").options(table='comment_analysis_full', keyspace=keyspace).load()
    
    # normalize business table metrics
    avg_stars, avg_review_count, avg_is_open = business.groupBy().avg('stars', 'review_count', 'is_open').collect()[0]
    std_stars, std_review_count, std_is_open = business.groupBy().agg(functions.stddev_pop('stars'), 
                                                                      functions.stddev_pop('review_count'), 
                                                                      functions.stddev_pop('is_open')).collect()[0]
    business = business.withColumn('avg_stars', functions.lit(avg_stars))
    business = business.withColumn('avg_review_count', functions.lit(avg_review_count))
    business = business.withColumn('avg_is_open', functions.lit(avg_is_open))
    business = business.withColumn('std_stars', functions.lit(std_stars))
    business = business.withColumn('std_review_count', functions.lit(std_review_count))
    business = business.withColumn('std_is_open', functions.lit(std_is_open))
    business = business.select(business['business_id'], 
                               ((business['stars'] - business['avg_stars']) / business['std_stars']).alias('stars'),
                               ((business['review_count'] - business['avg_review_count']) / business['std_review_count']).alias('review_count'),
                               ((business['is_open'] - business['avg_is_open']) / business['std_is_open']).alias('is_open'))

    # summarize comment_analysis
    comment_analysis = comment_analysis.replace('Positive', '1')
    comment_analysis = comment_analysis.replace('Negative', '-1')
    comment_analysis = comment_analysis.replace('Neutral', '0')
    comment_analysis = comment_analysis.select(comment_analysis['review_id'], comment_analysis['result'].cast('int'))
    comment_analysis = comment_analysis.withColumnRenamed('result', 'sentiment')
    review = review.join(comment_analysis, 'review_id')

    # normalize review table metrics
    review = review.groupBy('business_id').agg(functions.sum('useful').alias('useful'),
                                               functions.sum('funny').alias('funny'),
                                               functions.sum('cool').alias('cool'),
                                               functions.sum('sentiment').alias('sentiment'))
    avg_useful, avg_funny, avg_cool, avg_sentiment = review.groupBy().avg('useful', 'funny', 'cool', 'sentiment').collect()[0]
    std_useful, std_funny, std_cool, std_sentiment = review.groupBy().agg(functions.stddev_pop('useful'),
                                                                          functions.stddev_pop('funny'),
                                                                          functions.stddev_pop('cool'),
                                                                          functions.stddev_pop('sentiment')).collect()[0]
    review = review.withColumn('avg_useful', functions.lit(avg_useful))
    review = review.withColumn('avg_funny', functions.lit(avg_funny))
    review = review.withColumn('avg_cool', functions.lit(avg_cool))
    review = review.withColumn('avg_sentiment', functions.lit(avg_sentiment))
    review = review.withColumn('std_useful', functions.lit(std_useful))
    review = review.withColumn('std_funny', functions.lit(std_funny))
    review = review.withColumn('std_cool', functions.lit(std_cool))
    review = review.withColumn('std_sentiment', functions.lit(std_sentiment))
    review = review.select(review['business_id'],
                           ((review['useful'] - review['avg_useful']) / review['std_useful']).alias('useful'),
                           ((review['funny'] - review['avg_funny']) / review['std_funny']).alias('funny'),
                           ((review['cool'] - review['avg_cool']) / review['std_cool']).alias('cool'),
                           ((review['sentiment'] - review['avg_sentiment']) / review['std_sentiment']).alias('sentiment'))

    # summarize checkin count
    checkin = checkin.select('business_id', functions.size(functions.split(checkin['date'], ', ')).alias('date'))
    avg_date = checkin.groupBy().avg('date').collect()[0][0]
    std_date = checkin.groupBy().agg(functions.stddev_pop('date')).collect()[0][0]
    checkin = checkin.withColumn('avg_date', functions.lit(avg_date))
    checkin = checkin.withColumn('std_date', functions.lit(std_date))
    checkin = checkin.select(checkin['business_id'], ((checkin['date'] - checkin['avg_date']) / checkin['std_date']).alias('date'))

    # join for result score
    business = business.join(review, 'business_id', 'left')
    business = business.join(checkin, 'business_id', 'left')
    business = business.fillna(-2)

    popular_business_model_schema = get_popular_business_model_schema()
    popular_business_model = spark.read.csv(model_file, schema=popular_business_model_schema)
    weights = {}
    rows = popular_business_model.collect()
    for row in rows[1:]:
        weights[row['feature']] = row['weight']

    business = business.select('business_id', (weights['stars'] * business['stars'] + weights['review_count'] * business['review_count'] + weights['is_open'] * business['is_open'] + 
                                               weights['useful'] * business['useful'] + weights['funny'] * business['funny'] + weights['cool'] * business['cool'] + 
                                               weights['sentiment'] * business['sentiment'] + 
                                               weights['date'] * business['date']).alias('score'))

    # sort and save popular user data to cassandra
    business = business.sort('score', ascending=False)
    top = round(business.count() * int(top) / 100)
    #business.createTempView('business')
    #business = spark.sql("SELECT * FROM business LIMIT {}".format(top))
    business = business.limit(top)
    business.write.format("org.apache.spark.sql.cassandra").mode('overwrite').options(table='popular_business', keyspace=keyspace).option('confirm.truncate', True).save()

if __name__ == "__main__":
    keyspace = sys.argv[1]
    top = sys.argv[2]
    model_file = sys.argv[3]
    find_top_businesses(keyspace, top, model_file)
