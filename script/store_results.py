import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import datetime
from pyspark.sql import SparkSession, functions, types
from init import spark
from schema import get_user_schema, get_review_schema, get_business_schema
from utils import str_to_date, load_config

def store_all_users(keyspace, url):
    user_schema = get_user_schema()
    # load to spark dataframe
    user = spark.read.json(inputs + '/user.json', schema=user_schema)
    str_to_date_udf = functions.udf(str_to_date, types.DateType())
    user = user.withColumn('yelping_since_date', str_to_date_udf(user['yelping_since']))
    user = user.drop('yelping_since')
    user = user.withColumnRenamed('yelping_since_date', 'yelping_since')
    # write to postgres
    user.write.format("jdbc") \
      .mode('overwrite') \
      .option('confirm.truncate', True) \
      .option('url', url) \
      .option('dbtable', 'users') \
      .option('user', 'postgres') \
      .option('driver', 'org.postgresql.Driver') \
      .save()

def store_all_reviews(keyspace, url):
    # load to spark dataframe
    review = spark.read.format("org.apache.spark.sql.cassandra").options(table='review', keyspace=keyspace).load()
    str_to_date_udf = functions.udf(str_to_date, types.DateType())
    review = review.withColumn('review_date', str_to_date_udf(review['date']))
    review = review.drop('date')
    # write to postgres
    review.write.format("jdbc") \
      .mode('overwrite') \
      .option('confirm.truncate', True) \
      .option('url', url) \
      .option('dbtable', 'review') \
      .option('user', 'postgres') \
      .option('driver', 'org.postgresql.Driver') \
      .save()

def store_all_businesses(keyspace, url):
    business = spark.read.format("org.apache.spark.sql.cassandra").options(table='business', keyspace=keyspace).load()
    # load to spark dataframe
    business = business.drop('attributes', 'categories', 'hours')

    # write to postgres
    business.write.format("jdbc") \
      .mode('overwrite') \
      .option('confirm.truncate', True) \
      .option('url', url) \
      .option('dbtable', 'business') \
      .option('user', 'postgres') \
      .option('driver', 'org.postgresql.Driver') \
      .save()

def store_popular_businesses(keyspace, url):
    popular_business = spark.read.format("org.apache.spark.sql.cassandra").options(table='popular_business', keyspace=keyspace).load()
    # load to spark dataframe
    business = business.drop('attributes', 'categories', 'hours')

    # write to postgres
    business.write.format("jdbc") \
      .mode('overwrite') \
      .option('confirm.truncate', True) \
      .option('url', url) \
      .option('dbtable', 'business') \
      .option('user', 'postgres') \
      .option('driver', 'org.postgresql.Driver') \
      .save()      

def str_to_list(a_str):
    if a_str is None:
        return []
    return [v.strip() for v in a_str.split(',')]

def store_all_business_categories(keyspace, url):
    # load to spark dataframe
    business = spark.read.format("org.apache.spark.sql.cassandra").options(table='business', keyspace=keyspace).load()
    str_to_list_udf = functions.udf(str_to_list, types.ArrayType(types.StringType()))
    business = business.select(business['business_id'], functions.explode(str_to_list_udf(business['categories'])).alias('category'))

    # write to postgres
    business.write.format("jdbc") \
      .mode('overwrite') \
      .option('confirm.truncate', True) \
      .option('url', url) \
      .option('dbtable', 'business_category') \
      .option('user', 'postgres') \
      .option('driver', 'org.postgresql.Driver') \
      .save()

def store_popular_business_positive_review(keyspace, url):
    # load to spark dataframe
    popular_business_positive_review = spark.read.format("org.apache.spark.sql.cassandra").options(table='popular_business_positive_review', keyspace=keyspace).load()
    business = spark.read.format("org.apache.spark.sql.cassandra").options(table='business', keyspace=keyspace).load()
    popular_business_positive_review = popular_business_positive_review.join(business, 'business_id')
    popular_business_positive_review = popular_business_positive_review.select('business_id', 'name', 'count', 'score')

    # write to postgres
    popular_business_positive_review.write.format("jdbc") \
      .mode('overwrite') \
      .option('confirm.truncate', True) \
      .option('url', url) \
      .option('dbtable', 'popular_business_positive_review') \
      .option('user', 'postgres') \
      .option('driver', 'org.postgresql.Driver') \
      .save()    

def store_business_review_sentiments(keyspace, url):
    comment_sentiments = spark.read.format("org.apache.spark.sql.cassandra").options(table='comment_analysis_full', keyspace=keyspace).load()
    business = spark.read.format("org.apache.spark.sql.cassandra").options(table='business', keyspace=keyspace).load()
    review = spark.read.format("org.apache.spark.sql.cassandra").options(table='review', keyspace=keyspace).load()
    comment_sentiments = comment_sentiments.join(review, 'review_id')
    business = business.join(comment_sentiments, 'business_id')
    business_review_sentiments = business.select('business_id', 'result')
    business_review_sentiments = business_review_sentiments.groupBy('business_id', 'result').count()
    business_review_sentiments = business_review_sentiments.sort('count', ascending = False)
    business_review_sentiments.show()
    
    # write to postgres
    business_review_sentiments.write.format("jdbc") \
      .mode('overwrite') \
      .option('confirm.truncate', True) \
      .option('url', url) \
      .option('dbtable', 'business_review_sentiments') \
      .option('user', 'postgres') \
      .option('driver', 'org.postgresql.Driver') \
      .save()  

def main(keyspace, url, function_name):
    # load config
    load_config()
    # store results
    getattr(sys.modules[__name__], function_name)(keyspace, url)

if __name__ == '__main__':
    keyspace = sys.argv[1]
    url = sys.argv[2]
    function_name = sys.argv[3]
    main(keyspace, url, function_name)

