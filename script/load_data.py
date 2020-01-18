import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import datetime
from pyspark.sql import SparkSession, functions, types

from init import spark
from schema import get_user_schema, get_review_schema, get_business_schema, get_checkin_schema
from utils import str_to_date, load_config

def load_user(inputs, keyspace):
    user_schema = get_user_schema()
    # load to spark dataframe
    user = spark.read.json(inputs + '/user.json', schema=user_schema)
    str_to_date_udf = functions.udf(str_to_date, types.DateType())
    user = user.withColumn('yelping_since_date', str_to_date_udf(user['yelping_since']))
    user = user.drop('yelping_since')
    user = user.withColumnRenamed('yelping_since_date', 'yelping_since')
    # write to cassandra
    user.write.format("org.apache.spark.sql.cassandra").options(table='user', keyspace=keyspace).save()

def load_review(inputs, keyspace):
    review_schema = get_review_schema()
    # load to spark dataframe
    review = spark.read.json(inputs + '/review.json', schema=review_schema)
    str_to_date_udf = functions.udf(str_to_date, types.DateType())
    review = review.withColumn('review_date', str_to_date_udf(review['date']))
    review = review.drop('date')
    # write to cassandra
    review.write.format("org.apache.spark.sql.cassandra").options(table='review', keyspace=keyspace).save()

def load_business(inputs, url):
    business_schema = get_business_schema()
    # load to spark dataframe
    business = spark.read.json(inputs + '/business.json', schema=business_schema)
    business = business.drop('attributes', 'categories', 'hours').cache()
    # write to cassandra
    business.write.format("org.apache.spark.sql.cassandra").options(table='business', keyspace=keyspace).save()

def load_checkin(inputs, keyspace):
    checkin_schema = get_checkin_schema()
    # load to spark dataframe
    checkin = spark.read.json(inputs + '/checkin.json', schema=checkin_schema)
    # write to cassandra
    checkin.write.format("org.apache.spark.sql.cassandra").options(table='checkin', keyspace=keyspace).save()

def main(inputs, keyspace):
    # load config
    load_config()
    # load data
    load_business(inputs, keyspace)
    load_user(inputs, keyspace)
    load_checkin(inputs, keyspace)
    load_review(inputs, keyspace)

if __name__ == '__main__':
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    main(inputs, keyspace)
