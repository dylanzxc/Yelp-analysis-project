import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import datetime
from pyspark.sql import SparkSession, functions, types
from init import spark

def find_business_review_score(keyspace, positive_review_score):
    # read review from cassandra
    review = spark.read.format("org.apache.spark.sql.cassandra").options(table='review', keyspace=keyspace).load()
    review = review.select('review_id', 'user_id', 'business_id', 'stars')

    # read popular users from cassandra
    popular_user = spark.read.format("org.apache.spark.sql.cassandra").options(table='popular_user', keyspace=keyspace).load()

    # read popular business from cassandra
    popular_business = spark.read.format("org.apache.spark.sql.cassandra").options(table='popular_business', keyspace=keyspace).load()

    # we only want reviews from popular users
    review = review.join(popular_user, 'user_id')
    review = review.withColumnRenamed('score', 'user_score')

    # for each popular business, count review that has >= 3 star
    review = review.join(popular_business, 'business_id')
    review = review.withColumnRenamed('score', 'business_score')
    review = review.where(review['stars'] >= positive_review_score)
    business_positive_review = review.groupBy('business_id').count()
    popular_business_positive_review = business_positive_review.join(popular_business, 'business_id')

    # save business count score table to cassandra
    popular_business_positive_review.write.format("org.apache.spark.sql.cassandra").mode('overwrite').options(table='popular_business_positive_review', keyspace=keyspace).option('confirm.truncate', True).save()

    # output correlation coefficient
    corr = popular_business_positive_review.corr('count', 'score')
    print("\nThe correlation between business popularity score and positive review count from popular user is:  ", corr)

if __name__ == "__main__":
    keyspace = sys.argv[1]
    positive_review_score = sys.argv[2]
    find_business_review_score(keyspace, positive_review_score)
