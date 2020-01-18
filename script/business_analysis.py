import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types, functions
from pyspark.sql.functions import split, explode, lower, trim

cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('visualize').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def main(keyspace, url):
    business = spark.read.format("org.apache.spark.sql.cassandra").options(table='business', keyspace=keyspace).load()
    popular_business = spark.read.format("org.apache.spark.sql.cassandra").options(table='popular_business', keyspace=keyspace).load()
    business_with_scores = business.join(popular_business, business['business_id'] == popular_business['business_id']).drop(business['business_id'])


    # CITY WITH MOST POPULAR BUSINESSES (AVG. POPULARITY SCORES OF BUSINESS' CONSIDERED)
    avg_score_business = business_with_scores.groupBy('city').avg('score')
    avg_score_business = avg_score_business.withColumnRenamed('avg(score)', 'avg_score')
    avg_score_business = avg_score_business.sort('avg_score', ascending = False)
    # business.show()
    # avg_score_business.show()

    # write to postgres
    avg_score_business.write.format("jdbc") \
      .mode('overwrite') \
      .option('confirm.truncate', True) \
      .option('url', url) \
      .option('dbtable', 'avg_score_business') \
      .option('user', 'postgres') \
      .option('driver', 'org.postgresql.Driver') \
      .save()

    

    # AVERAGE STARS FOR EACH STATE
    avg_stars = business.groupBy('state').avg('stars')
    avg_stars = avg_stars.withColumnRenamed('avg(stars)', 'avg_stars')
    avg_stars = avg_stars.sort('avg_stars', ascending = False)
    # avg_stars.show()

    # write to postgres
    avg_stars.write.format("jdbc") \
      .mode('overwrite') \
      .option('confirm.truncate', True) \
      .option('url', url) \
      .option('dbtable', 'avg_stars') \
      .option('user', 'postgres') \
      .option('driver', 'org.postgresql.Driver') \
      .save()



    # CITY WITH MOST NUMBER OF POPULAR BUSINESSES
    count_business = business_with_scores.groupBy('city').count()
    count_business = count_business.sort('count', ascending = False)
    # count_business.show()

    # write to postgres
    count_business.write.format("jdbc") \
      .mode('overwrite') \
      .option('confirm.truncate', True) \
      .option('url', url) \
      .option('dbtable', 'count_business') \
      .option('user', 'postgres') \
      .option('driver', 'org.postgresql.Driver') \
      .save()



    # WHICH IS THE MOST COMMON CATEGORY AMONG THE POPULAR BUSINESSES?
    business_categories = business_with_scores.select('business_id', 'categories')
    business_categories = business_categories.withColumn('categories', explode(split(business_categories['categories'] , ',')))
    business_categories = business_categories.withColumn('categories', trim(business_categories['categories']))
    business_categories = business_categories.groupBy('categories').count()
    business_categories = business_categories.sort('count', ascending = False)
    # business_categories.show()

    # write to postgres
    business_categories.write.format("jdbc") \
      .mode('overwrite') \
      .option('confirm.truncate', True) \
      .option('url', url) \
      .option('dbtable', 'business_categories') \
      .option('user', 'postgres') \
      .option('driver', 'org.postgresql.Driver') \
      .save()


if __name__ == "__main__":
    keyspace = sys.argv[1]
    url = sys.argv[2]
    main(keyspace, url)