import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import datetime
from pyspark.sql import SparkSession, functions, types
from init import spark
from schema import get_popular_user_model_schema

@functions.udf(returnType = types.IntegerType())
def num_elements(elite):
    if elite:
        return len(elite)
    return 0

def find_top_users(keyspace, top, model_file):
    # read user from cassandra
    user = spark.read.format("org.apache.spark.sql.cassandra").options(table='user', keyspace=keyspace).load()
    user = user.select([c for c in user.columns if c not in {'name', 'yelping_since', 'useful', 'funny', 'cool', 'average_stars'}])

    # combine all compliment scores into one
    user = user.withColumn('compliment', sum(user[col] for col in ['compliment_cool', 'compliment_cute', 'compliment_funny', 
                                                                'compliment_hot', 'compliment_list', 'compliment_more', 
                                                                'compliment_note', 'compliment_photos', 'compliment_plain', 
                                                                'compliment_profile', 'compliment_writer']))

    user = user.select('user_id', 'compliment', 'elite', 'fans', 'friends', 'review_count')

    # replace elite and friends with no. of years elite and no. of friends
    user = user.withColumn('elite', num_elements(user['elite'])).drop(user['elite'])
    user = user.withColumn('friends', num_elements(user['friends'])).drop(user['friends'])

    # normalize user table metrics
    avg_compliment, avg_elite, avg_fans, avg_friends, avg_review_count = user.groupBy() \
                                                    .avg('compliment', 'elite', 'fans', 'friends', 'review_count').collect()[0]

    std_compliment, std_elite, std_fans, std_friends, std_review_count = user.groupBy() \
                                                    .agg(functions.stddev_pop('compliment'),
                                                         functions.stddev_pop('elite'),
                                                         functions.stddev_pop('fans'),
                                                         functions.stddev_pop('friends'),
                                                         functions.stddev_pop('review_count')).collect()[0]

    user = user.withColumn('avg_compliment', functions.lit(avg_compliment))
    user = user.withColumn('avg_elite', functions.lit(avg_elite))
    user = user.withColumn('avg_fans', functions.lit(avg_fans))
    user = user.withColumn('avg_friends', functions.lit(avg_friends))
    user = user.withColumn('avg_review_count', functions.lit(avg_review_count))

    user = user.withColumn('std_compliment', functions.lit(std_compliment))
    user = user.withColumn('std_elite', functions.lit(std_elite))
    user = user.withColumn('std_fans', functions.lit(std_fans))
    user = user.withColumn('std_friends', functions.lit(std_friends))
    user = user.withColumn('std_review_count', functions.lit(std_review_count))

    user = user.select(user['user_id'],
                       ((user['compliment'] - user['avg_compliment']) / user['std_compliment']).alias('compliment'),
                       ((user['elite'] - user['avg_elite']) / user['std_elite']).alias('elite'),
                       ((user['fans'] - user['avg_fans']) / user['std_fans']).alias('fans'),
                       ((user['friends'] - user['avg_friends']) / user['std_friends']).alias('friends'),
                       ((user['review_count'] - user['avg_review_count']) / user['std_review_count']).alias('review_count'))
    
    user = user.fillna(-2)

    popular_user_model_schema = get_popular_user_model_schema()
    popular_user_model = spark.read.csv(model_file, schema=popular_user_model_schema)
    weights = {}
    rows = popular_user_model.collect()
    for row in rows[1:]:
        weights[row['feature']] = row['weight']

    user = user.select('user_id', (weights['compliment'] * user['compliment'] + weights['elite'] * user['elite'] + weights['fans'] * user['fans'] +
                                   weights['friends'] * user['friends'] + weights['review_count'] * user['review_count']).alias('score'))

    # sort and save popular user data to cassandra
    user = user.sort('score', ascending = False)
    user = user.limit(round(user.count() * int(top) / 100))
    user.write.format("org.apache.spark.sql.cassandra").mode('overwrite').option('confirm.truncate', True).options(table='popular_user', keyspace=keyspace).save()


if __name__ == "__main__":
    keyspace = sys.argv[1]
    top = sys.argv[2]
    model_file = sys.argv[3]
    find_top_users(keyspace, top, model_file)
