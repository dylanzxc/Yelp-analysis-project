import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def get_business_schema():
    business_schema = types.StructType([
      types.StructField('business_id', types.StringType()),
      types.StructField('name', types.StringType()),
      types.StructField('address', types.StringType()),
      types.StructField('city', types.StringType()),
      types.StructField('state', types.StringType()),
      types.StructField('postal_code', types.StringType()),
      types.StructField('latitude', types.FloatType()),
      types.StructField('longitude', types.FloatType()),
      types.StructField('stars', types.FloatType()),
      types.StructField('review_count', types.IntegerType()),
      types.StructField('is_open', types.IntegerType()),
      types.StructField('attributes', types.MapType(types.StringType(), types.StringType())),
      types.StructField('categories', types.StringType()),
      types.StructField('hours', types.MapType(types.StringType(), types.StringType())),
    ])
    return business_schema

def get_user_schema():
    user_schema = types.StructType([
      types.StructField('user_id', types.StringType()),
      types.StructField('name', types.StringType()),
      types.StructField('review_count', types.IntegerType()),
      types.StructField('yelping_since', types.StringType()),
      types.StructField('friends', types.StringType()),
      types.StructField('useful', types.IntegerType()),
      types.StructField('funny', types.IntegerType()),
      types.StructField('cool', types.IntegerType()),
      types.StructField('fans', types.IntegerType()),
      types.StructField('elite', types.StringType()),
      types.StructField('average_stars', types.FloatType()),
      types.StructField('compliment_hot', types.IntegerType()),
      types.StructField('compliment_more', types.IntegerType()),
      types.StructField('compliment_profile', types.IntegerType()),
      types.StructField('compliment_cute', types.IntegerType()),
      types.StructField('compliment_list', types.IntegerType()),
      types.StructField('compliment_note', types.IntegerType()),
      types.StructField('compliment_plain', types.IntegerType()),
      types.StructField('compliment_cool', types.IntegerType()),
      types.StructField('compliment_funny', types.IntegerType()),
      types.StructField('compliment_writer', types.IntegerType()),
      types.StructField('compliment_photos', types.IntegerType()),
    ])
    return user_schema

def get_review_schema():
    review_schema = types.StructType([
      types.StructField('review_id', types.StringType()),
      types.StructField('user_id', types.StringType()),
      types.StructField('business_id', types.StringType()),
      types.StructField('stars', types.FloatType()),
      types.StructField('date', types.StringType()),
      types.StructField('text', types.StringType()),
      types.StructField('useful', types.IntegerType()),
      types.StructField('funny', types.IntegerType()),
      types.StructField('cool', types.IntegerType()),
    ])
    return review_schema

def get_checkin_schema():
    checkin_schema = types.StructType([
        types.StructField('business_id', types.StringType()),
        types.StructField('date', types.StringType())
    ])
    return checkin_schema

def get_popular_user_model_schema():
    popular_user_model_schema = types.StructType([
        types.StructField('feature', types.StringType()),
        types.StructField('weight', types.FloatType())
    ])
    return popular_user_model_schema

def get_popular_business_model_schema():
    popular_business_model_schema = types.StructType([
        types.StructField('feature', types.StringType()),
        types.StructField('weight', types.FloatType())
    ])
    return popular_business_model_schema