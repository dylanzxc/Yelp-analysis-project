import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import csv
from pyspark.sql import SparkSession, functions, types
from init import spark

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

def main(keyspace, model_file, input_file):
    labeled_data_schema = types.StructType([
        types.StructField('name', types.StringType()),
        types.StructField('tripadvisor/google_ratings', types.FloatType()),
        types.StructField('tripadvisor/google_review_count', types.IntegerType()),
    ])
    labeled_data = spark.read.csv(input_file, schema=labeled_data_schema)
    labeled_data = labeled_data.withColumn('target_popularity', labeled_data['tripadvisor/google_ratings']*labeled_data['tripadvisor/google_review_count'])
    labeled_data = labeled_data.drop('tripadvisor/google_ratings', 'tripadvisor/google_review_count')
    business = spark.read.format("org.apache.spark.sql.cassandra").options(table='business', keyspace=keyspace).load()
    business = business.select('business_id', 'name', 'stars', 'review_count', 'is_open')   
    data = business.join(labeled_data, 'name')
    
    review = spark.read.format("org.apache.spark.sql.cassandra").options(table='review', keyspace=keyspace).load()
    review = review.select('business_id', 'useful', 'funny', 'cool')
    review = review.groupBy('business_id').avg('useful','funny', 'cool')
    
    checkin = spark.read.format("org.apache.spark.sql.cassandra").options(table='checkin', keyspace=keyspace).load()
    checkin = checkin.select('business_id', functions.size(functions.split(checkin['date'], ', ')).alias('date'))
    avg_date = checkin.groupBy().avg('date').collect()[0][0]
    std_date = checkin.groupBy().agg(functions.stddev_pop('date')).collect()[0][0]
    checkin = checkin.withColumn('avg_date', functions.lit(avg_date))
    checkin = checkin.withColumn('std_date', functions.lit(std_date))
    checkin = checkin.select(checkin['business_id'], ((checkin['date'] - checkin['avg_date']) / checkin['std_date']).alias('date'))

    data = data.join(review, 'business_id')
    data = data.join(checkin, 'business_id', 'left')
    train, test = data.randomSplit([0.9, 0.1])
    data = data.cache()
    test = test.cache()

    assemble_features = VectorAssembler(inputCols=['stars','review_count','is_open', 'avg(useful)', 'avg(funny)', 'avg(cool)'], outputCol='features')
    # classifier = GBTRegressor(featuresCol='features', labelCol='target_popularity')
    classifier = LinearRegression(featuresCol='features', labelCol='target_popularity', maxIter=100, solver="l-bfgs")
    pipeline = Pipeline(stages=[assemble_features, classifier])
    #regParam=lambda elasticNetParam=lusso or rigid regularization
    paramGrid = ParamGridBuilder().addGrid(classifier.regParam, [0.1, 0.01]).addGrid(classifier.elasticNetParam, [0,1.0]).build()
    modelEvaluator=RegressionEvaluator(predictionCol='prediction', labelCol='target_popularity', metricName='rmse')
    crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=modelEvaluator,
                          numFolds=10)
    cvModel = crossval.fit(data)
#    predictions.show()

    predictions = cvModel.transform(test)
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='target_popularity', metricName='r2')
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='target_popularity', metricName='rmse')
    r2 = r2_evaluator.evaluate(predictions)
    rmse = rmse_evaluator.evaluate(predictions)
    print('r-square for model: %g' % (r2, ))
    print('root mean square error for model: %g' % (rmse, ))
    pip_model = cvModel.bestModel
    # pip_model.write().overwrite().save(model_file)

    features = ['stars', 'review_count', 'is_open', 'useful', 'funny', 'cool', 'date']
    with open(model_file, mode='w') as model_f:
        model_writer = csv.writer(model_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        model_writer.writerow(['feature', 'weight'])
        for i in range(len(pip_model.coefficients)):
            coefficient = pip_model.coefficients[i]
            model_writer.writerow([features[i], coefficient])


if __name__ == '__main__':
    keyspace = sys.argv[1]
    model_file = sys.argv[2]
    input_file = sys.argv[3]
    main(keyspace, model_file, input_file)
