import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pyspark.sql.types as T
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline
import math
from datetime import datetime
import pandas as pd


spark = SparkSession.builder\
        .appName("BDT Project")\
        .config("spark.sql.catalogImplementation","hive")\
        .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .enableHiveSupport()\
        .getOrCreate()

print(spark.catalog.listDatabases())

print(spark.catalog.listTables("projectdb"))
# Reading the table of incidents
incidents = spark.read.format("avro").table('projectdb.incidents')
incidents.createOrReplaceTempView('incidents')

features = ['inc_time', 'inc_weekday', 'rep_type_code', 'inc_code', 'sup_distr_2012', 'neighbourhoods', 'cnn', 'csd', 'cpd']
# Text features - 'inc_weekday', 'rep_type_code'. Decided to not mess with rep_datetime, as it does not convert to unix timestamp for some reason
# You can check for the reasoning why this subset of data was chosen in: https://docs.google.com/spreadsheets/d/1SZ8XbjgMw4pZfQdgEO7VI2-hAMVjmwlGHIP0oLKaQ_E/edit?usp=sharing
target = ['longitude', 'latitude']
# Removing everything else
incidents = incidents[features + target]

# Helper functions to get the sin_cos_transformations done
def minutes_sin(x):
    if x:
        x = datetime.fromtimestamp(x) 
        return math.sin(2 * math.pi * x.minute / 60)
    else:
        return x

def minutes_cos(x):
    if x:
        x = datetime.fromtimestamp(x)
        return math.cos(2 * math.pi * x.minute / 60)
    else:
        return x

def hours_sin(x):
    if x:
        x = datetime.fromtimestamp(x) 
        return math.sin(2 * math.pi * x.hour / 24)
    else:
        return x

def hours_cos(x):
    if x:
        x = datetime.fromtimestamp(x)
        return math.cos(2 * math.pi * x.hour / 24)
    else:
        return x

udf_min_sin = F.udf(minutes_sin, T.FloatType())
udf_min_cos = F.udf(minutes_cos, T.FloatType())
udf_h_sin = F.udf(hours_sin, T.FloatType())
udf_h_cos = F.udf(hours_cos, T.FloatType())
# Handling the time data
incidents = incidents.withColumn("inc_time_t", F.unix_timestamp('inc_time', 'HH:mm'))
incidents = incidents.withColumn("inc_min_sin", udf_min_sin("inc_time_t"))
incidents = incidents.withColumn("inc_min_cos", udf_min_cos("inc_time_t"))
incidents = incidents.withColumn("inc_h_sin", udf_h_sin("inc_time_t"))
incidents = incidents.withColumn("inc_h_cos", udf_h_cos("inc_time_t"))
# Drop unused 'inc_time'
incidents = incidents.drop('inc_time')

categorical = ['inc_weekday', 'rep_type_code']
# Both of the features look like a good data for label encoding

indexers = [ StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c)).setHandleInvalid("skip") for c in categorical ]

pipeline = Pipeline(stages=indexers)
model = pipeline.fit(incidents)
data = model.transform(incidents)

# Removing categorical
for c in categorical:
    data = data.drop(c)

# Also removing nans, as there are not lot of them    
data = data.dropna()

features = ['inc_code', 'sup_distr_2012', 'neighbourhoods', 'cnn', 'csd', 'cpd', 'inc_time_t', 'inc_min_sin', 'inc_min_cos', 'inc_h_sin', 'inc_h_cos', 'inc_weekday_indexed', 'rep_type_code_indexed']
targets = ['longitude', 'latitude']
# Assembling features
feature_assembler = VectorAssembler(inputCols=features, outputCol='features')
target_assembler = VectorAssembler(inputCols=targets, outputCol='targets')
transformed = feature_assembler.transform(data).select('features', 'longitude', 'latitude')

(trainingData, testData) = transformed.randomSplit([0.7, 0.3], 42)
# LINEAR REGRESSOR
lr = LinearRegression(featuresCol='features', labelCol='longitude')

evaluator = RegressionEvaluator(labelCol="latitude", predictionCol="prediction", metricName="rmse")

paramGrid = ParamGridBuilder().addGrid(lr.elasticNetParam, [0, 0.5]).addGrid(lr.regParam, [0, 0.01]).build()

crossval = CrossValidator(estimator = lr, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds = 4, seed=42)

cvModel = crossval.fit(trainingData)

predictions = cvModel.transform(testData)

predictions.coalesce(1)
    .select("prediction",'longitude')
    .write
    .mode("overwrite")
    .format("csv")
    .option("sep", ",")
    .option("header","true")
    .csv("/project/output/lr_predictions.csv")

rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data LR = %g" % rmse)

# GBTRegressor
gbt = GBTRegressor(featuresCol="features", labelCol='latitude', maxIter=10, seed=42)

evaluator = RegressionEvaluator(labelCol="longitude", predictionCol="prediction", metricName="rmse")

paramGrid = ParamGridBuilder().addGrid(gbt.maxDepth, [2, 5]).addGrid(lr.lossType, ['squared', 'absolute']).build()

crossval = CrossValidator(estimator = gbt, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds = 4, seed=42)

cvModel = crossval.fit(trainingData)

predictions = cvModel.transform(testData)

predictions.coalesce(1)
    .select("prediction",'longitude')
    .write
    .mode("overwrite")
    .format("csv")
    .option("sep", ",")
    .option("header","true")
    .csv("/project/output/gbt_predictions.csv")

rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data GBT = %g" % rmse)
