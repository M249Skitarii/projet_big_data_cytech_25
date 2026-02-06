from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, col
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import os
from test.py_test import validate_nyc_taxi_df

# ---------------- Config MinIO ----------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "nyc-validated-b1"
PARQUET_PATH = "yellow_tripdata_2025-11-validated.parquet"

# ---------------- Spark Session ----------------
spark = (
    SparkSession.builder
    .appName("NYC Taxi Preprocessing")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false")
    .config("spark.pyspark.python",
            "/app/.venv/bin/python")
    .config("spark.pyspark.driver.python", "/app/.venv/bin/python")
    .getOrCreate()
)

# ---------------- Lecture + test unitaires Parquet ----------------
df = spark.read.parquet(f"s3a://{BUCKET}/{PARQUET_PATH}")
df = df.repartition(16)
validate_nyc_taxi_df(df)

# ---------------- Feature engineering ----------------
df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
       .withColumn("pickup_dayofweek", dayofweek(col("tpep_pickup_datetime")))
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
categorical_cols = [
    'RatecodeID',
    'payment_type',
    'PULocationID',
    'DOLocationID',
    'pickup_hour',
    'pickup_dayofweek'
    ]
numeric_cols = [
    'passenger_count',
    'trip_distance',
    'congestion_surcharge',
    'Airport_fee',
    'cbd_congestion_fee'
    ]
target_col = 'total_amount'

# ---------------- OneHot Encoding Spark ----------------
indexers = [
    StringIndexer(
        inputCol=c,
        outputCol=f"{c}_idx",
        handleInvalid="keep"
        ) for c in categorical_cols]
encoders = [
    OneHotEncoder(
        inputCol=f"{c}_idx",
        outputCol=f"{c}_ohe"
        ) for c in categorical_cols]

assembler = VectorAssembler(
    inputCols=numeric_cols + [f"{c}_ohe" for c in categorical_cols],
    outputCol="features"
)

gbt = GBTRegressor(
    featuresCol="features",
    labelCol=target_col,
    maxIter=100,
    maxDepth=6
    )

# ---------------- Pipeline complet ----------------
pipeline = Pipeline(stages=indexers + encoders + [assembler, gbt])
pipeline_model = pipeline.fit(train_df)
# ---------------- Évaluation ----------------
predictions = pipeline_model.transform(test_df)
evaluator = RegressionEvaluator(
    labelCol=target_col,
    predictionCol="prediction",
    metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"RMSE: {rmse:.2f}")

# ---------------- Sauvegarde du pipeline complet ----------------
pipeline_model_path = "/home/root/pipeline_model"
os.makedirs(os.path.dirname(pipeline_model_path), exist_ok=True)
pipeline_model.write().overwrite().save(pipeline_model_path)
print(f"Pipeline complet sauvegardé à {pipeline_model_path}")
