
import os
import streamlit as stl
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"
os.environ["PATH"] = "/usr/lib/jvm/java-21-openjdk-amd64/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from test.py_test import validate_streamlit_input

# Configuration environnement
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"

@stl.cache_resource
def load_all():
    spark = (
        SparkSession
        .builder
        .appName("StreamlitApp")
        .master("local[*]")
        .getOrCreate()
    )
    # On charge le PIPELINE complet
    model = PipelineModel.load("/home/root/pipeline_model")
    return spark, model


spark, model = load_all()

stl.title("🚖 Taxi NYC Predictor")

# Formulaire simple (colonnes brutes)
p_count = stl.number_input(
    "Passagers",
    1, 6, 1)
dist = stl.number_input(
    "Distance",
    0.1, 100.0, 2.5)
hour = stl.slider(
    "Heure",
    0, 23, 12)
day = stl.slider(
    "Jour (1=Dim, 7=Sam)",
    1, 7, 1)
vendor = stl.slider(
    "1= Creative Mobile Technologies, LLC; 2= VeriFone Inc",
    1, 2, 1)

if stl.button("Prédire"):
    input_df = spark.createDataFrame([{
        "passenger_count": float(p_count),
        "trip_distance": float(dist),
        "pickup_hour": int(hour),
        "pickup_dayofweek": int(day),
        "VendorID": int(vendor),
        "RatecodeID": "1",
        "payment_type": "1",
        "PULocationID": "138",
        "DOLocationID": "237",
        "congestion_surcharge": 0.0,
        "Airport_fee": 0.0,
        "cbd_congestion_fee": 0.0
    }])
    validate_streamlit_input(input_df)
    # Magie : le pipeline transforme et prédit d'un coup
    prediction = model.transform(input_df).collect()[0]["prediction"]
    stl.success(f"Estimation : ${prediction:.2f}")
