"""
Test script
"""

import os
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("TestSparkJob") \
        .getOrCreate()

    # Your code can now use the spark session
    print("Hello, World!")
    print(os.getcwd())

    # Create a simple dataframe to verify Spark is working
    df = spark.createDataFrame([(1, "test"), (2, "example")], ["id", "value"])
    df.show()

    # Always stop the spark session when done
    spark.stop()

