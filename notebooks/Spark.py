from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.config("spark.jars", "/opt/bitnami/spark/drivers/postgresql-42.5.3.jar").getOrCreate()
print(spark)


DfMatches=spark.read.csv("../data/matches.csv", header=True)


DfMatches.write.format("jdbc") \
        .option("url", "jdbc:postgresql://172.18.0.2:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "arsenalmatches") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()