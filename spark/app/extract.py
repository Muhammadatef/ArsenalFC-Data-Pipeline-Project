from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    """Create and return a Spark session."""
    spark = SparkSession.builder.config("spark.jars", "/Drivers/SQL_Sever/jdbc/postgresql-42.7.3.jar").getOrCreate()
    return spark

def read_and_write_csv_to_postgres(spark, csv_path, db_table):
    """
    Read a CSV file into a DataFrame and write it to a PostgreSQL table.
    
    Parameters:
    - spark: The Spark session.
    - csv_path: Path to the CSV file.
    - db_table: Name of the database table where data will be written.
    """
    df = spark.read.csv(csv_path, header=True)
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", db_table) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()

def main():
    """Main function to execute the ETL process."""
    spark = create_spark_session()

    # Paths to the CSV files
    matches_csv_path = "../data/matches.csv"
    goalkeepers_csv_path = "../data/goalkeepers.csv"
    players_csv_path = "../data/players.csv"

    # Database table names
    matches_db_table = "arsenalmatches"
    goalkeepers_db_table = "arsenalGK"
    players_db_table = "arsenalPlayers"

    # Process each CSV file
    read_and_write_csv_to_postgres(spark, matches_csv_path, matches_db_table)
    read_and_write_csv_to_postgres(spark, goalkeepers_csv_path, goalkeepers_db_table)
    read_and_write_csv_to_postgres(spark, players_csv_path, players_db_table)

if __name__ == "__main__":
    main()
