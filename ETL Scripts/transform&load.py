from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, year, month, dayofmonth, dayofweek, quarter, expr, lit
from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, quarter

def create_spark_session():
    """
    Create and configure a Spark session.
    """
    spark = SparkSession.builder.config("spark.jars", "/Drivers/SQL_Sever/jdbc/postgresql-42.7.3.jar").getOrCreate()
    return spark

def load_data_from_postgres(spark, tbl_list):
    """
    Load data from PostgreSQL into Spark dataframes.
    
    Args:
    - spark: The Spark session object.
    - tbl_list: A list of table names to be loaded from PostgreSQL.
    
    Returns:
    A dictionary of Spark dataframes.
    """
    dataframe = {}
    for table in tbl_list:
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", f"{table}") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .load()
        dataframe[table] = df
    return dataframe

def transform_load_matches_data(spark, dataframe):
    """
    Perform transformations on the Arsenal matches data.
    
    Args:
    - spark: The Spark session object.
    - dataframe: The dictionary containing Spark dataframes.
    """
    Matches = dataframe['arsenalmatches']
    Matches.createOrReplaceTempView("Matches")
    
    # Execute SQL queries for various transformations
    # distinct_matches = spark.sql("""SELECT COUNT(DISTINCT Date) FROM Matches""")
    # matches_count = spark.sql(""""SELECT COUNT(Date) FROM Matches""")
    # dates = spark.sql("""SELECT MAX(Date) AS Max_date, MIN(Date) AS Min_Date FROM Matches""")
    DimMatch= Matches.withColumn("MatchID", monotonically_increasing_id())
    DimMatch = DimMatch.withColumn("FormattedDate", date_format(to_date("Date", "yyyy-M-d"), "yyyy-MM-dd"))

    ## Load DimMatch to DWH 
    DimMatch.write.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "dwh.DimArsenalMatches") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .mode("overwrite") \
    .save()

    return DimMatch

def transform_load_Players_data(spark, dataframe,DimMatch):

    Players = dataframe['arsenalPlayers']
    Players.createOrReplaceTempView("Players")

    spark.sql("""
    select concat(firstname, " ", lastname) as fullname
    from Players

    """)

    distnict_players= spark.sql("""
    select distinct concat(firstname, " ", lastname) as fullname
    from Players

    """)
    

    players_Dates= spark.sql("""
    select count(distinct Date) 
    from players

    """)
    

    distnict_players= distnict_players.withColumn("PlayerID", monotonically_increasing_id())

    Players= Players.withColumn('fullname', concat_ws(" ", col('FirstName'),col('LastName')))
    Players.select("fullname").show(5, False)

    DimPlayers= Players.join(distnict_players, on ='fullname', how="inner")
    DimPlayers.columns

    # For DimPlayers with original format M/d/yyyy
    DimPlayers = DimPlayers.withColumn("FormattedDate", date_format(to_date("Date", "M/d/yyyy"), "yyyy-MM-dd"))
    FactPlayers = DimMatch.join(DimPlayers, on='FormattedDate', how= 'left')


    FactPlayers = FactPlayers.drop('Date')

    # Register the DataFrame as a temporary view
    FactPlayers.createOrReplaceTempView("fact_players")

    # SQL query to select rows with any null values
    query = "SELECT * FROM fact_players WHERE " + ' OR '.join([f"{col} IS NULL" for col in FactPlayers.columns])

    # Execute the query
    rows_with_nulls_sql = spark.sql(query)

   

    FactPlayers = FactPlayers.drop('Season',
    'Tour',
    'Time',
    'Opponent',
    'HoAw',
    'Stadium','Coach',
    'Referee',
    'fullname',
    'LastName',
    'FirstName','Line')

    
    #Loading FactPlayers to DWH
    FactPlayers.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.FactArsenalPlayers") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()
    


    

    DimPlayers= DimPlayers.drop('Date',
    'Start',
    'Min',
    'G',
    'A',
    'PK',
    'PKA',
    'S',
    'SoT',
    'YK',
    'RK',
    'Touches',
    'Tackles',
    'Ints',
    'Blocks',
    'xG',
    'npxG',
    'xAG',
    'Passes',
    'PassesA',
    'PrgPas',
    'Carries',
    'PrgCar',
    'Line',
    'C','FormattedDate', 'Pos')

    ## Loading DimPlayers to DWH 
    DimPlayers.write.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "dwh.DimArsenalPlayers") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .mode("overwrite") \
    .save()

    return DimPlayers,FactPlayers

def transform_GK_data(spark, dataframe):

    GoalKeepers = dataframe['arsenalGK']
    GoalKeepers.createOrReplaceTempView("GK")

    GoalKeepers= GoalKeepers.withColumn('fullname', concat_ws(" ", col('FirstName'),col('LastName')))
    # GoalKeepers.select("fullname").show(5, False)


    GoalKeepers_f = spark.sql("""
        select distinct concat(firstname, " ", lastname) as fullname
        from GK

    """)


    GK_ = spark.sql ("""
    select Count(fullname) from GK
    """)



    GoalKeepers_f= GoalKeepers_f.withColumn('GkID',monotonically_increasing_id()+1)


    DimGoalKeepers= GoalKeepers.join(GoalKeepers_f, on ='fullname', how="inner")

    

    DimGoalKeepers = DimGoalKeepers.withColumn("FormattedDate", date_format(to_date("Date", "M/d/yyyy"), "yyyy-MM-dd"))




    FactGk = DimMatch.join(DimGoalKeepers, on='FormattedDate', how='left')

    FactGk = FactGk.drop( 
    'Season',
    'Tour',
    'Time',
    'Opponent',
    'HoAw',
    'Stadium',
    'Coach',
    'Referee',
    'Pos',
    'fullname',
    'LastName',
    'FirstName','Date'
    
    )
    
    ## Loading FactGK to DWH
    FactGk.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.FactArsenalGoalKeepers") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()


    DimGoalKeepers.columns

    DimGoalKeepers= DimGoalKeepers.drop('Min','Start',
    'SoTA',
    'GA',
    'Saves',
    'PSxG',
    'PKatt',
    'PKA',
    'PKm',
    'PassAtt',
    'Throws',
    'AvgLen',
    'GKAtt',
    'GKAvgLen','Date','C','FormattedDate')


    DimGoalKeepers.createOrReplaceTempView("DimGoalKeepers")
    DimGoalKeepers = spark.sql(""" select distinct * from DimGoalKeepers""")
    DimGoalKeepers =DimGoalKeepers.dropDuplicates()

    ##Loading DimGK to DWH
    DimGoalKeepers.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.DimArsenalGoalKeepers") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()
    return DimGoalKeepers,FactGk


def create_and_load_dim_date(spark, date_df):
    """
    Create the dim_date DataFrame from a source DataFrame and load it into a PostgreSQL table.
    
    Args:
    - spark: The Spark session object.
    - source_df: The source Spark DataFrame with a 'Date' column.
    
    Returns:
    The dim_date DataFrame.
    """
    # Transform the source DataFrame to create the dim_date DataFrame
    date_df = spark.range(date_diff + 1).select(to_date(expr(f"date_add(to_date('{min_date}', 'yyyy-MM-dd'), cast(id as int))"), "yyyy-MM-dd").alias("Date"))
    
    dim_date_df = date_df.select(
        "Date",
        year("Date").alias("Year"),
        month("Date").alias("Month"),
        dayofmonth("Date").alias("Day"),
        dayofweek("Date").alias("Weekday"),
        quarter("Date").alias("Quarter")
    )
    
    # Load the dim_date DataFrame to the PostgreSQL database
    dim_date_df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.DimDate") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()
    
    # Return the transformed DataFrame
    return dim_date_df

# To use this function, call it with the Spark session and source DataFrame that contains the Date column.
# For example:
# spark = create_spark_session()
# source_df = spark.read...  # load your source data with a 'Date' column
# dim_date_df = create_and_load_dim_date(spark, source_df)

    

    # Additional transformations can be added here as needed

def create_and_load_dim_date(spark):
    """
    Create the dim_date DataFrame and load it into a PostgreSQL table.
    """
    # Example logic to calculate min_date and date_diff
    # You'll need to replace this with actual logic to determine these values
    min_date = '2017-08-11'
    max_date = '2023-02-25'
    date_diff = (to_date(lit(max_date), 'yyyy-MM-dd') - to_date(lit(min_date), 'yyyy-MM-dd')).days

    # Now create the date_df DataFrame
    date_df = spark.range(date_diff + 1).select(expr(f"date_add(to_date('{min_date}', 'yyyy-MM-dd'), id)").alias("Date"))
    
    # Create the dim_date DataFrame with additional date parts
    dim_date_df = date_df.select(
        "Date",
        year("Date").alias("Year"),
        month("Date").alias("Month"),
        dayofmonth("Date").alias("Day"),
        dayofweek("Date").alias("Weekday"),
        quarter("Date").alias("Quarter")
    )
    
    # Load the dim_date DataFrame to the PostgreSQL database
    dim_date_df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/arsenalfc") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dwh.DimDate") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()

# Adjust the main function accordingly
def main():
    """
    Main execution block.
    """
    spark = create_spark_session()
    
    tbl_list = ['arsenalmatches', 'arsenalPlayers', 'arsenalGK']
    dataframe = load_data_from_postgres(spark, tbl_list)
    
    transform_load_matches_data(spark, dataframe)
    transform_load_Players_data(spark, dataframe)
    transform_GK_data(spark, dataframe)
    create_and_load_dim_date(spark)  # Adjusted call without date_df

if __name__ == "__main__":
    main()
