
from datetime import datetime,timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date, max as spark_max,lit,round as spark_round

from postgress_db_connect import DB_CONFIG

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NuukaEnergy Analysis") \
    .config("spark.jars", "postgresql-42.7.4.jar") \
    .getOrCreate()

output_location ="file:///C:/temp/data/top_5_properties_energy_consumption.parquet"
#get last 7 days data
today = datetime.today()
get_last_7th_dt = timedelta(days=7)
query = f"""(SELECT reportinggroup, create_dttm, value, unit, location 
            FROM nuuka_energy_test.energy_usage WHERE create_dttm >= '{get_last_7th_dt}') AS subquery"""

# Read data from the PostgreSQL table
df = spark.read.jdbc(
    url=f"jdbc:postgresql://{DB_CONFIG.get('host')}:{DB_CONFIG.get('port')}/{DB_CONFIG.get('dbname')}",
    table="nuuka_energy_test.energy_usage",
    properties=DB_CONFIG
)


df = df.withColumn("create_date", to_date(col("create_dttm")))

# Get the latest 7-day average for each property
latest_avg_df = df.groupBy("location","reportinggroup").agg(
    avg("value").alias("latest_7_day_avg")
).orderBy(col('latest_7_day_avg').desc()).limit(5)

top_5_avg_df = latest_avg_df.withColumn('latest_7_day_avg',spark_round(col('latest_7_day_avg'),4))\
                         .withColumn('snapshot_date',lit(today))

# Save the filtered data to a Parquet file
top_5_avg_df.show()
top_5_avg_df.write.mode("append").parquet(output_location)


# Stop the Spark session
spark.stop()