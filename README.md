# Q6
# Nα υλοποιηθεί το Query 6 χρησιμοποιώντας τo DataFrame API. Εφαρμόστε οριζόντια και κάθετη κλιμάκωση (horizontal and vertical scaling) των πόρων που δεσμεύετε για την εκτέλεση χρησιμοποιώντας τα κατάλληλα spark configurations (spark.executor.instances, spark.executor.cores, spark.executor.memory). 
# Καλείστε να εκτελέσετε την υλοποίησή σας χρησιμοποιώντας συνολικούς πόρους 8 cores και 16GB μνήμης με τα παρακάτω configurations:
# ⦁	2 executors X 4 cores/8GB memory
# ⦁	4 executors X 2 cores/4GB memory
# ⦁	8 executors X 1 core/2 GB memory
# Σχολιάστε τα αποτελέσματα

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum

spark = SparkSession.builder \
    .appName("Parquet Analysis") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()
df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/yellow_tripdata_2024")
df_zone = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/taxi_zone_lookup")


df_pickup = df_zone.alias("pickup_zone")
df_dropoff = df_zone.alias("dropoff_zone")

df_joined = df \
    .join(df_pickup, df["PULocationID"] == col("pickup_zone.LocationID"), "inner")

df_result = df_joined.groupBy(col("pickup_zone.Borough").alias("Borough")).agg(
        sum("tip_amount").alias("tip_amount($)"),
        sum("fare_amount").alias("fare_amount($)"),
        sum("tolls_amount").alias("tools_amount($)"),
        sum("extra").alias("extra($)"),
        sum("mta_tax").alias("MTA Tax($)"),
        sum("congestion_surcharge").alias("congestion_surcharge($)"),
        sum("airport_fee").alias("airport_fee($)"),
        sum("total_amount").alias("total_amount($)")
).orderBy(col("total_amount($)").desc())


df_result.show()

################################################################

#ΧΡΟΝΟΣ ΥΛΟΠΟΙΗΣΗΣ = 1 , 14 ΔΕΥΤΕΡΟΛΕΠΤΑ

################################################################



from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum

spark = SparkSession.builder \
    .appName("Parquet Analysis") \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/yellow_tripdata_2024")
df_zone = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/taxi_zone_lookup")


df_pickup = df_zone.alias("pickup_zone")
df_dropoff = df_zone.alias("dropoff_zone")

df_joined = df \
    .join(df_pickup, df["PULocationID"] == col("pickup_zone.LocationID"), "inner")

df_result = df_joined.groupBy(col("pickup_zone.Borough").alias("Borough")).agg(
        sum("tip_amount").alias("tip_amount($)"),
        sum("fare_amount").alias("fare_amount($)"),
        sum("tolls_amount").alias("tools_amount($)"),
        sum("extra").alias("extra($)"),
        sum("mta_tax").alias("MTA Tax($)"),
        sum("congestion_surcharge").alias("congestion_surcharge($)"),
        sum("airport_fee").alias("airport_fee($)"),
        sum("total_amount").alias("total_amount($)")
).orderBy(col("total_amount($)").desc())


df_result.show()

################################################################

#ΧΡΟΝΟΣ ΥΛΟΠΟΙΗΣΗΣ = 0 , 24 ΔΕΥΤΕΡΟΛΕΠΤΑ

################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum

spark = SparkSession.builder \
    .appName("Parquet Analysis") \
    .config("spark.executor.instances", "8") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/yellow_tripdata_2024")
df_zone = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/taxi_zone_lookup")


df_pickup = df_zone.alias("pickup_zone")
df_dropoff = df_zone.alias("dropoff_zone")

df_joined = df \
    .join(df_pickup, df["PULocationID"] == col("pickup_zone.LocationID"), "inner")

df_result = df_joined.groupBy(col("pickup_zone.Borough").alias("Borough")).agg(
        sum("tip_amount").alias("tip_amount($)"),
        sum("fare_amount").alias("fare_amount($)"),
        sum("tolls_amount").alias("tools_amount($)"),
        sum("extra").alias("extra($)"),
        sum("mta_tax").alias("MTA Tax($)"),
        sum("congestion_surcharge").alias("congestion_surcharge($)"),
        sum("airport_fee").alias("airport_fee($)"),
        sum("total_amount").alias("total_amount($)")
).orderBy(col("total_amount($)").desc())


df_result.show()

################################################################

#ΧΡΟΝΟΣ ΥΛΟΠΟΙΗΣΗΣ = 0 , 21

################################################################


