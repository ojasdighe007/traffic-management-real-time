from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# Spark Session Config
spark = (
    SparkSession.builder
    .appName("TrafficBronzeStreaming")
    # cluster master
    .master("spark://spark-master:7077")
    # delta lake 
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Kafka Raw Stream 

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "traffic-data")
    .option("startingOffsets", "latest") # 3 main types: earliest, latest, ? Latest stops duplicate processing (starts from committed offsets)
    .load()
)

# Convert Binary to String 

json_stream = raw_stream.selectExpr(
    "CAST (value AS STRING) as raw_json",
    "timestamp as kafka_timestamp"# this is kafka timestamp, not event timestamp. Event timestamp is almost always inside the value JSON
)

# Flexible Schema to make sure the data type covers the ideal event case as well as the bad/corrupt event case. 
# Example - Speed should be an Integer type in ideal case, but bad events may send it in string type.

traffic_schema = StructType([
    StructField("vehicle_id",StringType()),
    StructField("road_id",StringType()),
    StructField("city_zone",StringType()),
    StructField("speed",StringType()),
    StructField("congestion_level",StringType()),
    StructField("weather",StringType()),
    StructField("event_time",StringType())
])

parsed = json_stream.withColumn(
    "data",
    from_json(col("raw_json"), traffic_schema)
)

flattened = parsed.select(
    "raw_json",
    "kafka_timestamp",
    "data.*"
)

# Bronze Delta Write

bronze_query = (
    flattened.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_bronze")
    .option("path","/opt/spark/warehouse/traffic_bronze")
    .start()
)

# If this line is not included, then the stream stops after processing once.
# Continue processing and don't stop until process terminated explicitly
spark.streams.awaitAnyTermination()