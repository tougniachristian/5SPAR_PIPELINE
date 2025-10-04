from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

spark = SparkSession.builder \
    .appName("MastodonToPostgres") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("id", StringType()),
    StructField("content", StringType()),
    StructField("account", StringType()),
    StructField("created_at", StringType())
])

print("📡 Connexion à Kafka...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "mastodon_stream") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

cleaned_df = json_df.select(
    col("id").cast("long").alias("id"),
    regexp_replace(col("content"), "<[^>]+>", "").alias("content"),
    col("account"),
    to_timestamp(col("created_at")).alias("created_at")
).filter(col("id").isNotNull())

print("✅ Pipeline configuré")

def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    count = batch_df.count()
    print(f"📝 Batch {batch_id}: {count} toots")
    
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/5SPAR") \
            .option("dbtable", "mastodon_stream") \
            .option("user", "myuser") \
            .option("password", "mysecretpassword") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(f"✅ {count} toots écrits dans PostgreSQL")
    except Exception as e:
        print(f"❌ Erreur: {e}")

print("🚀 Démarrage du streaming vers PostgreSQL...")

query = cleaned_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/checkpoint_pg") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✅ Stream actif - Ctrl+C pour arrêter")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Arrêt...")
    query.stop()
    spark.stop()