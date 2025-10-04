from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("SentimentAnalysis") \
    .config("spark.jars", "/usr/local/spark/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("📖 Lecture des données...")
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/5SPAR") \
    .option("dbtable", "mastodon_stream") \
    .option("user", "myuser") \
    .option("password", "mysecretpassword") \
    .load()

print(f"Total toots: {df.count()}")

# Analyse de sentiment basique
positive_words = ["love", "great", "good", "happy", "excellent", "amazing", "wonderful"]
negative_words = ["hate", "bad", "terrible", "sad", "awful", "horrible", "worst"]

df_sentiment = df.withColumn(
    "sentiment",
    when(
        array_contains(split(lower(col("content")), " "), "love") | 
        array_contains(split(lower(col("content")), " "), "great") |
        array_contains(split(lower(col("content")), " "), "happy"), 
        "positive"
    )
    .when(
        array_contains(split(lower(col("content")), " "), "hate") | 
        array_contains(split(lower(col("content")), " "), "bad") |
        array_contains(split(lower(col("content")), " "), "terrible"), 
        "negative"
    )
    .otherwise("neutral")
)

print("💾 Sauvegarde dans PostgreSQL...")
df_sentiment.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/5SPAR") \
    .option("dbtable", "mastodon_sentiment") \
    .option("user", "myuser") \
    .option("password", "mysecretpassword") \
    .mode("overwrite") \
    .save()

# Statistiques
stats = df_sentiment.groupBy("sentiment").count()
print("📊 Distribution des sentiments:")
stats.show()

spark.stop()