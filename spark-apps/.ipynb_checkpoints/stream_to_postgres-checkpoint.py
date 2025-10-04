from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# 1️⃣ Créer une session Spark
spark = SparkSession.builder \
    .appName("MastodonStreamProcessor") \
    .master("local[*]") \
    .getOrCreate()

# 2️⃣ Définir le schéma JSON des messages Mastodon
schema = StructType([
    StructField("id", StringType(), True),
    StructField("content", StringType(), True),
    StructField("account", StringType(), True),
    StructField("created_at", StringType(), True)
])

# 3️⃣ Lire le flux Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "mastodon_stream") \
    .option("startingOffsets", "latest") \
    .load()

# 4️⃣ Extraire la valeur JSON et la convertir en colonnes
json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5️⃣ Exemple : compter les toots par compte toutes les 1 minute
agg_df = json_df.groupBy("account").count()

# 6️⃣ Écrire les résultats dans la console pour test
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
