from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder \
    .appName("TrainSentimentModel") \
    .config("spark.jars", "/usr/local/spark/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

# ÉTAPE 1 : Données d'entraînement (exemples étiquetés)
# Tu peux utiliser le dataset Kaggle Sentiment140 ou créer tes propres exemples
training_data = spark.createDataFrame([
    ("I love this so much", "positive"),
    ("This is amazing", "positive"),
    ("Great work", "positive"),
    ("I hate this", "negative"),
    ("Terrible experience", "negative"),
    ("This is awful", "negative"),
    ("It's okay", "neutral"),
    ("Nothing special", "neutral")
], ["text", "label"])

# Convertir labels en nombres (requis par le modèle)
from pyspark.ml.feature import StringIndexer
label_indexer = StringIndexer(inputCol="label", outputCol="label_num")

# ÉTAPE 2 : Transformer texte en vecteurs (nombres)
tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
vectorizer = CountVectorizer(inputCol="filtered_words", outputCol="raw_features")
idf = IDF(inputCol="raw_features", outputCol="features")

# ÉTAPE 3 : Modèle de classification
lr = LogisticRegression(featuresCol="features", labelCol="label_num")

# ÉTAPE 4 : Pipeline (enchaîne toutes les étapes)
pipeline = Pipeline(stages=[
    label_indexer,
    tokenizer,
    remover,
    vectorizer,
    idf,
    lr
])

# ÉTAPE 5 : Diviser données (80% train, 20% test)
train, test = training_data.randomSplit([0.8, 0.2], seed=42)

# ÉTAPE 6 : Entraîner le modèle
print("Entraînement du modèle...")
model = pipeline.fit(train)

# ÉTAPE 7 : Tester la précision
predictions = model.transform(test)
evaluator = MulticlassClassificationEvaluator(
    labelCol="label_num",
    predictionCol="prediction",
    metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)
print(f"Précision du modèle : {accuracy * 100:.2f}%")

# ÉTAPE 8 : Appliquer sur tes toots Mastodon
print("Application sur les données Mastodon...")
mastodon_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/5SPAR") \
    .option("dbtable", "mastodon_stream") \
    .option("user", "myuser") \
    .option("password", "mysecretpassword") \
    .load()

# Renommer 'content' en 'text' pour le modèle
mastodon_df = mastodon_df.withColumnRenamed("content", "text")

# Prédire les sentiments
results = model.transform(mastodon_df)

# Convertir prédictions en labels (0→negative, 1→neutral, 2→positive)
from pyspark.sql.functions import when, col
final_results = results.withColumn(
    "sentiment",
    when(col("prediction") == 0, "negative")
    .when(col("prediction") == 1, "neutral")
    .otherwise("positive")
).select("id", "text", "account", "created_at", "sentiment")

# ÉTAPE 9 : Sauvegarder résultats
print("Sauvegarde dans PostgreSQL...")
final_results.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db:5432/5SPAR") \
    .option("dbtable", "mastodon_sentiment_ml") \
    .option("user", "myuser") \
    .option("password", "mysecretpassword") \
    .mode("overwrite") \
    .save()

print("Terminé!")
spark.stop()