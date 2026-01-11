from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    # 1. Initialisation de la Session Spark
    spark = SparkSession.builder \
        .appName("SmartCityTrafficAnalysis") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") # Moins de logs inutiles

    # 2. Définition du Schéma (Indispensable pour du JSON)
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("road_id", StringType(), True),
        StructField("road_type", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("vehicle_count", IntegerType(), True),
        StructField("average_speed", IntegerType(), True),
        StructField("occupancy_rate", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])

    print("⚡ Lecture des données brutes depuis HDFS...")
    # On lit tous les fichiers JSON du dossier raw
    # path explicite avec le port HDFS interne
    input_path = "hdfs://namenode:9000/data/raw/traffic/*.json"
    
    try:
        df_raw = spark.read.schema(schema).json(input_path)
    except Exception as e:
        print(f"⚠️ Erreur de lecture (Dossier peut-être vide ?) : {e}")
        return

    if df_raw.count() == 0:
        print("⚠️ Aucune donnée à traiter.")
        return

    # 3. Traitement & Agrégations (KPIs)
    
    # KPI 1 : Vitesse moyenne et Trafic par Zone
    df_zone_stats = df_raw.groupBy("zone").agg(
        avg("average_speed").alias("avg_speed"),
        avg("occupancy_rate").alias("avg_occupancy"),
        count("event_id").alias("total_events")
    ).withColumn("processing_time", current_timestamp())

    print("\n--- RÉSULTATS PAR ZONE (Aperçu) ---")
    df_zone_stats.show()

    # KPI 2 : Détection des congestions (Vitesse < 30 km/h)
    df_congestion = df_raw.filter(col("average_speed") < 30) \
                          .select("zone", "road_id", "average_speed", "timestamp")

    # 4. Sauvegarde en format PARQUET (Analytics Zone)
    output_path = "hdfs://namenode:9000/data/analytics/traffic_stats"
    
    print(f"💾 Sauvegarde des résultats dans {output_path}...")
    
    # Mode 'overwrite' : On écrase l'analyse précédente (pour ce test)
    # En prod, on partitionnerait par date.
    df_zone_stats.write.mode("overwrite").parquet(output_path)
    
    print("✅ Traitement Spark terminé avec succès !")
    spark.stop()

if __name__ == "__main__":
    main()