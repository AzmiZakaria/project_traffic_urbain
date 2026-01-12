from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    spark = SparkSession.builder \
        .appName("SmartCityTrafficAnalysis") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Schéma
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

    # Lecture HDFS
    print("Lecture HDFS...")
    try:
        df_raw = spark.read.schema(schema).json("hdfs://namenode:9000/data/raw/traffic/*.json")
    except:
        print("⚠️ Pas de données.")
        return

    if df_raw.count() == 0: return

    # Agrégations
    df_stats = df_raw.groupBy("zone").agg(
        avg("average_speed").alias("avg_speed"),
        avg("occupancy_rate").alias("avg_occupancy"),
        count("event_id").alias("total_events")
    ).withColumn("processing_time", current_timestamp())
    
    df_stats.show()

    # 1. Sauvegarde HDFS (Data Lake)
    print("💾 Ecriture HDFS (Parquet)...")
    df_stats.write.mode("overwrite").parquet("hdfs://namenode:9000/data/analytics/traffic_stats")

    # 2. Sauvegarde PostgreSQL (Pour Grafana) - NOUVEAU BLOC
    print("💾 Ecriture PostgreSQL...")
    
    # Configuration JDBC (Utilise les infos de ton docker-compose)
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    jdbc_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    # On écrit dans la table 'traffic_analytics'
    # mode 'append' pour garder l'historique des calculs
    df_stats.write \
        .mode("append") \
        .jdbc(url=jdbc_url, table="traffic_analytics", properties=jdbc_properties)

    print("✅ Terminé.")
    spark.stop()

if __name__ == "__main__":
    main()