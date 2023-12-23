import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType

def create_spark_connection():
    conn = None
    try:
        conn = SparkSession.builder.appName("pyspark-notebook").\
            config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1").\
            config('spark.cassandra.connection.host', 'localhost'). \
            getOrCreate()

        conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
    return conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'weather') \
            .option('startingOffsets', 'earliest') \
            .load() \
            #.option("failOnDataLoss", "false")
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.weather_monitor (
        unique_id UUID PRIMARY KEY,
        city TEXT,
        temperature DECIMAL,
        humidity DECIMAL,
        weather TEXT,
        creation_time TEXT);
    """)

    print("Table created successfully!")

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("unique_id", StringType(), False),
        StructField("city", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("weather", StringType()),
        StructField("creation_time", StringType()),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)") \
    .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

def insert_data(session, **kwargs):
    print("inserting data...")
    unique_id = kwargs.get('unique_id')
    city = kwargs.get('city')
    temperature = kwargs.get('temperature')
    humidity = kwargs.get('humidity')
    weather = kwargs.get('weather')
    creation_time = kwargs.get('creation_time')
    
    try:
        session.execute("""
            INSERT INTO spark_streams.weather_monitor(unique_id, city, temperature, humidity, weather, creation_time)
            VALUES (%s, %s, %s, %s, %s,%s);
        """, (unique_id, city, temperature, humidity, weather,creation_time))
        logging.info(f"Data inserted successfully")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', "checkpoint/data")
                               .option('keyspace', 'spark_streams')
                               .option('table', 'weather_monitor')
                               .start())

            streaming_query.awaitTermination()