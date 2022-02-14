from pyspark.sql import SparkSession
import psycopg2
from hdfs import InsecureClient
import os
from pyspark.sql.types import StructType

pg_url = "jdbc:postgresql://postgres:5432/pagila"
pg_properties = {"user": "postgres", "password": "secret"}

def main():
  spark = SparkSession.builder\
    .master('spark://spark-master:7077')\
    .appName('homework')\
    .getOrCreate()

  actorSchema = StructType() \
    .add("actor_id", "integer")\
    .add("first_name", "string")\
    .add("last_name", "string")\
    .add("last_update", "string")

  actors = spark.read.csv("hdfs://hadoop-namenode:9000/bronze/actor.csv", schema=actorSchema)
  actors.tail(5)

def dump_tables():
  hdfs_url = 'http://hadoop-namenode:5070/'
  client = InsecureClient(hdfs_url, user='user')

  pg_creds = {
    'host': 'postgres'
    , 'port': '5432'
    , 'database': 'pagila'
    , 'user': 'postgres'
    , 'password': 'secret'
  }

  tables = [
    'actor',
    'address',
    'category',
    'city',
    'country',
    'customer',
    'film',
    'film_actor',
    'film_category',
    'inventory',
    'language',
    'payment',
    'rental',
    'staff',
    'store'
  ]

  with psycopg2.connect(**pg_creds) as pg_connection:
    cursor = pg_connection.cursor()
    for table in tables:
      with client.write(os.path.join('/bronze', f'{table}.csv'), overwrite=True) as csv_file:
        cursor.copy_expert(f"COPY (SELECT * FROM ONLY {table}) TO STDOUT WITH HEADER CSV", csv_file)


main()
