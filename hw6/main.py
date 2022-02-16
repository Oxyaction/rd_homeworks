from re import L
from pyspark.sql import SparkSession
import psycopg2
from hdfs import InsecureClient
import os
from pyspark.sql.types import StructType
import pyspark.sql.functions as F


pg_url = 'jdbc:postgresql://postgres:5432/pagila'
pg_properties = {'user': 'postgres', 'password': 'secret'}

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

schemas = {
  'payment': StructType() \
    .add('payment_id', 'string')\
    .add('customer_id', 'string')\
    .add('staff_id', 'string')\
    .add('rental_id', 'string')\
    .add('amount', 'float')\
    .add('payment_date', 'string')
}

spark = None
def get_spark():
  global spark
  if spark is None:
    spark = SparkSession.builder\
      .master('local[*]')\
      .appName('homework')\
      .getOrCreate()

  return spark

read_files = {}
def spark_read_file(file):
  if file not in read_files:
    spark = get_spark()
    schema = None
    if (file in schemas):
      schema = schemas[file]
    read_files[file] = spark.read.csv(f'hdfs://hadoop-namenode:9000/bronze/{file}.csv', header=True, schema=schema)
  
  return read_files[file]


def films_count_per_category():
  category_df = spark_read_file('category')
  film_category_df = spark_read_file('film_category')

  res = category_df\
    .join(film_category_df, 'category_id', 'left')\
    .groupBy(category_df.name)\
    .count()\
    .sort('count', ascending=False)

  res.show()

def ten_most_rented_actors():
  actor = spark_read_file('actor')
  film_actor = spark_read_file('film_actor')
  inventory = spark_read_file('inventory')
  rental = spark_read_file('rental')

  res = actor\
    .join(film_actor, 'actor_id')\
    .join(inventory, 'film_id')\
    .join(rental, 'inventory_id')\
    .groupBy(actor.actor_id)\
    .count()\
    .sort('count', ascending=False)\
    .limit(10)

  res = res\
    .join(actor, 'actor_id')\
    .select('first_name', 'last_name', 'count')\
    .sort('count', ascending=False)

  res.show()

# 3. вывести категорию фильмов, на которую потратили больше всего денег.
def most_expensive_category():
  film_category = spark_read_file('film_category')
  category = spark_read_file('category')
  inventory = spark_read_file('inventory')
  rental = spark_read_file('rental')
  payment = spark_read_file('payment')

  res = film_category\
    .join(category, 'category_id')\
    .join(inventory, 'film_id', 'left')\
    .join(rental, 'inventory_id', 'left')\
    .join(payment, 'rental_id', 'left')\
    .groupBy(category.category_id)\
    .agg(F.sum('amount').alias('total'))\
    .sort('total', ascending=False)\
    .limit(1)

  res = res.join(category, 'category_id')

  res.show()

# 4. вывести названия фильмов, которых нет в inventory
def not_inventorized_films():
  film = spark_read_file('film')
  inventory = spark_read_file('inventory')

  res = film\
    .join(inventory, 'film_id', 'leftanti')

  res.show()

# 5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех..
def top_3_children_actors():
  actor = spark_read_file('actor')
  film_actor = spark_read_file('film_actor')
  film_category = spark_read_file('film_category')
  category = spark_read_file('category')

  # getting number of films per actor
  res1 = actor\
    .join(film_actor, 'actor_id')\
    .join(film_category, 'film_id')\
    .join(category, 'category_id')\
    .where(category.name == 'Children')\
    .groupBy(actor.actor_id)\
    .count()\
    .sort('count', ascending=False)\

  # getting top 3 numbers
  res2 = res1\
    .select('count')\
    .distinct()\
    .sort('count', ascending=False)\
    .limit(3)

  # getting top 3 actors
  res = res1\
    .join(res2, 'count')\
    .join(actor, 'actor_id')\
    .sort('count', ascending=False)\

  res.show()


# 6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
def cities_active_inactive_clients():
  pass
  
# 7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
def category_most_rent():
  pass


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

  with psycopg2.connect(**pg_creds) as pg_connection:
    cursor = pg_connection.cursor()
    for table in tables:
      # filepath = os.path.join('/opt', 'bronze', f'{table}.csv')
      # with open(filepath, 'w') as file:
      #   cursor.copy_expert(f'COPY (SELECT * FROM ONLY {table}) TO STDOUT WITH HEADER CSV', file)
        # file.write(data)
      with client.write(os.path.join('/bronze', f'{table}.csv'), overwrite=True) as csv_file:
        cursor.copy_expert(f'COPY (SELECT * FROM {table}) TO STDOUT WITH HEADER CSV', csv_file)


def main():
  # films_count_per_category()
  # ten_most_rented_actors()
  # most_expensive_category()
  # not_inventorized_films()
  top_3_children_actors()


main()
