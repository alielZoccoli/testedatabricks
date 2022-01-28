# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

spark = SparkSession.builder.appName('sparka').getOrCreate()

server_name = "jdbc:sqlserver://testekyros.database.windows.net:1433"
database_name = "Teste"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "t1"
username = "alielz"
password = "pass" # Please specify password here

schema = StructType([
    StructField('Teste123', StringType(), False),
    StructField('Date', DateType(), False),
    StructField('TempHighF', IntegerType(), False),
    StructField('TempLowF', IntegerType(), False)
])

data = [
    [ 'BLI', date(2021, 4, 3), 52, 43],
    [ 'BLI', date(2021, 4, 2), 50, 38],
    [ 'BLI', date(2021, 4, 1), 52, 41],
    [ 'PDX', date(2021, 4, 3), 64, 45],
    [ 'PDX', date(2021, 4, 2), 61, 41],
    [ 'PDX', date(2021, 4, 1), 66, 39],
    [ 'SEA', date(2021, 4, 3), 57, 43],
    [ 'SEA', date(2021, 4, 2), 54, 39],
    [ 'SEA', date(2021, 4, 1), 56, 41]
]

df = spark.createDataFrame(data, schema)

display(df)
