# This is a sample Python script.
import pandas
import pyspark
import os
from utils import *
from usecases import *
from SparkStreaming import *
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

def print_hi(name):

    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = SparkSession.builder.appName('input1').config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.23.jar").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    spark.conf.set("spark.sql.debug.maxToStringFields", 6000)
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin","False")

    print_hi('PyCharm')

    #df_mysql = readCSV(spark)
    #writeToMysql(df_mysql)
    sc.setLogLevel("INFO");
    #createschemadynamic(spark)
    #flatten_Jsonfile(spark)
    DF_Empty(spark)






# See PyCharm help at https://www.jetbrains.com/help/pycharm/
