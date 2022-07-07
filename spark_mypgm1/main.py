# This is a sample Python script.
import pandas
import pyspark
import os
from utils import *
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
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    print_hi('PyCharm')
    #exploreDir('/home/hadoop/Downloads/dataanalytics-main')
    #recursiveFileList("/home/hadoop/Downloads/dataanalytics-main")

    #recursiveDirList('/home/hadoop/Downloads/dataanalytics-main/Spark')
    #df_mysql = readCSV(spark)
    #writeToMysql(df_mysql)
    #sc.setLogLevel("INFO");

    # path_value = '/home/hadoop/Downloads/dataanalytics-main/Hive/sales/sales1.csv'
    # df = pandasreadcsv(path_value,spark)
    # #writetohdfs(df,spark)
    #hdfstohivetable(df,spark)
    createschemadynamic(spark)

    #dropindex(spark)
   #readTextFile(sc,spark)
   # readmysql(spark)
    #readJson()
# value = input("Please enter a string:\n")
# print(f'You entered {value}')
# value1 = input("Please enter first integer:\n")
# value2 = input("Please enter second integer:\n")
# calc(value1,value2)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
