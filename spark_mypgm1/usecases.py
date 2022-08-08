import logging
import pandas
import pandas as pd
import json
import os
from os import walk
import os.path

from os import path
import mysql.connector
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from multiprocessing.pool import *
import logging
import getpass

def calc(value1, value2):
    # value1 = input("Please enter first integer:\n")
    # value2 = input("Please enter second integer:\n")
    # calc(value1, value2)

    try:
        v1 = int(value1)
        v2 = int(value2)
        choice = input(
            "Enter 1 for addition.\nEnter 2 for subtraction.\nEnter 3 for Multiplication.\nEnter 4 for Division:\n")
        choice = int(choice)

        if choice == 1:
            num = v1 + v2
            print(f'You entered {v1} and {v2} and their addition is {num}')
        elif choice == 2:
            num = v1 - v2
            print(f'You entered {v1} and {v2} and their subtraction is {num}')
        elif choice == 3:
            num = v1 * v2
            print(f'You entered {v1} and {v2} and their multiplication is {num}')
        elif choice == 4:
            num = v1 / v2
            print(f'You entered {v1} and {v2} and their division is {num}')
        else:
            print("Wrong Choice, terminating the program.")
    except:
        print('Exception occurred')
    else:
        print('Calculator Program')
    finally:
        n = int(input("Enter a number: "))
        if (n % 2) == 0:
            print("{0} is Even".format(n))
        else:
            print("{0} is Odd".format(n))


def handle_dbldelmr(spark):
    #handle_dbldelmr(spark)

    # how to handle double delimiters in pyspark

    #read CSV file
    df = spark.read.csv("file:///home/hadoop/dbledmr.csv",header=True,sep='||')
    df.show()

def handle_muldelmr(spark):
    #handle_muldelmr(spark)

    # how to handle multiple delimiters in pyspark

    #read CSV file
    df = spark.read.csv("file:///home/hadoop/muledmr.csv",header=True,sep=',')
    df1 = df.withColumn("marks_split",split(col("marks"),"[|]"))\
            .withColumn("sub1",col("marks_split")[0])\
            .withColumn("sub2",col("marks_split")[1])\
            .withColumn("sub3",col("marks_split")[2])\
            .withColumn("sub4",col("marks_split")[3])\
            .drop("marks").drop("marks_split")
    df1.show()

def file_wordcount(sc, spark):
    # file_wordcount(sc,spark)
    
    data = [" Keep your face always toward the sunshine — and shadows will fall behind you — Walt Whitman "]
    # rdd = spark.sparkContext.parallelize(data)
    # for i in rdd.collect():
    #     print(i)
    # #flatmap
    # rdd2 = rdd.flatMap(lambda  x: x.split(" "))
    # for i in rdd2.collect():
    #     print(i)
    # #mapping 1
    # rdd3 = rdd2.map(lambda x: (x,1))
    # for i in rdd3.collect():
    #     print(i)
    # # reducebykey to count
    # rdd4 = rdd3.reduceByKey(lambda x,y : x+y)
    # for i in rdd4.collect():
    #     print(i)

    rdd = spark.sparkContext.parallelize(data).flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    for i in rdd.collect():
        print(i)

def Finddistance_crossjoin(spark):
    #Finddistance_crossjoin(spark)

    l = [[1,"Station1","4:20 AM"],[1,"Station2","5:30 AM"],[1,"Station3","7:30 AM"],
         [2,"Station2","5:50 AM"],[2,"Station4","7:30 AM"],[2,"Station5","11:30 AM"],[2,"Station6","1:30 PM"]]

    df = spark.createDataFrame(l,["BusID","Station","Time"])
    #print("Input Spark DataFrame:")
    #df.filter("BusID=1").show()
    #df.show()
    #df.join(df,how ='cross',on="BusID")
    windowSpec = Window.partitionBy("BusID").orderBy(to_timestamp("Time","hh:mm a").asc())
    df_wind = df.withColumn("row_num",row_number().over(windowSpec))
    #df2 = df_wind
    #df2.show()
    #spark.sql.analyzer.failAmbiguousSelfJoin = False
    df_out = df_wind.join(df_wind.alias("df2"),
                          (df_wind["row_num"]<col("df2.row_num"))\
             & (df_wind["BusID"]==col("df2.BusID")))\
        .select(df_wind["BusID"],df_wind["Station"].alias("Source_Point"),
                df_wind["Time"].alias("Source_Time"),
                col("df2.Station").alias("Destination_Point"),
                col("df2.Time").alias("Destination_Time"))
    df_out.show()
    print("output DataFrame:")

    df_final = df_out.withColumn("Travel_Time",(to_timestamp("Destination_Time",'hh:mm a').cast("long")-to_timestamp("Source_Time",'hh:mm a').cast("long"))/60).drop("Source_Time","Destination_Time").orderBy("Source_Point","Destination_Point")
    df_final.show()

def Find_minmax(spark):
    #Find_minmax(spark)

    df = spark.read.format("csv").option("header", "true") \
        .option("inferschema", "true") \
        .load("file:///home/hadoop/MinMax.csv")
    df.printSchema()
    df.show()
    df1 =df.withColumn("great",greatest("term1","term2","term3","term4"))\
            .withColumn("least",least("term1","term2","term3","term4"))
    df1.show()

def Format_DatetoTimeStamp(spark):
    #Format_DatetoTimeStamp(spark)

    df = spark.read.format("csv").option("header","true")\
         .option("inferschema","true")\
         .option("timestampFormat","M/d/yyy")\
         .load("file:///home/hadoop/DTtoTS.csv")
    df.printSchema()
    df.show()

def dateyytoyyyy(spark):
    #dateyytoyyyy(spark)

    # Must set this config to handle the date format
    # spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    df = spark.read.option("nullvalue","NULL").csv('file:///home/hadoop/yytoyyyy.csv',header=True,inferSchema=True)
    df.show()
    #df.printSchema()
    #print(df.select("HIREDATE").show())
    df1 = df.withColumn("HIREDATE",to_date("HIREDATE","dd-MM-yy"))
    df1.show()

def addingleadingzeros(spark):
    #addingleadingzeros(spark)

    l = [('Banu',30),('Ram',7),('Mano',65),('jamal',50),('geetha',100),('aarthi',45),('kiran',90),('Manju',34)]
    columns = ['Name', 'Score']
    df = spark.createDataFrame(data=l,schema = columns)
    df.show()
    #df.withColumnRenamed("Name", "NameoftheEmp").show()
    #df.printSchema()
    df = df.withColumn('Score_New',lpad(df.Score,3,'0')).show()

def format_string_addingleadingzeros(spark):
    #format_string_addingleadingzeros(spark)

    l = [('Banu', 30), ('Ram', 7), ('Mano', 65), ('jamal', 50), ('geetha', 100), ('aarthi', 45), ('kiran', 90),
         ('Manju', 34)]
    columns = ['Name', 'Score']
    df = spark.createDataFrame(data=l, schema=columns)
    df.show()
    df = df.withColumn("Score_000",format_string("%03d",col("Score")))
    df.show()
    df = df.withColumn("Name_Score",format_string("%s#%03d",col("Name"),col("Score")))
    df.show()

def ConcatSubstring_addingleadingzeros(spark):
    #ConcatSubstring_addingleadingzeros(spark)

    l = [('Banu', 30), ('Ram', 7), ('Mano', 65), ('jamal', 50), ('geetha', 100), ('aarthi', 45), ('kiran', 90),
         ('Manju', 34)]
    columns = ['Name', 'Score']
    df = spark.createDataFrame(data=l, schema=columns)
    df.show()
    df2 = df.withColumn("Score_000",concat(lit("00"),col("Score")))
    df2.show()
    df3 = df2.withColumn("Score_000",substring(col("Score_000"),-3,3))
    df3.show()
def flatten_Jsonfile(spark):
    #flatten_Jsonfile(spark)

    df = spark.read.format("csv").option("header", "true") \
        .option("inferschema", "true") \
        .load("file:///home/hadoop/codes/spark_mylearning/spark_mypgm1/Source files/Sampleflatten.csv")
    df.printSchema()
    df.show()
    df.select("*", json_tuple("request", "Response")).show()
    df.select("*", json_tuple("request", "Response")).drop("request") \
        .select("*", json_tuple("c0", "MessageId", "Latitude", "longitude") \
                .alias("MessageId", "Latitude", "longitude")).drop("c0").show()

def Split_column(spark):
    #Split_column(spark)

    df = spark.read.format("csv").option("header", "true") \
        .option("inferschema", "true") \
        .load("file:///home/hadoop/codes/spark_mylearning/spark_mypgm1/Source files/Splcol.csv")
    #df.printSchema()
    df.show()
    df1 = df.withColumn("NamArr", split(col("name"), "[,]")) \
            .withColumn("firstname", col("NamArr")[0]) \
            .withColumn("middlename", col("NamArr")[1]) \
            .withColumn("lastname", col("NamArr")[2]) \
            .drop("name").drop("NamArr")
    df1.show()
def DF_Empty(spark):
    #DF_Empty(spark)

    df = spark.read.format("csv").option("header", "true") \
        .option("inferschema", "true").option("sep","|")\
        .load("file:///home/hadoop/codes/spark_mylearning/spark_mypgm1/Source files/uspopulation.csv")
    #df.printSchema()
    df.show()
    groupdata = df.filter("state_code == 'NY'").groupby("city").sum("2019_estimate")
    groupdata.show()
    #IS_Empty_Group(groupdata)
    RDDIS_Empty(groupdata)

def RDDIS_Empty(groupdata):
    #RDDIS_Empty(groupdata)
    if groupdata.rdd.isEmpty():
        print("Dataframe is empty")
    else:
        print("Dataframe has values")

def IS_Empty_Group(groupdata):
    #IS_Empty_Group(groupdata)
    if groupdata.count() > 0:
        print("Dataframe has values")
    else:
        print("Dataframe is empty")

def Filter_Tempview(spark):
    #Filter_Tempview(spark)

    df = spark.read.format("csv").option("header", "true") \
        .option("inferschema", "true") \
        .load("file:///home/hadoop/MinMax.csv")
    df.show()
    df1 = df.filter("term4 < 50")
    df1.show()
    df.createOrReplaceTempView('filterview')
    df2 = spark.sql("select * from filterview where term1>50")
    df2.show()
