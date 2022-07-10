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
import logging
import getpass


def readJson():

    #readJson()

    f = open('/home/hadoop/Downloads/dataanalytics-main/Spark/Emp/empdetail.json')
    data = json.load(f)
    for i in data['emp_details']:
        print(i)
    f.close()


def readCSV(spark):
    #df_mysql = readCSV(spark)

    df = spark.read.csv('file:///home/hadoop/Downloads/dataanalytics-main/Spark/Emp/emp1.csv', header=True, sep=':',
                        comment='@', quote='"', inferSchema='True', enforceSchema='False',
                        ignoreLeadingWhiteSpace='True',
                        ignoreTrailingWhiteSpace='True', dateFormat='mm-dd-yyyy').show()


def readTextFile(sc, spark):
    #readTextFile(sc, spark)

    try:
        rdd = sc.textFile('file:///home/hadoop/Downloads/dataanalytics-main/Hive/Emp/emp.txt')
        df2 = spark.read.csv(rdd).show()
    except Exception as e:
        #print('error occurred')
        print(e)


def exploreDir(inputPath):
    #exploreDir('/home/hadoop/Downloads/dataanalytics-main')

    list = os.listdir(inputPath)
    print(list)


def walk_error_handler(exception_instance):
    print("uh-oh!")  # you probably want to do something more substantial here..
    print(exception_instance)


def recursiveFileList(inputPath):
    #recursiveFileList("/home/hadoop/Downloads/dataanalytics-main")

    try:
        res = []
        for (dir_path, dir_names, file_names) in walk(inputPath, onerror=walk_error_handler):
            res.extend(file_names)
        print(res)
    except Exception as e:
        print(e)


def recursiveDirList(inputPath):
    #recursiveDirList('/home/hadoop/Downloads/dataanalytics-main/Spark')

    if not path.exists(inputPath):
        raise TypeError("Path not found")
    for (root, dirs, files) in os.walk(inputPath, topdown=True):
        print(root)
        print(dirs)
        print(files)
        print('--------------------------------')
def getastring():
    #getastring()

    value = input("Please enter a string:\n")
    print(f'You entered {value}')

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


def readmysql(spark):

    #readmysql(spark)

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://127.0.0.1:3306/hivemetastore") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", "sales") \
        .option("user", "hive") \
        .option("password", 'hive123!') \
        .load()
    df.show()


def writeToMysql(df_input):
    db_connection = mysql.connector.connect(host='localhost', port=3306, user='hive', passwd='hive123!', db='mysql')

    # create and use a new database/schema TestDB
    db_cursor = db_connection.cursor()
    # db_cursor.execute("CREATE DATABASE TestDB;")
    db_cursor.execute("USE hivemetastore;")
    db_cursor.execute("select * from product")
    # tuples = db_cursor.fetchall()
    # print(tuples)
    iris_tuples = list(df_input.itertuples())
    iris_tuples_string = ",".join(["(" + ",".join([str(w) for w in wt]) + ")" for wt in iris_tuples])
    print(iris_tuples_string)
def createschemadynamic(spark):
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("/home/hadoop/Downloads/dataanalytics-main/Hive/sales/sales.csv")
    parts = lines.map(lambda l: l.split(","))
    # Each line is converted to a tuple.
    people = parts.map(lambda p: (p[0], p[1],p[2],p[3],p[4],p[5].strip()))

    # The schema is encoded in a string.
    schemaString = "id sname pid amt sdatets did"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaPeople = spark.createDataFrame(people, schema)

    # Creates a temporary view using the DataFrame
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT name FROM people")

    results.show()

def pandasreadcsv(path_value,spark):
    # path_value = '/home/hadoop/Downloads/dataanalytics-main/Hive/sales/sales1.csv'
    # df = pandasreadcsv(path_value, spark)

    P_df = pandas.read_csv(path_value,sep='|',index_col=False)
    P_df.reset_index(drop=True, inplace=True)
    print(P_df)
    S_df = spark.createDataFrame(P_df)
    print(S_df)
    # S_df.printSchema()
    #S_df.write.format('jdbc').options(url='jdbc:mysql://localhost/hivemetastore',driver='com.mysql.cj.jdbc.Driver',dbtable='marks',user='hive', password='hive123!').mode('append').save()
    #S_df.write.save('dfs:///user/hdfs/test/mark_new', format='parquet', mode='append')
    #S_df.coalesce(1).write.orc('hdfs:///user/hdfs/spark', 'overwrite')
    return S_df


def writetohdfs(df,spark):
    # path_value = '/home/hadoop/Downloads/dataanalytics-main/Hive/sales/sales1.csv'
    # df = pandasreadcsv(path_value, spark)
    # writetohdfs(df, spark)

    try:
        print(df)
        df.printSchema()
        df.coalesce(1).write.orc('hdfs:///user/hdfs/spark','overwrite')
    except:
        print('exception occured')
    else:
        # df.coalesce(1).write.format("orc").mode("overwrite").save("hdfs:///user/hdfs/spark/processed")
        # my_file = open("log_file.txt", "w")
        # fname ='hdfs:///user/hdfs/spark/processed/part-00000-705e06c8-51f5-4176-a4ac-53f326038245-c000.snappy.orc'
        # size = os.path.getsize('f:hdfs:///user/hdfs/spark/processed/part-00000-705e06c8-51f5-4176-a4ac-53f326038245-c000.snappy.orc')
        #
        # my_file.write(f'Filename:{fname}\n')
        # my_file.write("Status:completed\n")
        # print('Size of file is', size, 'bytes')

        # get file stats
        # stats = os.stat('f:/file.txt')
        # print('Size of file is', stats.st_size, 'bytes')
        #
        my_file = open("log_file.txt")
        content = my_file.read()
        my_file.close()
        print(content)

def hdfstohivetable(df,spark):
    # path_value = '/home/hadoop/Downloads/dataanalytics-main/Hive/sales/sales1.csv'
    # df = pandasreadcsv(path_value, spark)
    # writetohdfs(df, spark)
    # hdfstohivetable(df, spark)

    df.write.format("orc").mode("overwrite").saveAsTable("hive_store_db.test_table1")
    df1 = spark.sql("select * from hive_store_db.test_table1")
    #df1= spark.sql("create table test_table(val1 int,val2 int,val3 int) stored as orc location '/user/hdfs/spark")

    print(df)

def dropindex(spark):
    #dropindex(spark)

    l1 = ['blue', 'red', 'green', 'black', 'yellow']
    print(l1)
    df = pd.DataFrame(l1)
    df1 = spark.createDataFrame(df).show()
    # df = df.reset_index(drop=True)
    # print(df.head())
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

def handle_complexjsondata(spark):

    df = spark.read.option("multiline","true").json("file:///home/hadoop/Complexjson.json")
    df.select(explode("batters.batter"))
    df.show()
    df_final = df.withColumn("topping_explode",explode("topping"))\
                 .withColumn("topping_id",col("topping_explode.id"))\
                 .withColumn("topping_type",col("topping_explode.type"))\
                 .drop("topping","topping_explode")\
                 .withColumn("batter_explode",explode("batters.batter"))\
                 .withColumn("batter_id", col("batter_explode.id"))\
                 .withColumn("batter_type", col("batter_explode.type"))\
                 .drop("batters","batter_explode")
    df_final.show()


