
import pandas
import pandas as pd
import json
import os
import os.path
import logging
import logging.config
import getpass
from os import walk
from os import path
import mysql.connector
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from multiprocessing.pool import *

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
        #There are three ways to read text files into PySpark DataFrame
        #Read text file using spark.read.text()
        #f=spark.read.text("file:///home/hadoop/wc.txt")
        #Read text file using spark.read.csv()
        #f = spark.read.csv("file:///home/hadoop/wc.txt")
        #Read text file using spark.read.format()
        #f=spark.read.format("text").load("file:///home/hadoop/wc.txt")
        # f.show()

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

def readmysql(spark):
    #readmysql(spark)

    try:
        from configparser import ConfigParser
    except ImportError:
        from ConfigParser import ConfigParser  # ver. < 3.0

    # instantiate
    config = ConfigParser()

    # parse existing file
    config.read('default.ini')

    # read values from a section
    url = config.get('mysql_db','url')
    driver = config.get('mysql_db','driver')
    user = config.get('mysql_db','user')
    password = config.get('mysql_db','password')

    df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", "sales") \
        .option("user", user) \
        .option("password", password) \
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

    return S_df


def writetohdfs(df,spark):
    # path_value = '/home/hadoop/Downloads/dataanalytics-main/Hive/sales/sales1.csv'
    # df = pandasreadcsv(path_value, spark)
    # writetohdfs(df, spark)

    try:
        #print(df)
        df.printSchema()
        df.coalesce(1).write.orc('hdfs:///user/hdfs/spark','overwrite')
    except:
        logging.config.fileConfig(path.normpath("applog.config"))

        # Create the logger
        # Admin_Client: The name of a logger defined in the config file
        myLogger = logging.getLogger('utils')

        msg = 'This is a Warning'
        myLogger.debug(msg)
        myLogger.info(msg)
        myLogger.warn(msg)
        myLogger.error(msg)
        myLogger.critical(msg)

        # Shut down the logger
        logging.shutdown()

def read_hivetable(spark):
    #read from hive table

    spark.sql('use default;')

    #spark.sql('show tables;').show()
    df = spark.sql("select count(1) from storeinfo;")
    df.show(10)


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

def handle_complexjsondata(spark):
    #handle_complexjsondata(spark)

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


def createFiledynamic(filename):
    #createFiledynamic("test1")

    f =open("/home/hadoop/sample/"+filename+".txt","w")
    for i in range(100000):
        f.write("sample data creating:"+str(i))
    f.close()
    print("File created as:"+str(filename))
    return filename

def create_multipleFiledynamic():
    #create_multipleFiledynamic()

    parallel = ThreadPool(8)
    parallel.map(createFiledynamic,["test"+str(i) for i in range(10)])
