import os
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark import SparkConf , SparkContext
from pyspark.sql.functions import lit
from pyspark.sql import HiveContext , SparkSession


database_nm='denton_district'
current_year='2017'
table_nm='Appraisal_State_Code'
input_file_location='/home/vaibhav_mr10/data/denton_district/APPRAISAL_STATE_CODE.TXT'
input_file_metadata='/home/vaibhav_mr10/metadata.csv'
dest_hdfs_path='/data/'

#Check file status
if os.path.isfile(input_file_location) and os.path.isfile(input_file_metadata):
   print (input_file_location)
   print ("File exist")
else:
   print ("File not exist")
   print (input_file_location)

#Intilialize spark shell
#Spark SESSION
spark = SparkSession.builder.master("local").appName("Data Ingestion").config("spark.sql.warehouse.dir", "/user/hive/warehouse").enableHiveSupport().getOrCreate()
print("SparkSession Created")

#conf = SparkConf().setAppName("Data Ingestion").setMaster('local')
#sc = SparkContext(conf=conf)
sqc = SQLContext(spark)
#hvc=HiveContext(sc)

#check hive table exists or not
if table_nm in sqc.tableNames(database_nm):
print(table_nm+'**************'+"Exist")
else:
print(table_nm+'*****************'+"Not exist")


#read file into dataframe
df = spark.read.format('com.databricks.spark.csv').options(header='true').options(sep=",").load('file:'+input_file_metadata)
print("File Read Successfully!!!!!!!!!!!!!!!!!!!!!!")
#get current date

#start_time = datetime.now().strftime("%Y%m%d%H%M")
#print(start_time)

#add 2 columns
#df = df.withColumn('DataLoad_Timestamp', lit(start_time))
#df = df.withColumn('Data_Calender_Year', lit(current_year))
#df.show(1)

#write_file_path=dest_hdfs_path+database_nm+'/'+table_nm+'/'+start_time
#write csv into destination hdfs folder
#df.write.format('com.databricks.spark.csv').options(header='false').options(sep=',').save(write_file_path)




#check input(df.count) and destination(hive count(*))file row count
#df_count=df.count()
#print("Dataframe row count:",df_count)

#Add parition to the hive table