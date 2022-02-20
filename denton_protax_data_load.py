import os 
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark import SparkConf , SparkContext
from pyspark.sql.functions import lit
from pyspark.sql import HiveContext



current_year='2017'
table_nm='multi_owner'
file_nm='/home/cloudera/Downloads/Denton/Protax/multi_owner.csv'
input_file_location='/home/cloudera/Downloads/Denton/Protax/_CommentsExport.txt'
dest_hdfs_path='hdfs://master01.ellicium.com:8020/data/'





#Check file status
if os.path.isfile(input_file_location):
    print ("File exist")
else:
    print ("File not exist")


#get current date
start_time = datetime.now().strftime("%Y%m%d%H%M")
print(start_time)

write_file_path=dest_hdfs_path+table_nm+'_'+start_time+'.csv'
print(write_file_path)


#Intilialize spark shell
conf = SparkConf().setAppName("Data Ingestion").setMaster('local')
sc = SparkContext(conf=conf)
sqc = SQLContext(sc)
hvc=HiveContext(sc)



#read file into dataframe
df = sqc.read.format("text").option("delimiter","|").load(wri)
df.show()

#add 2 columns
df = df.withColumn('DataLoad_Timestamp', lit(start_time))
df = df.withColumn('Data_Calender_Year', lit(current_year))
row_count=df.count()


if table_nm in sqlContext.tableNames("default"):
	print("Exist")
else:
	print("not exist")

#write csv into destination folder
#df.write.format("csv").save(write_file_path)



