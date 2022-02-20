try:
	import traceback
except Exception,e:
	print "Error while importing package 'traceback':"
	print e
	exit()


try:
	from pyspark.sql import HiveContext
	from pyspark.sql.types import *
	from pyspark.sql import Row
	from pyspark.sql.SparkSession 
except Exception,e:
	traceback.print_exc()
	exit()


def load_file_to_DF():
	spark = SparkSession.builder 
    .master("local") 
    .appName("loadFileInHive") 
    .getOrCreate()

    
