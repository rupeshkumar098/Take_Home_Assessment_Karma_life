#####-----Importing Libraries-----#####

#Importing python lib
from datatime import datetime
import sys

#Importing pyspark modules
from pyspark.context import SparkContext
import pyspark.sql.function as f

#Importing glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.jobs import Job
from awsglue.transforms import *








#Initialize context and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

#Parameters
glue_db = "raw_db"
glue_tbl1 = "raw_table_event"
glue_tbl2 = "raw_table_user"

###########################
#####-----Extract-----#####
###########################

#Log starting time
dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", dt_start)

#Read data of both the tables to Glue dynamic frames
dFrame_event = glue_context.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_tbl1)
dFrame_user = glue_context.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_tbl2)

#Convert dynamic frames to data frames to use sandard pyspark functions
df1 = dFrame_event.toDF()
df2_frame_user = dFrame_user.toDF()





#############################
#####-----Transform-----#####
#############################

#Creating a new data frame to store all reuired columns from exisiting data frames.
#After this join, df2 will have all columns of itself and 2 columns from df1, which is required for the Task.
df2.join(df1, df2.user_id == df1.where(f.array_contains(col("unique_users_installed_list"), df2.user_id))).select(df2["*"],df1["utm_medium","utm_campaign"])





########################
#####-----Load-----#####
########################

#Converting data frames to dynamic frames
dFrame_write = DynamicFrame.fromDF(df2, glue_context, "dFrame_write")

#Writing data back to s3 
## @type: DataSink
## @args: [connection_type = "s3", catalog_database_name = "Pre_db", format = "glueparquet", connection_options = {"path": "s3://final_table/new", "enableUpdateCatalog":true, "updateBehavior":"UPDATE_IN_DATABASE"}, catalog_table_name = "Final_tablle", transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = dFrame_write]
	
	
DataSink0 = glueContext.getSink(path = "s3://final_table/new", connection_type = "s3", updateBehavior = "UPDATE_IN_DATABASE", enableUpdateCatalog = True, transformation_ctx = "DataSink0")
DataSink0.setCatalogInfo(catalogDatabase = "Pre_db", catalogTableName = "Final_tablle")
DataSink0.setFormat("glueparquet")
DataSink0.writeFrame(dFrame_write)




