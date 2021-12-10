import sys #onetime
import logging
from io import StringIO # python3; python2: BytesIO 
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import col,lit
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
import pandas as pd
import s3fs
import ast
import time,datetime

def flatten_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [F.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df

def trim_string(untrimmed_df):
    string_cols = [f.name for f in untrimmed_df.schema.fields if (isinstance(f.dataType, StringType))]
    other_cols = [f.name for f in untrimmed_df.schema.fields if not(isinstance(f.dataType, StringType))]
    trimmed_df = untrimmed_df.select(other_cols+[col(c).substr(1, 6000).alias(c) for c in string_cols])
    trimmed_df.show(5)
    return trimmed_df 
    
   
def null_to_string(null_df):
    null_cols = [f.name for f in null_df.schema.fields if (isinstance(f.dataType, NullType))]
    other_cols = [f.name for f in null_df.schema.fields if not(isinstance(f.dataType, NullType))]
    string_df = null_df.select(other_cols+[col(c).cast(StringType()).alias(c) for c in null_cols])
    string_df.show(5)
    return string_df 
    
def rename_ambigious_col(df):
    old_col=df.schema.names
    running_list=[]
    new_col=[]
    i=0
    for column in old_col:
        if(column in running_list):
            new_col.append(column+"_"+str(i))
            i=i+1
        else:
            new_col.append(column)
            running_list.append(column)
            
    return new_col
    
## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dfnew1 = pd.read_csv('s3://convosight-prod-redshift/config/10_user_metrics_snapshot.csv')
print("-----------------printing config data----------------------------")
print(dfnew1)
print("-----------------printing no of rows in config file----------------------------")
no_of_table = len(dfnew1.index)
print(no_of_table)
max_update_time = []
max_update_timestamp = []
job_run_completion = []
no_of_rows = []
a =[]
b =[]
redshift_table = ''
redshift_table_main = ''
now = datetime.datetime.now()
job_runtime = now
for i in range(no_of_table):
    try:
        dynamodb_tablename = dfnew1['tablename'][i]
        partitionkey =  dfnew1['partitionkey'][i]
        sortkey =  dfnew1['sortkey'][i]
        createtime =  dfnew1['createtime'][i]
        updatetime =  dfnew1['updatetime'][i]
        cast_decision =  dfnew1['cast_columns'][i]
        print("-----------------------table details--------------------")
        print(dynamodb_tablename)
        print(partitionkey)
        print(sortkey)
        print(createtime)
        print(updatetime)
        print(cast_decision)
        
        datasource0 = glueContext.create_dynamic_frame.from_options(
            connection_type="dynamodb",
            connection_options={
                "dynamodb.input.tableName": dynamodb_tablename,
                "dynamodb.throughput.read.percent": "1.0",
                "dynamodb.splits": "472"
            }
        )
        
        
        print("-----------------------table schema-----------------------------")
        datasource0.printSchema()
        if cast_decision == 'none':
            datasource1 = ResolveChoice.apply(datasource0, choice = "make_cols")
        else :
            decision = list(ast.literal_eval(cast_decision))
            print(decision)
            datasource1 = ResolveChoice.apply(datasource0,specs = decision)
        datasource1 = ResolveChoice.apply(datasource1, choice = "make_cols")
        datasource1 = UnnestFrame.apply(frame = datasource1) 
        print("-----------------------after resolving table schema-----------------------------")
        datasource1.printSchema()
        dfirst0 = datasource1.toDF()
        dfirst01 = dfirst0.toDF(*[c.lower() for c in dfirst0.columns])
        dfirst = dfirst01.toDF(*(c.replace('.', '_') for c in dfirst01.columns))
        print("-----------------------all lower case table schema-----------------------------")
        dfirst.printSchema()
        dfirst = dfirst.withColumn("last_job_run",lit(now))
        dfirst = dfirst.withColumn("key_id",sha2(concat_ws("-",partitionkey,sortkey), 256))
        
        print("-----------------------table with hash key-----------------------------")
        dfirst.show(5)
        
        dfirst = dfirst.withColumn('createdatutc', to_timestamp("user_createdatutc"))
        dfirst = dfirst.withColumn("createddatetime",unix_timestamp("createdatutc", "yyyy-MM-dd HH:mm:ss"))
        dfirst = dfirst.withColumn('updatedatutc', to_timestamp("user_updatedatutc"))
        dfirst = dfirst.withColumn("updateddatetime",unix_timestamp("updatedatutc", "yyyy-MM-dd HH:mm:ss"))
        
        print("----------------------after_removing_ambigious_col------------------------------")
        after_removing_ambigious_col = rename_ambigious_col(dfirst)
        print(after_removing_ambigious_col)
        dfirst = dfirst.toDF(*after_removing_ambigious_col)
        
        scalar_cols = [f.name for f in dfirst.schema.fields if not(isinstance(f.dataType, ArrayType))]
        list_cols = [f.name for f in dfirst.schema.fields if (isinstance(f.dataType, ArrayType))]
        scalar_df = dfirst.select(*scalar_cols)
        after_flattening = flatten_df(scalar_df)
        after_trimming = trim_string(after_flattening)
        final_df = null_to_string(after_trimming)
        final_df.printSchema()
        print("------------final df------------")
        final_df.show(5)
        
        new_df = DynamicFrame.fromDF(final_df, glueContext, "dynamic_df")
        redshift_table = dynamodb_tablename.replace("-","_")
        redshift_table_main = redshift_table + "_main"
        if  updatetime == 'not-present':
            max_time = final_df.select(max("createddatetime")).collect()[0][0]
            max_timestamp = final_df.select(max("user_createdatutc")).collect()[0][0]
            
        else :
            max_time = final_df.select(max("updateddatetime")).collect()[0][0]
            max_timestamp = final_df.select(max("user_updatedatutc")).collect()[0][0]
        job_runtime = datetime.datetime.now()
        max_update_time.append(max_time)
        max_update_timestamp.append(max_timestamp)
        job_run_completion.append(job_runtime)
        row_count = dfirst.count()
        no_of_rows.append(row_count)
        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = new_df, catalog_connection = "prod_dynamo_db_redshift", connection_options = {"dbtable":redshift_table_main, "database": "convosight_prod_dynamo_data"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink1")
        
        for item in list_cols :
            
            sub_tab =dfirst.select(partitionkey,sortkey,"key_id",item,"updateddatetime","updatedatutc","createddatetime","createdatutc","last_job_run")
            sub_tab = sub_tab.select(partitionkey,sortkey,"key_id","updateddatetime","updatedatutc","createddatetime","createdatutc","last_job_run", F.posexplode(item).alias("position", "list_value"))
            sub_tab = sub_tab.withColumn("key_id_1",sha2(concat_ws("-","key_id","position"), 256))
                
            sub_tab_after_flattening = flatten_df(sub_tab)
            sub_tab_after_trimming = trim_string(sub_tab_after_flattening)
            sub_tab_final_df = null_to_string(sub_tab_after_trimming)
            sub_tab_final_df.printSchema()
            print("------------final df------------")
            sub_tab_final_df.show(5)
            new_df_1 = DynamicFrame.fromDF(sub_tab_final_df, glueContext, "dynamic_df")
            sub_table_name = "{}_{}".format(redshift_table,item)
            datasink_subtable = glueContext.write_dynamic_frame.from_jdbc_conf(frame = new_df_1, catalog_connection = "prod_dynamo_db_redshift", connection_options = {"dbtable": sub_table_name, "database": "convosight_prod_dynamo_data"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink2")
        
        print("table:- "+ dynamodb_tablename +" -> done")
        a.append(str(dynamodb_tablename))
        b.append("success")
    except Exception as e:
        logging.exception('table {} not done'.format(dynamodb_tablename))
        a.append(str(dynamodb_tablename))
        b.append("failed")
        max_update_time.append(0)
        max_update_timestamp.append(0)
        job_run_completion.append(job_runtime)
        continue
    
year = now.year
month = now.month
day = now.day
columns = ["table_name","status","row_count"]
status_df = spark.createDataFrame(zip(a, b,no_of_rows), columns).coalesce(1)
status_df.show()
status_df_glue = DynamicFrame.fromDF(status_df, glueContext, "dynamic_df")
dfnew1["MAX_UPDATE_TIME"] = max_update_time
dfnew1["MAX_UPDATE_TIMESTAMP"] = max_update_timestamp
dfnew1["JOB_RUN_COMPLETION"] = job_run_completion
dfnew1.to_csv ('s3://convosight-prod-redshift/config/10_user_metrics_snapshot.csv', index = False, header=True)
s3_target = "s3://convosight-prod-redshift/status_report/10_user_metrics_snapshot/"
s3path = s3_target + "/year=" + "{:0>4}".format(str(year)) + "/month=" + "{:0>2}".format(str(month)) + "/day=" + "{:0>2}".format(str(day)) + "/"
s3sink = glueContext.write_dynamic_frame.from_options(frame = status_df_glue, connection_type = "s3", connection_options = {"path": s3path}, format = "csv", transformation_ctx = "s3sink")
job.commit()
        