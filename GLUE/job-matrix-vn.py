import sys
from pyspark.sql.functions import substring, to_timestamp, when, concat_ws
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark import SparkConf
from awsglue.job import Job
from zipfile import ZipFile
import pandas as pd

from botocore.exceptions import ClientError
import boto3
import json
import operator
import io

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','CONFIG_TABLE'])

myconfig=SparkConf().set('spark.rpc.message.maxSize','1024').set('spark.driver.memory','9g') 
#we modified config of spark, because we have problem with big files.

#SparkConf can be directly used with its .set  property
sc = SparkContext(conf=myconfig)
#sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)



def_table = args['CONFIG_TABLE']
#custom config by table
config_table = json.loads(def_table)

job.init(args['JOB_NAME']+'-'+config_table['table_name'], args)


secret_name = "/PROD/MATRIX"
region_name = "us-east-1"

# Create a Secrets Manager client
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)


"""
s3://ue1stgdesaas3ftp001/RAW-SFTP/F685/Julio2023/MATRIX/TACUCAB/FULL/
"""

#Read SSM secret manager (information of conection  redshift)
try:
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
except ClientError as e:
    # For a list of exceptions thrown, see
    # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    print(e)
    raise e

# Decrypts secret using the associated KMS key.
secret = get_secret_value_response['SecretString']
config_main = json.loads(secret)
bucket_name = "ue1stgdesaas3ftp001"

if type(config_table["filename"]) == type([]):
    pass
else:
    list_files=config_table["filename"].split(',')
    config_table["filename"] = [config_table["filename"]]


"""
"type_file": "txt",
        "table_name_dir": "PTOSVTA",
"""
fields = config_table["data"]
table_name_dir = config_table["table_name_dir"]
type_file = config_table["type_file"]
is_merge = config_table["is_merge"]
pks = config_table["pks"]

path = f's3://ue1stgdesaas3ftp001/RAW-SFTP/F685/Julio2023/MATRIX/{table_name_dir}/FULL/'
df1 = spark.read.option("encoding", "ISO-8859-1").text(path)
init = 1
#Cast data type
for field in fields:
    if field['type'] == 'TIMESTAMP':
        df1 = df1.withColumn(field['name'].lower(), to_timestamp(substring        ('value', init, field['size']),'yyyyMMdd'))
    elif field['type'] == 'BOOLEAN': 
        name_tmp  = field['name'].lower()+ "_tmp"
        df1 = df1.withColumn(name_tmp, substring('value', init    ,     field['size']))
        #df1 = df1.withColumn(name_tmp, to_timestamp(substring('value', init    ,     field['size']),'yyyyMMdd'))
        df1 = df1.withColumn(field['name'].lower(), when(df1[name_tmp] ==     "1"    ,True).when(df1[name_tmp]  == "0",False))#.otherwise(df    .gender))
        df1 = df1.drop(name_tmp)
    elif field['type'] == 'INTEGER' or field['type'] == 'BIGINT': 
        df1 = df1.withColumn(field['name'].lower(), substring('value', init    ,     field['size']).cast("Integer"))
    elif operator.contains(field['type'], 'DECIMAL') : #into
        precision =int(field['type'].split('(')[1].split(',')[1][:-1])
        name_tmp  = field['name'].lower()+ "_tmp"
        name_tmp2  = field['name'].lower()+ "_tmp2"
        
        df1 = df1.withColumn(name_tmp, substring('value', init , field['size']-precision))
        df1 = df1.withColumn(name_tmp2, substring('value', init+ field['size']- precision, precision))
        
        df1 = df1.withColumn(field['name'].lower(), concat_ws(".",df1[name_tmp],df1[name_tmp2]).cast("Float"))
        df1 = df1.drop(name_tmp)
        df1 = df1.drop(name_tmp2)
        #df1 = df1.withColumn(field['name'].lower(), substring('value', init    ,     field['size']))
        #df1 = df1.withColumn(field['name'].lower(), substring('value', init    ,     field['size']).cast("Float"))
    else:#Default is STRING
        df1 = df1.withColumn(field['name'].lower(), substring('value', init    ,     field['size']))
    init = init + field['size']        
df1 = df1.drop("value")
#REDSHIFT
db_host = config_main['DB_HOST']
db_port = config_main['DB_PORT']
db_name = config_main['DB_NAME']
db_user = config_main['DB_USER']
db_pwd = config_main['DB_PWD']

table_temporal = config_main['TABLE_TEMPORAL']# Bucket temporary for files     load
iam_redshift = config_main['IAM_REDSHIFT'] #IAM asociate with cluster     redshift

# datos redshift
url_redshift = "jdbc:redshift://{0}:{1}/{2}?user={3}&password={4}".format        (db_host, db_port, db_name, db_user, db_pwd)
#connection JDBC
schema_redshift = "trusted"
schema_redshift = "test"
name_table = config_table["table_name"].lower()
table_redshift = "{0}.{1}".format(schema_redshift, name_table)

if True:

    df1.write \
                        .format("com.databricks.spark.redshift") \
                        .option("url", url_redshift) \
                        .option("dbtable", table_redshift) \
                        .option("tempdir", table_temporal) \
                        .option("aws_iam_role", iam_redshift) \
                        .mode("overwrite") \
                        .save()
else:
    df1.write \
                        .format("com.databricks.spark.redshift") \
                        .option("url", url_redshift) \
                        .option("dbtable", table_redshift) \
                        .option("tempdir", table_temporal) \
                        .option("aws_iam_role", iam_redshift) \
                        .option("postactions", query_merge) \
                        .mode("overwrite") \
                                .save()
#print (path)

job.commit()


data_frame.withColumn("input_file", input_file_name())



import os
import sys
import json
import boto3
import time
import datetime
import pprint
import botocore
import traceback
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config import setup_observer
from services.s3 import S3Service
from services.athena import AthenaService
from util import datalake_util
from pyspark.sql.types import IntegerType, StringType, LongType
from pyspark.sql import functions as F, SparkSession, SQLContext


#from datetime import datetime, timedelta
#from datalake import *

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'BUCKET_NAME', 'FOLDER_SCHEMA', 'TABLE_NAME', 'SELECTED_COLUMNS',
     'SELECTED_CODE_REF_EXTERNAL', 'CLEAN_COLUMNS_STRING',
     'JSON_SNS_PATH', 'INCREMENTAL', 'JSON_S3_PATH', 'JSON_ATHENA_PATH','JSON_GLUE_PATH',
     'GENERATE_DDL'
    ]
)



sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

FOLDER_HUB = "HUB"
FOLDER_RAW = "RAW"
FOLDER_OPT = "OPT"
FOLDER_TMP = "TMP"
FOLDER_CONFIG = "CONFIG"

for k, v in args.items():  # instance global variables
    globals()[k] = v

# 00 or 01
is_incremental = int(INCREMENTAL)
validation = '20' if is_incremental else 'LOAD'

is_incremental = int(INCREMENTAL)

REGION_NAME = 'us-east-1'

dt_s3 = S3Service({'bucket': BUCKET_NAME, 'region': REGION_NAME})
#dt_s3 = DatalakeS3Action(BUCKET_NAME,'us-east-1',JSON_SNS_PATH, JSON_S3_PATH, JSON_ATHENA_PATH)
dt_athena = AthenaService({'bucket': BUCKET_NAME, 'region': REGION_NAME})
#dt_athena = DatalakePersistenceDB(BUCKET_NAME,'us-east-1')

pub, s3_sub = setup_observer.init(dt_s3)

dict_sns = dt_s3.read_dict_from_path(JSON_SNS_PATH)

dict_glue = dt_s3.read_dict_from_path(JSON_GLUE_PATH)

dict_s3 = dt_s3.read_dict_from_path(JSON_S3_PATH)
# instance s3 global variables
for k, v in dict_s3.items():
    globals()[k.replace('--', '')] = v

for k, v in dict_glue.items():
    globals()[k.replace('--', '')] = v

dict_athena = dt_s3.read_dict_from_path(JSON_ATHENA_PATH)
for k, v in dict_athena.items():
    globals()[k.replace('--', '')] = v

SELECTED_COLUMNS = datalake_util.convert_coma(SELECTED_COLUMNS)
CLEAN_COLUMNS_STRING_PARSE = datalake_util.convert_coma(CLEAN_COLUMNS_STRING)

file_starting_txt = "starting.txt"
file_loading_txt = "loading.txt"
file_processing_txt = "processing.txt"
file_completing_txt = "completing.txt"
file_structure_pending_txt = "structure_pending.txt"
file_structure_processing_txt = "structure_processing.txt"
file_structure_sequence_txt = "structure_sequence.txt"
file_structure_transform_txt = "structure_transform.txt"
file_working_readme_ddl = "readme.ddl"
file_empty = ""

# Ultima foto
folder_tmp_athena = 'TMP_ATHENA'
folder_lzn = 'LZN'
folder_structure = 'STRUCTURE'
folder_work = 'WORKING'

arch_structure_lzn = "structure_lzn.txt"
arch_structure_pending = "structure_pending.txt"

tipo_carga = args['INCREMENTAL']
# -----------------------------------------------

file_auto_incremental_txt = "auto_incremental.txt"
file_initial_final_txt = "initial_final.txt"

type_alert = ["INFO", "WARNING", "SUCCESS"]
folder_structure_name = "STRUCTURE"
folder_process_name, folder_sequence_name, folder_transform_name = [
    "PROCESS", "SEQUENCE", "TRANSFORM"]
folder_detail_name, folder_meta_store_name, folder_lzn_name = [
    "DETAIL", "META_STORE", "LZN"]
folder_working_name, folder_test_name = ["WORKING", "TEST"]
# filter table name raw
#filter_schema_name = "{0}/{1}/{2}".format(FOLDER_RAW, FOLDER_PROJECT, FOLDER_SCHEMA)
filter_schema_name = "{0}/{1}".format(FOLDER_PROJECT, FOLDER_SCHEMA)
filter_table_name_raw = "{0}/{1}/".format(filter_schema_name, TABLE_NAME)
# raw
# filter table name temp
filter_schema_name = "{0}/{1}/{2}".format(FOLDER_TMP, FOLDER_PROJECT, FOLDER_SCHEMA)
filter_table_name_temp = "{0}/{1}".format(filter_schema_name, TABLE_NAME)
# temp
# filter table name hub
filter_schema_name = "{0}/{1}/{2}".format(FOLDER_HUB, FOLDER_PROJECT, FOLDER_SCHEMA)
filter_table_name_hub = "{0}/{1}".format(filter_schema_name, TABLE_NAME)
# hub
# filter table name opt
filter_schema_name = "{0}/{1}/{2}".format(FOLDER_OPT, FOLDER_PROJECT, FOLDER_SCHEMA)
filter_table_name_opt = "{0}/{1}".format(filter_schema_name, TABLE_NAME)
# opt

# filter table name config
filter_schema_name = "{0}/{1}/{2}".format(FOLDER_CONFIG, FOLDER_PROJECT, FOLDER_SCHEMA)
filter_table_name_config = "{0}/{1}".format(filter_schema_name, TABLE_NAME)
# config 

filter_process_name = "{0}/{1}".format(filter_table_name_temp, folder_process_name)
filter_structure_name = "{0}/{1}".format(
    filter_table_name_config, folder_structure_name)
filter_sequence_name = "{0}/{1}".format(filter_table_name_temp,
                                        folder_sequence_name)
filter_transform_name = "{0}/{1}".format(
    filter_table_name_temp, folder_transform_name)
#filter_lzn_name = "{0}/{1}".format(filter_table_name_opt, folder_lzn_name)
filter_lzn_name = "{0}".format(filter_table_name_opt)
filter_working_name = "{0}/{1}".format(filter_table_name_hub, folder_working_name)

filter_detail_name = "{0}/{1}".format(filter_table_name_temp, folder_detail_name)
filter_meta_store_name = "{0}/{1}".format(
    filter_table_name_temp, folder_meta_store_name)

create_file_table_processing = "{0}/{1}".format(
    filter_table_name_temp, file_processing_txt)
create_file_process_processing = "{0}/{1}".format(
    filter_process_name, file_processing_txt)
create_file_process_starting = "{0}/{1}".format(
    filter_process_name, file_starting_txt)
create_file_process_loading = "{0}/{1}".format(
    filter_process_name, file_loading_txt)
create_file_process_completing = "{0}/{1}".format(
    filter_process_name, file_completing_txt)
create_file_structure_pending = "{0}/{1}".format(
    filter_structure_name, file_structure_pending_txt)
create_file_structure_processing = "{0}/{1}".format(
    filter_structure_name, file_structure_processing_txt)
create_file_structure_sequence = "{0}/{1}".format(
    filter_structure_name, file_structure_sequence_txt)
create_file_structure_transform = "{0}/{1}".format(
    filter_structure_name, file_structure_transform_txt)

create_file_auto_incremental = "{0}/{1}".format(
    filter_structure_name, file_auto_incremental_txt)
create_file_initial_final = "{0}/{1}".format(
    filter_structure_name, file_initial_final_txt)


#CAMBIAR DE UBICACION ANTES DE SABER Q ES LOAD
#"""


if not is_incremental:
    prefix  = filter_table_name_raw + "LOAD"
    if datalake_util.validate(BUCKET_DATA, prefix):
        dt_s3.clear_folder_bulk(filter_transform_name + '/')
        dt_s3.clear_folder_bulk(filter_sequence_name + '/')
        dt_s3.clear_folder_bulk(filter_detail_name + '/')
        dt_s3.clear_folder_bulk(filter_structure_name + '/')
        dt_s3.clear_folder_bulk(filter_table_name_opt + '/')

        dt_s3.clear_folder_bulk(filter_table_name_temp + '/')
else:
    pass



def send_email_warning(status='***', table='', obs=''):
    arn = dict_sns['arn']

    subject = 'Data Lake - {} - {}'.format(status, table)
    body = "GENERICO: {}\nProceso: {}\nEstado: {}\nObservacion: {}".format(
        "Generico", table, status, obs
    )
    email_text = "From: {}\nSubject: {}\n\n{}".format(
        'Malla', subject, body
    )
    sns = boto3.client('sns', region_name=REGION_NAME)
    sns.publish(
        TopicArn=arn,
        Message=email_text
    )

def return_filename_timestamp(col):
    col_text = col.split("/")[-1]
    col_split_text = os.path.splitext(col_text)[0]
    return col_split_text

def handle_read_dataframe(spark, path):
    #import time
    flag = True
    retry = 1
    while flag:
        try:
            df = spark.read.parquet(path)
            flag = False
        except Exception as e:
            if retry > 3:
                flag = False
                print("Unexpected error: %s" % e)
                print("error: %s" % e)
                raise Exception("Unexpected error: %s" % e)
            else:
                time.sleep(retry*3)
                print(retry,"handle_read_df", datetime.datetime.now())
                retry += 1
    return df


def handle_write_dataframe(spark, df, path): #JOSE
    #import time
    flag = True
    retry = 1
    while flag:
        try:
            df.write.mode("overwrite").parquet(path)
            #dyf_tmp = DynamicFrame.fromDF(df, glueContext, "dyf_tmp")
            #glueContext.write_dynamic_frame.from_options(frame = dyf_tmp, connection_type = "s3",
            #             connection_options = {"path": path}, format = "parquet")
            flag = False
        except Exception as e:
            if retry > 3:
                flag = False
                #######
                #LIMPIAR CACHE
                spark.catalog.clearCache()
                #######
                print("Unexpected error: %s" % e)
                print("error: %s" % e)
                raise Exception("Unexpected error: %s" % e)
            else:
                time.sleep(retry*3)
                print(retry,"handle_write_df", datetime.datetime.now())
                retry += 1
    return df
    
return_filename_timestamp_udf = F.udf(return_filename_timestamp)

def GenOS():
    #######
    #LIMPIAR CACHE
    spark.catalog.clearCache()
    #######
    # list if csv files exist in the table
    to_persist = [
        file_empty, folder_process_name, folder_structure_name,
        folder_detail_name, folder_sequence_name, folder_meta_store_name,
        folder_transform_name, folder_lzn_name, folder_working_name,
        folder_test_name,
    ]

    table_s3_file = [s3_file.key for s3_file in\
        #dt_s3.bucket.objects.filter(Prefix=datalake_util.s3_path(filter_table_name_raw))#, Delimiter="/")
        dt_s3.get_objects(filter_table_name_raw)
        if s3_file.key.split("/")[3] not in to_persist]

    pub.dispatch({"bucket": BUCKET_PROCESS, "region":REGION_NAME})

    process_file_txt = [s3_file.key for s3_file in\
        #dt_s3.bucket.objects.filter(Prefix=datalake_util.s3_path(filter_process_name))
        dt_s3.get_objects(filter_process_name)
        if s3_file.key.split("/")[5] in\
            (file_starting_txt, file_loading_txt, file_processing_txt,
             file_completing_txt)]

    table_file_txt = [s3_file for s3_file in table_s3_file\
        if '.txt' in os.path.splitext(s3_file)]

    files = sorted([obj for obj in table_s3_file if str(
        obj.split("/")[-1]).startswith(validation)])

    files_filter = []

    for obj in table_s3_file:
        fname = str(obj.split("/")[-1])
        if fname.startswith(validation):
            files_filter.append(obj)
   
    files = sorted(files_filter)

    is_data = False
    if len(table_file_txt) <= 0 and len(process_file_txt) <= 0:
        ######################################################################
        # GEN 1
        # create file in process and table
        is_data = True
        with open(file_processing_txt, 'wb+') as data:
            dt_s3.handle_put_obj(dt_s3.s3_model.bucket, create_file_process_processing, data)
            #dt_s3.s3_model.bucket.put_object(Key=create_file_process_processing, Body=data)
            # CAMBIA DE BUCKET PARA IR A LA RAIZ
            pub.dispatch({"bucket": BUCKET_DATA, "region": REGION_NAME})
            dt_s3.handle_put_obj(dt_s3.s3_model.bucket, create_file_table_processing, data)
            #dt_s3.s3_model.bucket.put_object(Key=create_file_table_processing, Body=data)

        # move all file folder process
        count = 0
        globals()['files_raw'] = files
        for obj in files:
            count += 1
            obj_name = obj.split("/")[-1]
            dst_key = datalake_util.s3_path(filter_process_name, obj_name, file=True)
            copy_source = {'Bucket': BUCKET_DATA, 'Key': obj}
            dt_s3.s3_model.s3.Object(BUCKET_PROCESS, dst_key).copy_from(CopySource=copy_source)
            #dt_s3.s3.Object(BUCKET_NAME, obj).delete()
            if not is_incremental and count == 1:
                globals()['files_raw'] = [obj]
                time.sleep(2)
                break
        # create file in process
        pub.dispatch({"bucket": BUCKET_PROCESS, "region": REGION_NAME})
        with open(file_starting_txt, 'wb+') as data:
            dt_s3.handle_put_obj(dt_s3.s3_model.bucket, create_file_process_starting, data)
            #dt_s3.s3_model.bucket.put_object(Key=create_file_process_starting, Body=data)

        # delete file processing table and process
        dt_s3.s3_model.s3.Object(BUCKET_PROCESS, create_file_process_processing).delete()
        dt_s3.s3_model.s3.Object(BUCKET_DATA, create_file_table_processing).delete()

        time.sleep(5)

    ######################################################################
    # GEN 2
    # list all files in process
    #process_s3_file = [s3_file.key for s3_file in dt_s3.bucket.objects.filter(Prefix=filter_process_name)
    process_s3_file = [s3_file.key for s3_file in dt_s3.get_objects(filter_process_name)
                    if s3_file.key.split("/")[5] not in file_empty]

    print("*************************")
    print(process_s3_file)
    print("*************************")
    # list all files in process if exist file starting.txt
    process_file_starting = [s3_file for s3_file in process_s3_file
                            if s3_file.split("/")[5] in file_starting_txt]
    # list all files in process if not exist file loading, processing o completing
    process_file_txt = [s3_file for s3_file in process_s3_file
                        if s3_file.split("/")[5] in (file_loading_txt, file_processing_txt, file_completing_txt)]


    files = sorted([obj for obj in process_s3_file if str(obj.split("/")[-1]).startswith(validation)])

    print(process_file_starting)
    print(process_s3_file)

    if len(process_s3_file) > 0 and len(process_file_starting) > 0:

        print("Entro en el proceso del generico 2")

        # create file loading in process
        with open(file_loading_txt, 'wb+') as data:
            dt_s3.handle_put_obj(dt_s3.s3_model.bucket, create_file_process_loading, data)
            #dt_s3.s3_model.bucket.put_object(Key=create_file_process_loading, Body=data)

        dt_s3.handle_download_file_obj(create_file_structure_pending, file_structure_pending_txt)
        current_value = datalake_util.file_open_read_update(file_structure_pending_txt, 1)
        VALOR_GENERICO = current_value

        # create folder inside detail
        with open(file_loading_txt, 'wb+') as data:
            create_sequence_inside_loading_txt = datalake_util.create_sequence_folder_inside_number(
                filter_table_name_raw, folder_sequence_name, current_value, file_loading_txt)
            dt_s3.handle_put_obj(dt_s3.s3_model.bucket, create_sequence_inside_loading_txt, data)
            #dt_s3.s3_model.bucket.put_object(
            #    Key=create_sequence_inside_loading_txt, Body=data)

        print(" ***************** Proceso gen  2 *************")
        print(" ----------- Files ------------")
        print(create_file_structure_pending)
        print(create_file_structure_sequence)
        print(create_sequence_inside_loading_txt)
        print(files)
        print(" ------------------------------")
        print(current_value)
        print(" ********************************************** ")

        # move all files inside the new folder
        for obj in files:
            obj_name = obj.split("/")[-1]
            dst_key = datalake_util.create_sequence_folder_inside_number(
                filter_table_name_temp, folder_sequence_name, current_value, obj_name)
            dst_key_detail = datalake_util.create_detail_folder_inside_number(
                filter_detail_name, obj_name)
            copy_source = {'Bucket': BUCKET_PROCESS, 'Key': obj}
            dt_s3.s3_model.s3.Object(BUCKET_PROCESS, dst_key).copy_from(CopySource=copy_source)
            dt_s3.s3_model.s3.Object(BUCKET_PROCESS, dst_key_detail).copy_from(
                CopySource=copy_source)
            dt_s3.s3_model.s3.Object(BUCKET_PROCESS, obj).delete()

        # delete file inside folder loading.txt
        dt_s3.s3_model.s3.Object(BUCKET_PROCESS, create_sequence_inside_loading_txt).delete()

        # create file completing inside new folder
        with open(file_completing_txt, 'wb+') as data:
            create_sequence_inside_completing_txt = datalake_util.create_sequence_folder_inside_number(
                filter_table_name_temp, folder_sequence_name, current_value, file_completing_txt)
            #dt_s3.s3_model.bucket.put_object(
            #    Key=create_sequence_inside_completing_txt, Body=data)

        # delete file starting and loading from process
        dt_s3.s3_model.s3.Object(BUCKET_PROCESS, create_file_process_starting).delete()
        dt_s3.s3_model.s3.Object(BUCKET_PROCESS, create_file_process_loading).delete()

        time.sleep(5)
    else:
        #JOSE
        ntable = FOLDER_PROJECT + '-' + FOLDER_SCHEMA + '-' + TABLE_NAME
        obs_ = "No se proceso la tabla, por que no hay archivos que procesar"
        send_email_warning(status='WARNING', table=ntable, obs=obs_)
        #ENVIAR WARNING
        return
    ###########################################################################################
    # GEN 3
    # read current structure_pending from folder structure
    
    current_value = VALOR_GENERICO
    
    block_s3 = VALOR_GENERICO
    
    #Actuliza el archivode monitoreo

    print(" ***************** Proceso gen  3 *************")
    print(" ----------- Files ------------")
    print(create_file_structure_pending)
    print(create_file_structure_sequence)
    print(" ------------------------------")
    print(current_value, block_s3)
    print(" ********************************************** ")

    # if exist blocks
    # JOSE ERROR COMUN QUE EL ARCHIVO NO SE ACTULIZA
    if current_value == block_s3:# or True: se dejara que pase todo por aca

        print("Entro en el proceso del generico 3")

        filter_block = "{0}/{1}".format(filter_sequence_name, block_s3)
        path_block = "{0}{1}/{2}/{3}/".format(S3_SCHEMA,
                                             BUCKET_PROCESS, filter_sequence_name, block_s3)
        path_block_number = "{0}*.csv".format(path_block)
        path_block_transform = "{0}{1}/{2}/{3}/{3}.csv".format(S3_SCHEMA, BUCKET_PROCESS, filter_transform_name,
                                                               block_s3)

        #sequence_load_files = sorted([s3_file.key for s3_file in dt_s3.s3_model.bucket.objects.filter(Prefix=filter_block)
        sequence_load_files = sorted([s3_file.key for s3_file in dt_s3.get_objects(filter_block)
                                      if str(s3_file.key.split("/")[-1]).startswith("LOAD")])

        df_load = spark.read.format("com.databricks.spark.csv") \
            .option("multiLine", "true") \
            .option("escape", "\"") \
            .option("charset", "ISO-8859-1") \
            .option("parserLib", "univocity") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .load(path_block_number)

        print("SELECT_COLUMNS")

        # selected columns
        if len(SELECTED_COLUMNS) > 0:
            df_select = df_load.select(
                [c for c in df_load.columns if c in SELECTED_COLUMNS])
        else:
            df_select = df_load

        print("CLEAN_STRING")
        # data clean strings

        #df_select2 = trimAllColumns(df_select)
        print(CLEAN_COLUMNS_STRING)
        if len(CLEAN_COLUMNS_STRING_PARSE) > 0:
            print('entro limpieza determinadas columnas')
            df_select2 = datalake_util.clean_columns_string(df_select, CLEAN_COLUMNS_STRING_PARSE)
        else:
            if CLEAN_COLUMNS_STRING in '_ALL_':
                print('limpieza total')
                df_select2 = datalake_util.clean_columns_string(df_select, df_select.columns)
            else:
                print('sin limpieza')
                df_select2 = df_select

        print("CREATE UPLOAD COLUMNS")

        # create column of block and upload
        date_now = datetime.datetime.now()- datetime.timedelta(hours=5)
        current_date = str(date_now.strftime('%Y-%m-%d %H:%M:%S'))

        df1 = df_select2.withColumn('UPLOAD_PROCESS_DATE', F.lit(current_date).cast("string"))
        
        print("CREATE FILENAME")
        # if compare load and cdc
        #print(df1.show())
        if len(sequence_load_files) > 0:
            print("***** if compare load and cdc **********")
            # create column of filename if exist LOAD
            df2 = df1.withColumn('FILENAME', F.lit(
                "19000101-0{0}0000000".format(str(int(block_s3)))).cast("string"))
        else:
            # create column of filename if exist CDC
            print(" ********* create column of filename if exist CDC ********")
            df2 = df1.withColumn('FILENAME', return_filename_timestamp_udf(
                F.input_file_name()).cast("string"))

        print("CREATE REFEXTERNA")
        # generate ref external
        #tmp_refexterna = SELECTED_CODE_REF_EXTERNAL
        #SELECTED_CODE_REF_EXTERNAL = tmp_refexterna.upper().replace(" ","")
        arr_col = SELECTED_CODE_REF_EXTERNAL.split(",")
        if SELECTED_CODE_REF_EXTERNAL not in '_NONE_':
            if len(arr_col) > 0:
                print('entro')
                df2.registerTempTable("tmp_transform")
                length = len(arr_col)
                valor = ''
                pipe = '|'
                for index in range(length - 1):
                    valor = valor + \
                        'COALESCE(x.{0},""),"'.format(arr_col[index]) + str(pipe) + '",'
                    #print("valor",valor)
                valor = valor + 'COALESCE(x.{0},"")'.format(arr_col[length - 1])
                print(valor)
                query_select = """ select  x.*,CONCAT({0},'') as REFEXTERNA_PROCESO from tmp_transform x
                            """.format(valor)
                # ref_external_udf = F.udf(lambda cols: "|".join([x if x not in (None, "|") else "*" for x in cols]),
                #                         StringType())
                # df_cod_external = df2.withColumn("REFEXTERNA", ref_external_udf(F.array(selected_code_ref_external)))
                df_cod_external = spark.sql(query_select)
            else:
                df_cod_external = df2
        else:
            df_cod_external = df2
        ############################
        #df2.unpersist()
        #SQLContext.uncacheTable("tmp_transform")#JOSE
        ############################
        print("CREATE COLUMNS OP")
        print(sequence_load_files)
        # if compare load and cdc
        if len(sequence_load_files) > 0:
            print(df_cod_external.columns)
            df4 = df_cod_external.select((F.lit("I")).alias('Op'), "*")
        else:
            df4 = df_cod_external


    ###########################################################################################
    # GEN 4
    current_value = VALOR_GENERICO
    
    block_s3 = VALOR_GENERICO
    
    #ESTRUCTURE-JOSE incremental-download 
    dt_s3.handle_download_file_obj(create_file_auto_incremental, file_auto_incremental_txt)
    last_id = int(datalake_util.file_open_read_update(file_auto_incremental_txt, 0))
    print(current_value, block_s3)

    print(" ***************** Proceso gen  4 *************")
    print(" ----------- Files ------------")
    print(create_file_structure_sequence)
    print(create_file_structure_transform)
    print(" ------------------------------")
    print(current_value, block_s3)
    print(" ********************************************** ")

    file = 'file_origen.csv'
    file_new = 'file_id.csv'
    file_valorinicial = 'inicial_valor.txt'
    file_valorfinal = 'file_valorfinal.txt'

    more_binary_data = b''
    # if exist blocks
    if current_value == block_s3:
        path_block = "{0}{1}/{2}/{3}".format(S3_SCHEMA,
                                             BUCKET_PROCESS, filter_transform_name, block_s3)
        path_block_number = "{0}/{1}.csv".format(path_block, block_s3)
        file_block_number = "{0}/{1}/{2}.csv".format(
            filter_transform_name, block_s3, block_s3)
        path_block_lzn = "{0}{1}/{2}/bloque={3}".format(#JOSE
            S3_SCHEMA, BUCKET_PROCESS, filter_lzn_name, block_s3)
        path_block_lzn_csv = "{0}{1}/{2}/{3}/{3}".format(
            S3_SCHEMA, BUCKET_PROCESS, filter_lzn_name, block_s3)
        file_block_lzn_csv = "{0}/{1}/{2}".format(
            filter_lzn_name, block_s3, 'file_uniqueid.csv')
        file_load_block_lzn_csv = "{0}{1}/{2}/{3}/{4}".format(
            S3_SCHEMA, BUCKET_PROCESS, filter_lzn_name, block_s3, 'file_uniqueid.csv')

        #### JOSEEEE  - REEMPLAZAR EL UNIQUE ID  INICIO ## 
        #UTILIZAR df4 para agregar el  UNIQUEID por dataframe
        init_val = last_id # + 1
        cant = int(df4.count())
        #df5 = df4.withColumn('unique_val_tmp', F.monotonically_increasing_id())
        from pyspark.sql import Window
        #from pyspark.sql.functions import desc
        #window = Window.orderBy(F.col('filename').desc())

        df4 = df4.withColumn('filename_int', F.regexp_replace('filename', '-', '').cast(LongType()) )

        window = Window.orderBy(F.col('filename_int'))

        #F.row_number().over(window) -- started in '1'
        df5 = df4.withColumn('unique_val_tmp', F.row_number().over(window))
        df5 = df5.withColumn('UNIQUEID', df5['unique_val_tmp'].cast(IntegerType()) + init_val)
        df5 = df5.withColumn("UNIQUEID",df5['UNIQUEID'].cast(StringType()))
        df5 = df5.drop("unique_val_tmp")
        df5 = df5.drop("filename_int")

        final_val = init_val + cant

        last_id_final = datalake_util.file_open_update_new_val(
                    file_valorfinal, final_val)
        #JOSE: Escribe en el fs el nuevo valor del autoincremental
        #JOSE: Sube arl archivo autoimcremental actualizado

        #ESTRUCTURE-JOSE incremental-upload 
        
        #FALTA CAMBIAR A handle
        #JAIME -CAMBIAR PARA QUE UTILICEN LA FUNCION handle
        # BUSCA A TODOS LOS Q UTILIZAN "dt_s3.s3_model.bucket"
        #dt_s3.handle_upload_file_obj(file_valorfinal, create_file_auto_incremental)
        
        dt_s3.s3_model.bucket.upload_file(
            file_valorfinal, create_file_auto_incremental)


        df_load = df5
        columns_new = []
        for index in df_load.columns:
            if (index != 'UNIQUEID'):
                columns_new.append(index)
        columns_new.append('UNIQUEID')

        df_load = df_load.select([c for c in columns_new])


        try:
            print("CREATE PARQUET LZN")

            print(path_block_lzn)
            #Guardar sin coalesce
            #df_load.write.mode("overwrite").parquet(path_block_lzn)
            handle_write_dataframe(spark, df_load, path_block_lzn)

            for obj in files_raw:
                dt_s3.s3_model.s3.Object(BUCKET_DATA, obj).delete()


            dt_s3.handle_upload_file_obj(create_file_structure_pending, file_structure_pending_txt)
            time.sleep(10)
            # update structure_transform
        except Exception as e:
            # restore structure_transform.txt
            print(e)
            raise Exception('Error al escribir en LZN')
        time.sleep(5)

def setup_snapshot_photo():
    #rutas athena / temporal
    global path_temp_get_athena
    path_temp_get_athena = "s3://{BUCKET_PROCESS}/{FOLDER_TMP}/{FOLDER_PROJECT}/{FOLDER_SCHEMA}/{TABLE_NAME}/{folder_tmp_athena}".format(**globals())
    global path_temp_get_athena2
    path_temp_get_athena2 = "{FOLDER_TMP}/{FOLDER_PROJECT}/{FOLDER_SCHEMA}/{TABLE_NAME}/{folder_tmp_athena}".format(**globals())
    # ruta archivo de control de bloques
    path_folder_structure_lzn = "{FOLDER_CONFIG}/{FOLDER_PROJECT}/{FOLDER_SCHEMA}/{TABLE_NAME}/{folder_structure}/{arch_structure_lzn}".format(**globals())
    path_folder_structure_pending = "{FOLDER_CONFIG}/{FOLDER_PROJECT}/{FOLDER_SCHEMA}/{TABLE_NAME}/{folder_structure}/{arch_structure_pending}".format(**globals())
        
    print(" *************** Ultima foto ***************")    
    print(path_folder_structure_lzn)
    print(path_folder_structure_pending)
    print(" *******************************************")

    #dt_s3.bucket.download_file(path_folder_structure_lzn, arch_structure_lzn)
    #
    #ESTRUCTURE-JOSE lzn-download 
    dt_s3.handle_download_file_obj(path_folder_structure_lzn, arch_structure_lzn)
    
    #dt_s3.bucket.download_file(path_folder_structure_pending, arch_structure_pending)
    #ESTRUCTURE-JOSE pending-download archivo
    dt_s3.handle_download_file_obj(path_folder_structure_pending, arch_structure_pending)
    #global block_value_ini
    block_value_ini = datalake_util.file_open_read_update(arch_structure_lzn, 1)
    #global block_value_fin
    block_value_fin = datalake_util.file_open_read_update(arch_structure_pending, 0)

    return block_value_ini, block_value_fin, path_folder_structure_lzn

def snapshot_photo(tipo_carga):

    block_value_ini, block_value_fin, path_folder_structure_lzn = setup_snapshot_photo()

    globals()['block_value_ini'] = block_value_ini
    globals()['block_value_fin'] = block_value_fin

    #verifica si es primera carga
    if(tipo_carga == '0'):

        print(" ****** tipo de carga *******", tipo_carga)
        print(block_value_ini, block_value_fin)

        if not is_incremental and int(block_value_ini) == 1:
            dt_s3.clear_folder_bulk(filter_table_name_hub + '/')

        # procesa bloque por bloque
        while (int(block_value_ini) <= int(block_value_fin)):
            #######################################################################################################################################################################################
            # leer bloques lzn
            #path_lzn_current_block = "s3://{0}/{1}/{2}/{3}/{4}/{5}/{6}.parquet".format(bucket_name,folder_project,folder_schema,folder_table_stg,folder_lzn,block_value_ini,block_value_ini)
            path_lzn_current_block = "s3://{BUCKET_PROCESS}/{FOLDER_OPT}/{FOLDER_PROJECT}/{FOLDER_SCHEMA}/{TABLE_NAME}/bloque={block_value_ini}".format(**globals())
            #df_lzn = spark.read.parquet(path_lzn_current_block)
            df_lzn = handle_read_dataframe(spark, path_lzn_current_block)
            # Que elimne duplicados en la misma particion cunado es historico
            
            #no borrar duplicados cuando es historico
            #df_lzn = df_lzn.withColumn("idunique",df_lzn['UNIQUEID'].cast(IntegerType()) )
            #df_lzn = df_lzn.orderBy(F.col('idunique').desc()).dropDuplicates(subset = ['REFEXTERNA_PROCESO'])
            #df_lzn = df_lzn.drop("idunique")

            # escribir bloques lzn_working
            #path_work_current_block = "s3://{0}/{1}/{2}/{3}/{4}/{5}/bloque={6}".format(bucket_name,folder_project,folder_schema,folder_table_stg,folder_work,folder_table_stg,block_value_ini)
            path_work_current_block = "s3://{BUCKET_PROCESS}/{FOLDER_HUB}/{FOLDER_PROJECT}/{FOLDER_SCHEMA}/{TABLE_NAME}/bloque={block_value_ini}".format(**globals())
            #df_lzn = df_lzn.withColumnRenamed('UPLOAD_BLOCK_PROCESS','bloque')
            print(path_work_current_block)
            df_lzn.coalesce(1).write.mode("overwrite").parquet(path_work_current_block)

            #df_lzn.write.parquet(path_work_current_block,mode="overwrite")
            #######################################################################################################################################################################################
            #print(df_lzn.show())
            #######################################################################################################################################################################################
            # scripts para refrescar particiones
            dt_athena.refresh_partition(SCHEMA_DB_ATHENA,TABLE_NAME,block_value_ini,path_work_current_block, path_temp_get_athena)
            
            
            dt_s3.handle_upload_file_obj(path_folder_structure_lzn, arch_structure_lzn)
            
            block_value_ini = datalake_util.file_open_read_update(arch_structure_lzn, 1)
            globals()["block_value_ini"] = block_value_ini
            
            print("*******************_2")
            print(globals()["block_value_ini"])
            #######################################################################################################################################################################################
    #Flujo carga Diaria  "CD"
    if(tipo_carga == '1'):

        print(" ****** tipo de carga *******", tipo_carga)
        print(block_value_ini, block_value_fin)
        # procesa bloque por bloque
        while (int(block_value_ini) <= int(block_value_fin)):
            #######################################################################################################################################################################################
            # leer bloques lzn
            #path_lzn_current_block = "s3://{0}/{1}/{2}/{3}/{4}/{5}/{6}.parquet".format(bucket_name,folder_project,folder_schema,folder_table_stg,folder_lzn,block_value_ini,block_value_ini)
            path_lzn_current_block = "s3://{BUCKET_PROCESS}/{FOLDER_OPT}/{FOLDER_PROJECT}/{FOLDER_SCHEMA}/{TABLE_NAME}/bloque={block_value_ini}".format(**globals())
            #df_lzn = spark.read.parquet(path_lzn_current_block)
            try:
                df_lzn = handle_read_dataframe(spark, path_lzn_current_block)
            except Exception as e:
                if block_value_ini == block_value_fin:
                    raise Exception(e)
                else:#PARCHE TEMPORAL POR QUE LOS PENDING NO ESTAN IGUAL Q LOS LZN
                    #######################################################################################################################################################################################
                    # Actualiza archivo structure_lzn.txt - Leer marcador de el ultimo block corrido
                    #dt_s3.bucket.upload_file(arch_structure_lzn, path_folder_structure_lzn)
                    #ESTRUCTURE-JOSE lzn-upload
                    dt_s3.handle_upload_file_obj(path_folder_structure_lzn, arch_structure_lzn)
                    #s3.Bucket(bucket_name).download_file(path_folder_structure_lzn, arch_structure_lzn)
                    block_value_ini = datalake_util.file_open_read_update(arch_structure_lzn, 1)
                    globals()["block_value_ini"] = block_value_ini
                    
                    print("*******************_2")
                    print(block_value_ini)
                    #######################################################################################################################################################################################
                    
                    # limpiar archivos de temporal
                    dt_s3.delete_folder(path_temp_get_athena2)
            
            # escribir bloques lzn_working
            #path_work_current_block = "s3://{0}/{1}/{2}/{3}/{4}/{5}/bloque={6}".format(bucket_name,folder_project,folder_schema,folder_table_stg,folder_work,folder_table_stg,block_value_ini)
            path_work_current_block = "s3://{BUCKET_PROCESS}/{FOLDER_HUB}/{FOLDER_PROJECT}/{FOLDER_SCHEMA}/{TABLE_NAME}/bloque={block_value_ini}".format(**globals())
            

            query_list_bloque = "select distinct t2.bloque from {0}.{1} as t2 where t2.bloque <>'{2}' and t2.refexterna_proceso in (select x2.refexterna_proceso from {0}.{1} as x2 where x2.bloque ='{2}');"
            query_list_bloque = query_list_bloque.format(SCHEMA_DB_ATHENA,TABLE_NAME,block_value_ini)

            df_lzn = df_lzn.withColumn("idunique",df_lzn['UNIQUEID'].cast(IntegerType()) )
            df_lzn = df_lzn.orderBy(F.col('idunique').desc()).dropDuplicates(subset = ['REFEXTERNA_PROCESO'])
            df_bloque_foto = df_lzn.drop("idunique")

            #df_bloque_foto.write.parquet(path_work_current_block,mode="overwrite")
            df_bloque_foto.coalesce(1).write.mode("overwrite").parquet(path_work_current_block)
            # REFRESCA PARTICION DE ULTIMA FOTO
            dt_athena.refresh_partition(SCHEMA_DB_ATHENA, TABLE_NAME,  block_value_ini, path_work_current_block, path_temp_get_athena)
            
            # calcula bloques a recrear
            path_resultado = dt_athena.set_script_athena(glue_region, SCHEMA_DB_ATHENA, query_list_bloque, path_temp_get_athena)
            id_script = "{0}/{1}.csv".format(path_temp_get_athena,path_resultado)
            df_list = spark.read.format("com.databricks.spark.csv").option("header","true").option("parserLib", "univocity").option("charset", "ISO-8859-1").option("escape", "\"").option("multiLine", "true").load(id_script)
            lista = df_list.collect()
            print("***********")
            for dato in lista:
                dato = str(dato['bloque'])
                globals()['dato'] = dato
                print("dato: "+dato)
                #path_rp_bloque_tmp = "s3://{BUCKET_PROCESS}/{FOLDER_TMP}/{FOLDER_PROJECT}/{FOLDER_SCHEMA}/{TABLE_NAME}_TMP/bloque={dato}".format(**globals())
                path_rp_bloque_tmp = "s3://{BUCKET_PROCESS}/{FOLDER_TMP}/{FOLDER_PROJECT}/{FOLDER_SCHEMA}/{TABLE_NAME}/WORKING_TMP/bloque={dato}".format(**globals())
                path_rp_bloque = "s3://{BUCKET_PROCESS}/{FOLDER_HUB}/{FOLDER_PROJECT}/{FOLDER_SCHEMA}/{TABLE_NAME}/bloque={dato}".format(**globals())

                #df_bloque_foto
                #df_rp_foto = spark.read.parquet(path_rp_bloque)
                df_rp_foto = handle_read_dataframe(spark, path_rp_bloque)

                df_rp_foto = df_rp_foto.alias('ta')
                tb = df_bloque_foto.alias('tb')
                df_rp_foto = df_rp_foto.join(tb, df_rp_foto['REFEXTERNA_PROCESO'] == tb['REFEXTERNA_PROCESO'],how='left') # Could also use 'left_outer'
                df_rp_foto = df_rp_foto.filter(F.col('tb.REFEXTERNA_PROCESO').isNull()).select('ta.*')#.show()
                
                handle_write_dataframe(spark, df_rp_foto, path_rp_bloque_tmp)
                
                #df_rp_foto.coalesce(1).write.mode("overwrite").parquet(path_rp_bloque_tmp)
                #df_rp_foto.write.parquet(path_rp_bloque_tmp,mode="overwrite")
                

                #df_rp_foto_f = spark.read.parquet(path_rp_bloque_tmp)
                df_rp_foto_f = handle_read_dataframe(spark, path_rp_bloque_tmp)

                handle_write_dataframe(spark, df_rp_foto_f, path_rp_bloque)

                #df_rp_foto_f.coalesce(1).write.mode("overwrite").parquet(path_rp_bloque)

                #df_rp_foto_f.write.parquet(path_rp_bloque,mode="overwrite")
                #######################################################################################################################################################################################
                # funcion para refrescar particiones
                dt_athena.refresh_partition(SCHEMA_DB_ATHENA, TABLE_NAME, dato, path_rp_bloque, path_temp_get_athena)
                #######################################################################################################################################################################################

            #######################################################################################################################################################################################
            # Actualiza archivo structure_lzn.txt - Leer marcador de el ultimo block corrido
            #dt_s3.bucket.upload_file(arch_structure_lzn, path_folder_structure_lzn)
            #ESTRUCTURE-JOSE lzn-upload
            dt_s3.handle_upload_file_obj(path_folder_structure_lzn, arch_structure_lzn)
            #s3.Bucket(bucket_name).download_file(path_folder_structure_lzn, arch_structure_lzn)
            block_value_ini = datalake_util.file_open_read_update(arch_structure_lzn, 1)
            globals()["block_value_ini"] = block_value_ini
            
            print("*******************_2")
            print(block_value_ini)
            #######################################################################################################################################################################################
            
            # limpiar archivos de temporal
            dt_s3.delete_folder(path_temp_get_athena2)

def main():
    # persistence TABLE ATHENA
    if int(GENERATE_DDL):
        #SCHEMA_DB_ATHENA, TABLE_NAME, table_columns, BUCKET_NAME, FOLDER_PROJECT, FOLDER_SCHEMA
        dict_tmp_athena = dict()
        dt_athena.persistence_table_athena(filter_table_name_raw, **dict_tmp_athena)

    # Prepare enviroment
    folder_structure_name = "STRUCTURE"
    file_structure_pending_txt = "structure_pending.txt"
    file_source_file = '{0}/{1}/{2}'.format(
        filter_table_name_config,
        folder_structure_name,
        file_structure_pending_txt
    )
    print(" ****** Ruta de el strucutre ***********")
    print(file_source_file)
    try:
        dt_s3.handle_download_file_obj(file_source_file, file_structure_pending_txt)
        bloque_actual = int(datalake_util.file_open_read_update(file_structure_pending_txt))
    except Exception:
        bloque_actual = 0
        print('Strucutre no encontrado')

    if bloque_actual == 0:
        dt_s3.building_working(filter_table_name_temp)

    #if len(list(dt_s3.bucket.objects.filter(Prefix=filter_table_name_config + '/STRUCTURE/'))) == 0:
    if len(list(dt_s3.get_objects("{0}/STRUCTURE/".format(filter_table_name_config)))) == 0:
        dt_s3.building_structure_initial(filter_table_name_config)

    # Cantidad de csv's
    # cont  representa la cantidad de csv a procesar
    # XXX: queda probar todos los LOADs de porrazo
    cont = 0
    print(filter_table_name_raw)

    #for obj in dt_s3.bucket.objects.filter(Prefix=datalake_util.s3_path(filter_table_name_raw)):#, Delimiter="/"):

    print("************************ Change bucket **********************")

    pub.dispatch({"bucket": BUCKET_DATA, "region": REGION_NAME})

    print(dt_s3.get_objects(filter_table_name_raw))

    is_load_in_cdc = False

    for obj in dt_s3.get_objects(filter_table_name_raw):#, Delimiter="/"):
        
        name = str(obj.key.split("/")[3])
        print("filter 1 ",obj.key, name,is_incremental)
        if name.endswith(".csv") and name.startswith(validation):
            cont += 1
        if is_incremental:
            if name.endswith(".csv") and name.startswith("LOAD"):
                is_load_in_cdc = True
                break
        else:
            if "LOAD00000001.csv".upper() == name.upper():
                print("SE LIMPIO DATA")
                """
                dt_s3.clear_folder_bulk(filter_transform_name + '/')
                dt_s3.clear_folder_bulk(filter_sequence_name + '/')
                dt_s3.clear_folder_bulk(filter_detail_name + '/')
                dt_s3.clear_folder_bulk(filter_structure_name + '/')
                dt_s3.clear_folder_bulk(filter_table_name_opt + '/')
                """
                #AGREGAR PRINT PARA CUANDO SE EJCUTA HISTORICO POR PARTES
    print("is_load_in_cdc",is_load_in_cdc)
    
    
    if is_incremental and cont > 0:
        cont = 1

    if bloque_actual == 0:
        cont = cont
    else:
        # XXX: cont = cont - bloque_actual
        pass

    if is_load_in_cdc:
        # JOSE VALIDADOR QUE NO SE EJECUTE  INCREMENTAL SI  HAY LOADDDd
        cont = -1
        ntable = FOLDER_PROJECT + '-' + FOLDER_SCHEMA + '-' + TABLE_NAME
        obs_ = "No se puede procesar CDC si hay LOAD en el directorio"
        send_email_warning(status='WARNING', table=ntable, obs=obs_)
        print(obs_)
    
    print('CANTIDAD DE VECES QUE SE EJECUTARA EL GENERICO -> ', cont)
    bloque = bloque_actual
    
    while cont > 0:
        bloque = bloque + 1
        try:
            pub.dispatch({"bucket": BUCKET_DATA, "region": REGION_NAME})
            GenOS()
            dt_s3.building_backup_x_block(bloque, filter_table_name_temp)
            cont = cont - 1
            print('Processing ......... ', cont)
            dt_s3.delete_before_backup(bloque - 1, filter_table_name_temp)

        except Exception as e:
            print("++++++++++++++++++++++++++++++++")
            print(e)
            print("++++++++++++++++++++++++++++++++")
            err = traceback.format_exc()
            print("********************************")
            print(err)
            print("********************************")
            dt_s3.clear_folder(datalake_util.s3_path(filter_table_name_temp))
            print('Waiting ....... ')
            dt_s3.delete_block_sequence(bloque, filter_table_name_temp)
            dt_s3.building_restore_x_block(bloque - 1, filter_table_name_temp)
            cont = -6
            raise Exception(err)


if __name__ == "__main__":
   
    main()
    pub.dispatch({"bucket": BUCKET_PROCESS, "region": REGION_NAME})
    snapshot_photo(tipo_carga)

#job.commit()