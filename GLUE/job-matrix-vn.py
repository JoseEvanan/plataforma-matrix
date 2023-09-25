import sys
from pyspark.sql.functions import substring, to_timestamp, when, concat_ws
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark import SparkConf
from awsglue.job import Job
from pyspark.sql import functions as F


from zipfile import ZipFile
import pandas as pd
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
import boto3
import json
import operator
import io

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','CONFIG_TABLE','FILE_PROCCES'])

myconfig=SparkConf().set('spark.rpc.message.maxSize','1024').set('spark.driver.memory','9g') 
#we modified config of spark, because we have problem with big files.

#SparkConf can be directly used with its .set  property
sc = SparkContext(conf=myconfig)
#sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)



def_table = args['CONFIG_TABLE']
file_process = args['FILE_PROCCES']

print("FILE_PROCCES", file_process)


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


####### FUNCTIONS
def get_df_data(df, fields, procces):
    #print("get_df_data", procces)
    init = 1
    #Cast data type
    for field in fields:
        if field['type'] == 'TIMESTAMP':
            df = df.withColumn(field['name'].lower(), to_timestamp(substring        ('value', init, field['size']),'yyyyMMdd'))
        elif field['type'] == 'BOOLEAN': 
            name_tmp  = field['name'].lower()+ "_tmp"
            df = df.withColumn(name_tmp, substring('value', init    ,     field['size']))
            #df1 = df1.withColumn(name_tmp, to_timestamp(substring('value', init    ,     field['size']),'yyyyMMdd'))
            df = df.withColumn(field['name'].lower(), when(df[name_tmp] ==     "1"    ,True).when(df[name_tmp]  == "0",False))#.otherwise(df    .gender))
            df = df.drop(name_tmp)
        elif field['type'] == 'INTEGER' or field['type'] == 'BIGINT': 
            df = df.withColumn(field['name'].lower(), substring('value', init    ,     field['size']).cast("Integer"))
        elif operator.contains(field['type'], 'DECIMAL') : #into
            precision =int(field['type'].split('(')[1].split(',')[1][:-1])
            name_tmp  = field['name'].lower()+ "_tmp"
            name_tmp2  = field['name'].lower()+ "_tmp2"
            
            df = df.withColumn(name_tmp, substring('value', init , field['size']-precision))
            df = df.withColumn(name_tmp2, substring('value', init+ field['size']- precision, precision))
            
            df = df.withColumn(field['name'].lower(), concat_ws(".",df[name_tmp],df[name_tmp2]).cast("Float"))
            df = df.drop(name_tmp)
            df = df.drop(name_tmp2)
            #df1 = df1.withColumn(field['name'].lower(), substring('value', init    ,     field['size']))
            #df1 = df1.withColumn(field['name'].lower(), substring('value', init    ,     field['size']).cast("Float"))
        else:#Default is STRING
            df = df.withColumn(field['name'].lower(), substring('value', init    ,     field['size']))
        init = init + field['size']        
    df = df.drop("value")
    df = df.dropna()#Tiene 1 registor en blanco cada archivo
    if procces =='FULL':
        date_file_load = get_datetime_file(bucket, path_key_load01)
        #print("date_file_load", df.count())

        df = df.withColumn('filename_matrix', F.input_file_name())
        df = df.withColumn("fec_proc_matrix", F.to_date(F.lit(date_file_load), "yyyyMMdd")) 
        df = df.withColumn('load_user_matrix', F.lit("matrix-full-delta"))
    else:
        df = df.withColumn('filename_matrix', F.input_file_name())
        df = df.withColumn("split_col_path", F.split(F.col("filename_matrix"), "/")) \
                            .withColumn("filename_matrix_tmp", F.col("split_col_path").getItem(F.size(F.col("split_col_path"))-1)) \
                            .withColumn("split_file_tmp", F.split(F.col("filename_matrix_tmp"), "\.")) \
                            .withColumn("fec_load_proc_tmp", F.col("split_file_tmp").getItem(0))
        
        df = df.withColumn("fec_proc_matrix", F.to_date(F.col("fec_load_proc_tmp"), "yyyyMMdd")) 
        df = df.withColumn('load_user_matrix', F.lit("matrix-full-delta"))
        
        df = df.drop("split_col_path","split_file_tmp","fec_load_proc_tmp","filename_matrix_tmp")

    return df 

def get_script_merge(config_table):
    values_ = config_table

    table_name = values_['table_name']
    table_name_cdc = values_['table_name_cdc']
    schema = values_['schema']

    ddl = values_['data']
    fields_pks = ','.join(
        list(
        map(lambda x: f"A.{x.strip()} = B.{x.strip()}",
            values_['pks'].split(","))))

    fields = list(map(lambda x: x['name'], ddl))

    text_update = ' , '.join(map(lambda x: f"A.{x}=B.{x}", fields))

    text_insert_head = ' , '.join(fields)

    text_insert_value = ' , '.join(map(lambda x: f"B.{x}", fields))

    text_merge = f"""

            MERGE INTO {table_name} A 
            using {table_name}__cdc B 
            ON ( {fields_pks} ) 
            WHEN matched THEN 
                UPDATE SET {text_update}
            WHEN NOT matched THEN 
                INSERT ({fields}) 
                VALUES ({text_insert_head});
        """
    return text_merge

def get_script_procces(config_table):

    
    values_ = config_table

    table_full_redshift = values_["table_full_redshift"] 
    table_delta_redshift = values_["table_delta_redshift"]

    ddl = values_['data']

    fields = list(map(lambda x: x['name'], ddl))


    text_insert_head = ' , '.join(fields)
    text_insert_head = text_insert_head +", filename_matrix, fec_proc_matrix, load_user_matrix"
    text_pk = values_['pks'].strip()

    text_merge = f"""
          
            DELETE FROM {table_full_redshift}
            WHERE {text_pk} in ( SELECT distinct {text_pk}
            FROM  {table_delta_redshift});
            INSERT INTO {table_full_redshift} ({text_insert_head}) SELECT {text_insert_head} 
                    FROM  (
                        SELECT d.*, ROW_NUMBER() OVER(PARTITION BY d.{text_pk} ORDER BY d.fec_proc_matrix DESC) AS row_num
                        FROM {table_delta_redshift} d
                        )
                    WHERE row_num = 1
         """
    # --; DROP TABLE IF EXISTS {table_delta_redshift}
    #print("text_merge ", text_merge)
    return text_merge

def get_datetime_file(bucket, path_key):
    client = boto3.client('s3')
    
    obj = client.get_object(Bucket=bucket, Key=path_key)
    date_file_load = obj.get('LastModified').strftime("%Y%m%d")
    #(type(date_file_load),date_file_load)
    return date_file_load

def load_full_load(path_full, fields):

        df_full = spark.read.option("encoding", "ISO-8859-1").text(path_full)

        df_full = get_df_data(df_full, fields, "FULL")
        #df_full = df_full.dropna()

        #print("load full_load", df_full.count())
        #Redshift
        #table_redshift = "{0}.{1}".format(schema_redshift, name_table)
        df_full.write \
                            .format("com.databricks.spark.redshift") \
                            .option("url", url_redshift) \
                            .option("dbtable", config_table["table_full_redshift"]) \
                            .option("tempdir", table_temporal) \
                            .option("aws_iam_role", iam_redshift) \
                            .mode("overwrite") \
                            .save()
        
        print("Count FULL ", df_full.count())

def load_delta_load(path_full_delta, fields):
    
    ###### ALL DELTA
    #Validar si no hay cambios
    df_delta_all = spark.read.option("encoding", "ISO-8859-1").text(path_full_delta)

    df_delta_all = get_df_data(df_delta_all, fields, "DELTA")


    if len(pks.split(","))>=1 and pks != "":
        #Redshift
        print("With PKS", pks)

        sql_procces = get_script_procces(config_table)
        
        df_delta_all.write \
                        .format("com.databricks.spark.redshift") \
                        .option("url", url_redshift) \
                        .option("dbtable", config_table["table_delta_redshift"]) \
                        .option("tempdir", table_temporal) \
                        .option("aws_iam_role", iam_redshift) \
                        .option("postactions", sql_procces) \
                        .mode("overwrite") \
                        .save()

    else: #without PKs
        df_delta_all.write \
                            .format("com.databricks.spark.redshift") \
                            .option("url", url_redshift) \
                            .option("dbtable", config_table["table_full_redshift"]) \
                            .option("tempdir", table_temporal) \
                            .option("aws_iam_role", iam_redshift) \
                            .mode("append") \
                            .save()
#######################
#REDSHIFT
db_host = config_main['DB_HOST']
db_port = config_main['DB_PORT']
db_name = config_main['DB_NAME']
db_user = config_main['DB_USER']
db_pwd = config_main['DB_PWD']

table_temporal = config_main['TABLE_TEMPORAL']# Bucket temporary for files     load
iam_redshift = config_main['IAM_REDSHIFT'] #IAM asociate with cluster     redshift

# datos redshift
url_redshift = "jdbc:redshift://{0}:{1}/{2}?user={3}&password={4}".format(db_host, db_port, db_name, db_user, db_pwd)
#connection JDBC
schema_redshift = "trusted"
schema_redshift = "test"
#######################



fields = config_table["data"]
table_name_dir = config_table["table_name_dir"]
name_table = config_table["table_name"].lower()
#type_file = config_table["type_file"]
procces = config_table["procces"]
pks = config_table["pks"]

#config_table["schema"] = schema_redshift
config_table["table_full_redshift"]  = "{0}.{1}".format(schema_redshift, name_table)
config_table["table_delta_redshift"]  = "{0}.{1}__cdc".format(schema_redshift, name_table)

bucket = 'ue1stgdesaas3ftp001'
prefix_dir = "RAW-SFTP/F685/Julio2023/MATRIX"
prefix_dir = "RAW-SFTP/MATRIX"
#RAW-SFTP/MATRIX/CLTESDAT/FULL/
path_base = f's3://{bucket}/{prefix_dir}'

    
path_full = f'{path_base}/{table_name_dir}/FULL/'
path_key_load01 = f'{prefix_dir}/{table_name_dir}/FULL/LOAD001.TXT' # key first LOAD

path_full_delta = f'{path_base}/{table_name_dir}/DELTA/' 


d = (datetime.today() - timedelta(hours=5))  - timedelta(days=1) # NOTA,  revisar horario de ejecucion
prefix = d.strftime("%Y%m%d")

print("Procces",procces)
if procces == 'FULL':
    ###### FULL

    # key = "RAW-SFTP/MATRIX/CLTESDAT/FULL/LOAD001.TXT"

    load_full_load(path_full, fields)
    
elif procces == 'DELTA':
    """
    df = spark.read.\
           format("csv").\
           option("header", "true").\
           load("s3://bucket-name/file-name.csv")
    """
    path = ""
    if file_process != "None":
        partition = file_process.split(".")[-1]
        config_table["table_delta_redshift"]  = "{0}.{1}__cdc_{2}".format(schema_redshift, name_table,partition)
        print("partition", partition)
        path = f's3://{bucket}/{file_process}'

        import boto3
        import botocore
        from botocore.errorfactory import ClientError

        s3 = boto3.client('s3')
        try:
            s3.head_object(Bucket=bucket, Key=file_process)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The key does not exist.
                raise (f"No existe actualizacion para {path}")
            else:
                raise ("Error en la ingesta")
        
    else:
        path = path_full_delta
    print("table_delta_redshift", config_table["table_delta_redshift"])
    #path = f'{path_base}/{file_process}'


    

    print("path", path)


    load_delta_load(path, fields)

 
        

  
elif procces == 'FULL-DELTA':
    ###### FULL

    # key = "RAW-SFTP/MATRIX/CLTESDAT/FULL/LOAD001.TXT"

    load_full_load(path_full, fields)

    load_delta_load(path_full_delta, fields)

else:
    raise ("ERROR: Proceso no configurado")



job.commit()