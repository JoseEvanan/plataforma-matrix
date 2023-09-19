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


####### FUNCTIONS
def get_df_data(df, fields):
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
    return df 

def get_script_merge(config_table):
    values_ = config_table

    table_name = values_['table_name']

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

def get_script_delta_lote():
    pass


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
type_file = config_table["type_file"]
procces = config_table["procces"]
pks = config_table["pks"]


bucket = 'ue1stgdesaas3ftp001'
prefix_dir = "RAW-SFTP/F685/Julio2023/MATRIX"
prefix_dir = "RAW-SFTP/MATRIX"
#RAW-SFTP/MATRIX/CLTESDAT/FULL/
path_base = f's3://{bucket}/{prefix_dir}'

d = (datetime.today() - timedelta(hours=5))  - timedelta(days=1) # NOTA,  revisar horario de ejecucion
prefix = d.strftime("%Y%m%d")

print("Procces",procces)
if procces == 'FULL':
    path = f'{path_base}/{table_name_dir}/FULL/'
    df_full = spark.read.option("encoding", "ISO-8859-1").text(path)
    
    df_full = get_df_data(df_full, fields)


    df_full = df_full.withColumn('fec_load_proc', F.lit(prefix))
    df_full = df_full.withColumn('load_user', F.lit("matrix"))
    
    #Redshift
    table_redshift = "{0}.{1}".format(schema_redshift, name_table)
    df_full.write \
                        .format("com.databricks.spark.redshift") \
                        .option("url", url_redshift) \
                        .option("dbtable", table_redshift) \
                        .option("tempdir", table_temporal) \
                        .option("aws_iam_role", iam_redshift) \
                        .mode("overwrite") \
                        .save()
    
elif procces == 'DELTA':
    
    prefix_file = f'{prefix_dir}/{table_name_dir}/DELTA/{prefix}.001'

    path = f'{path_base}/{table_name_dir}/DELTA/{prefix}'

    import boto3
    import botocore
    from botocore.errorfactory import ClientError

    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket, Key=prefix_file)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The key does not exist.
            raise (f"No existe actualizacion para {path}")
        else:
            raise ("Error en la ingesta")


    #Validar si no hay cambios
    df_delta_day = spark.read.option("encoding", "ISO-8859-1").text(path)
    df_delta_day = get_df_data(df_delta_day, fields)
    #df = df.drop("value")
    df_delta_day.withColumn('filename', F.input_file_name())
    df_delta_day = df_delta_day.withColumn('fec_load_proc', F.split('filename', '.')[0])
    df_delta_day = df_delta_day.withColumn('load_user', F.lit("matrix"))

    print(df_delta_day)
    if len(pks.split())>1:
        #Redshift
        table_redshift = "{0}.{1}__cdc".format(schema_redshift, name_table)
        sql_merge = get_script_merge(config_table)
        df_delta_day.write \
                        .format("com.databricks.spark.redshift") \
                        .option("url", url_redshift) \
                        .option("dbtable", table_redshift) \
                        .option("tempdir", table_temporal) \
                        .option("aws_iam_role", iam_redshift) \
                        .option("postactions", sql_merge) \
                        .mode("overwrite") \
                        .save()
    else: #without PKs
        #Redshift
        table_redshift = "{0}.{1}".format(schema_redshift, name_table)
        df_delta_day.write \
                            .format("com.databricks.spark.redshift") \
                            .option("url", url_redshift) \
                            .option("dbtable", table_redshift) \
                            .option("tempdir", table_temporal) \
                            .option("aws_iam_role", iam_redshift) \
                            .mode("append") \
                            .save()
        

  
elif procces == 'FULL-DELTA':
    ###### FULL
    """
    path = f'{path_base}/{table_name_dir}/FULL/'
    df_full = spark.read.option("encoding", "ISO-8859-1").text(path)
    
    df_full = get_df_data(df_full, fields)
    df_full = df_full.withColumn('fec_load_proc', F.lit(prefix))
    df_full = df_full.withColumn('load_user', F.lit("matrix"))
    #Redshift
    table_redshift = "{0}.{1}".format(schema_redshift, name_table)
    df_full.write \
                        .format("com.databricks.spark.redshift") \
                        .option("url", url_redshift) \
                        .option("dbtable", table_redshift) \
                        .option("tempdir", table_temporal) \
                        .option("aws_iam_role", iam_redshift) \
                        .mode("overwrite") \
                        .save()
    
    print("Count FULL ", df_full.count())
    """
    ###### ALL DELTA
    path = f'{path_base}/{table_name_dir}/DELTA/'
    #Validar si no hay cambios
    df_delta_all = spark.read.option("encoding", "ISO-8859-1").text(path)

    #df_delta_all = df_delta_all.filter(df[""].isNotNull())

    df_delta_all = df_delta_all.dropna()

    df_delta_all = get_df_data(df_delta_all, fields)
    
    df_delta_all = df_delta_all.withColumn('filename', F.input_file_name())
    #print((test.split("/")[-1]).split(".")[0])
    #split_col_path = F.split(df['filename'], '/')
    #plit(df['dob'], '-').getItem(0))
    
    df_delta_all = df_delta_all.withColumn("split_col_path", F.split(F.col("filename"), "/")) \
                        .withColumn("filename_matrix", F.col("split_col_path").getItem(F.size(F.col("split_col_path"))-1)) \
                        .withColumn("split_file_tmp", F.split(F.col("filename_matrix"), "\.")) \
                        .withColumn("fec_load_proc_tmp", F.col("split_file_tmp").getItem(0))
    
    df_delta_all = df_delta_all.withColumn("fec_proc_matrix", F.to_date(F.col("fec_load_proc_tmp"), "yyyyMMdd")) 
    df_delta_all.show(10)
    print("#"*20)
    df_delta_all.select('split_file_tmp',"fec_proc_matrix").show(10)
    
    df_delta_all = df_delta_all.drop("split_col_path")
    df_delta_all = df_delta_all.drop("split_file_tmp")
    df_delta_all = df_delta_all.drop("fec_load_proc_tmp")
    df_delta_all = df_delta_all.drop("filename")


    
    #20230804
    """
    from pyspark.sql.functions import split, col, size
    
    df.withColumn("Splits", split(col("s"), " ")) \
        .withColumn("0th", col("Splits").getItem(0)) \
        .withColumn("3rd", col("Splits").getItem(3)) \
        .withColumn("1st_from_end", col("Splits").getItem(size(col("Splits"))-1)) \
        .drop("Splits")
    """
    #df_delta_all = df_delta_all.withColumn('fec_load_proc', split_col.getItem(0))
    df_delta_all = df_delta_all.withColumn('load_user_matrix', F.lit("matrix"))
    #df_delta_all = df_delta_all.drop("filename")
    df_delta_all.show(10)
    print("Count DELTA ", df_delta_all.count())

    if len(pks.split(","))>=1:
        #Redshift
        print("CON PKS")
        table_redshift = "{0}.{1}__cdc".format(schema_redshift, name_table)
        sql_merge = get_script_merge(config_table)
        df_delta_all.write \
                        .format("com.databricks.spark.redshift") \
                        .option("url", url_redshift) \
                        .option("dbtable", table_redshift) \
                        .option("tempdir", table_temporal) \
                        .option("aws_iam_role", iam_redshift) \
                        .mode("overwrite") \
                        .save()
        """
        df_delta_all.write \
                        .format("com.databricks.spark.redshift") \
                        .option("url", url_redshift) \
                        .option("dbtable", table_redshift) \
                        .option("tempdir", table_temporal) \
                        .option("aws_iam_role", iam_redshift) \
                        .option("postactions", sql_merge) \
                        .mode("overwrite") \
                        .save()
        """
    else: #without PKs
        #Redshift
        table_redshift = "{0}.{1}".format(schema_redshift, name_table)
        df_delta_all.write \
                            .format("com.databricks.spark.redshift") \
                            .option("url", url_redshift) \
                            .option("dbtable", table_redshift) \
                            .option("tempdir", table_temporal) \
                            .option("aws_iam_role", iam_redshift) \
                            .mode("append") \
                            .save()

else:
    raise ("ERROR: Proceso no configurado")



job.commit()