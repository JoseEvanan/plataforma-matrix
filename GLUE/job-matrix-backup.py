import boto3
import json
import pg
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ['CONFIG_TABLE', 'PROCCES'])


for k, v in args.items():  # instance global variables
    globals()[k] = v


def_table = args['CONFIG_TABLE']
procces = args['PROCCES'] # backup - restore
#custom config by table
config_table = json.loads(def_table)


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
print(config_main)



def get_redshift_connection(DB_HOST,DB_PORT,DB_NAME,DB_USER,DB_PWD):
    """
    redshift_client = boto3.client('redshift',
    region_name='us-east-1')
    
    creds = redshift_client.get_cluster_credentials(
      DbUser=DB_USER,
      DbName=DB_NAME,
      ClusterIdentifier="ue1dbaprodreddlk001",
      DurationSeconds=3600)
    DB_PWD=creds['DbPassword']
    DB_USER=creds['DbUser']
    """

    rs_conn_string = "host=%s port=%s dbname=%s user=%s password=%s" % (
        DB_HOST,
        DB_PORT,
        DB_NAME,
        DB_USER,
        DB_PWD
    )
    conn = pg.connect(dbname=rs_conn_string)
    return conn



def ini(config_main, config_table, procces):
    #REDSHIFT
    db_host = config_main['DB_HOST']
    db_port = config_main['DB_PORT']
    db_name = config_main['DB_NAME']
    db_user = config_main['DB_USER']
    db_pwd = config_main['DB_PWD']

    iam_redshift = config_main['IAM_REDSHIFT'] #IAM asociate with cluster     redshift

    schema_redshift = "trusted"
    schema_redshift = "test"
    name_table = config_table["table_name"].lower()



    # create a date object representing March 1, 2023
    start_date = datetime.now()
    # the date can be formatted as a string if needed
    date_str = start_date.strftime('%Y%m%d')
    print(date_str)

    conn = get_redshift_connection(db_host,db_port,db_name,db_user,db_pwd)

    if procces == 'BACKUP':
        query_procces = f"""
                unload ('select * from {schema_redshift}.{name_table}')
            to 's3://ue1stgdesaas3mat005/BACKUP/{date_str}/{name_table}/'
            iam_role '{iam_redshift}'
            FORMAT AS PARQUET
            ALLOWOVERWRITE
            maxfilesize 512 mb;
                """
    elif procces == 'RESTORE':
        query_procces = f"""
                    COPY {schema_redshift}.{name_table}
                        FROM 's3://ue1stgdesaas3mat005/BACKUP/{date_str}/{name_table}/'
                        IAM_ROLE '{iam_redshift}'
                        FORMAT AS PARQUET;
                """
    else:
        raise  ("Proceso no conocido")
    
    conn.query(query_procces)#JOSE

ini(config_main, config_table, procces)

#s3://ue1stgdesaas3mat005/PLUGINS/req14-0.1-py3.6.egg
#https://ue1stgdesaas3mat005.s3.amazonaws.com/PLUGINS/req14-0.1-py3.6.egg