import json
import boto3
from json import dumps
from pprint import pprint
import boto3


def get_files(s3_client,bucket, prefix, token=None):
    
    
    if token:
        objects = s3_client.list_objects_v2(Bucket='ue1stgdesaas3ftp001',Prefix='RAW-SFTP/F685/Julio2023/MATRIX/', ContinuationToken=token)
    else:
        objects = s3_client.list_objects_v2(Bucket='ue1stgdesaas3ftp001',Prefix='RAW-SFTP/F685/Julio2023/MATRIX/')
    
    return  objects
    

    
def lambda_handler2(event, context):
    # TODO implement
    from datetime import datetime, timedelta
    #operation = event['operation']
    print(type(event))
    print(event)
    
    client = boto3.client('glue')
    
    for key, data in event.items():
        print(key, data)

        #for job in data['data']:
            # conversion
        data_str = dumps(data)
        
        response = client.start_job_run(
                       JobName = "job-matrix-vn", #'job-matrix-backup',#'job-matrix-backup', #'job-matrix-test',#job_matrix #job-M_ACUMULACION
                       Arguments = {
                         '--CONFIG_TABLE':   data_str
            })
        print(response)
        #raise
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def lambda_handler3(event, context):
    """
    Copy files event
    From: ue1stgdesaas3ftp001/RAW-SFTP/MATRIX/
    To: ue1stgdesaas3mat001/RAW/
    """
    s3 = boto3.resource('s3')
    pprint(event)
    print("*"*10)
    bucket_to  = "ue1stgdesaas3mat001"
    prefix_to = "RAW/"
    
    bucket_from = ""
    key_from = ""
    namefile = ""
    
    is_delta = False
    
    for record in event['Records']:
        bucket_from = record['s3']['bucket']['name']
        key_from = record['s3']['object']['key']
        
        if "DELTA" in key_from:
            is_delta = True
        else: 
            print("Carga Historica en proceso")
        
        #filename  = key_from.split('/')[-1]
        
    
 
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def lambda_handler(event, context):
    client = boto3.client('glue')
    
    
    for record in event['Records']:
        
    for key, data in event.items():
    for record in event['Records']:
        print(key, data)
        
        file_process = "s3://ue1stgdesaas3ftp001/RAW-SFTP/MATRIX/CLTESDAT/DELTA/20230807.001"
        file_process = "None"
        #key_from = record['s3']['object']['key']
        if "s3" in record and '' in record['s3'] and '' and record['s3']['object']
        file_process = "RAW-SFTP/MATRIX/CLTESDAT/DELTA/20230807.001"
        #file_process = "None"
        #for job in data['data']:
            # conversion
        data_str = dumps(data)
        
        response = client.start_job_run(
                       JobName = "job-matrix-vn", #'job-matrix-backup',#'job-matrix-backup', #'job-matrix-test',#job_matrix #job-M_ACUMULACION
                       Arguments = {
                         '--CONFIG_TABLE':   data_str,
                         '--FILE_PROCCES': file_process
            })
        print(response)
        #raise
    return {
        'statusCode': 200,
        'body': data_str
        }