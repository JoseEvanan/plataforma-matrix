import json
import boto3
from json import dumps
from pprint import pprint
import boto3




def lambda_handler(event, context):
    
    name_job_glue = "UE1COMDESAGLUMAT002"
    client = boto3.client('glue')
    config_path = "config.json"

    pprint(event)
    #raise
    
     
    # Opening JSON file
    f = open(config_path)
     
    # returns JSON object as
    # a dictionary
    config_table = json.load(f)
    #type-process "DELTA"/"FULL"/"FULL-DELTA"
    table_process = "DELTA"
    file_process = "None"
    if "process" in event :
        table_process = event["process"]
        if  "tables" in event:
            for table in event["tables"]:
                config_table_base = config_table[table.upper().strip()]
                
                config_table_base["procces"] = table_process
                print("config_table_base",config_table_base)
                #raise
                data_str = dumps(config_table_base)
                
                print(file_process, data_str)

                if "from_date" in event:
                    file_process = f"from-{event['from_date']}"
                response = client.start_job_run(
                           JobName = name_job_glue,
                           Arguments = {
                             '--CONFIG_TABLE':   data_str,
                             '--FILE_PROCCES': file_process
                })
        else:
            raise ("No hay tablas para procesar")
                
                
    elif "Records" in event:
        for record in event['Records']:
            #print(key, data)
            if "s3" in record and 'object' in record['s3'] and 'key' and record['s3']['object']:
                file_process = record['s3']['object']['key']
                #s3://ue1stgdesaas3ftp001/RAW-SFTP/MATRIX/CLTESDAT/DELTA/20230801.001
                table_rename = file_process.split("/")[-3]
                print(table_rename)
                config_table_base = {}
                for k,v in config_table.items():
                    if table_rename == v["table_name_dir"].upper():
                        config_table_base = v
                        
                        config_table_base["procces"] = table_process
                if len(config_table_base) == 0:
                    raise("La tabla no esta registrada en la configuracion")
                else:
                    data_str = dumps(config_table_base)
                    print(file_process, data_str)
                    response = client.start_job_run(
                                   JobName = name_job_glue,
                                   Arguments = {
                                     '--CONFIG_TABLE':   data_str,
                                     '--FILE_PROCCES': file_process
                        })
                    
                
            else: 
                raise("proceso no tiene archivo para procesar")

    

    return {
        'statusCode': 200,
        'body': data_str
        }