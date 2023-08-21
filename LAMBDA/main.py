

import os
import time
import json
import boto3 
from pprint import pprint

class InternalErrorQueryEngine(Exception):
    pass

class HiveCannotOpenSplit(Exception):
    pass

def set_script_athena(region, db_athena, script_athena, path_temp_get_athena):
    athena_client = boto3.client('athena',
                    region_name=region
                    )
    try:
        ## Actualiza tabla
        queryStart = athena_client.start_query_execution(
            QueryString = script_athena,
            QueryExecutionContext = {'Database': db_athena},
            ResultConfiguration={'OutputLocation': path_temp_get_athena}
        )
        estado_script = 'RUNNING'
        while estado_script  in ('RUNNING', 'QUEUED'):
            response = athena_client.get_query_execution(QueryExecutionId = queryStart['QueryExecutionId'])
            execution_status = response['QueryExecution']['Status']
            estado_script = execution_status['State']
            if estado_script == 'CANCELLED':
                print(script_athena)
                raise Exception('Query CANCELLED')
            elif  estado_script == 'FAILED':
                print(script_athena)
                print(response)
                if 'INTERNAL_ERROR_QUERY_ENGINE' in  execution_status['StateChangeReason']: 
                    raise InternalErrorQueryEngine()
                elif 'HIVE_CANNOT_OPEN_SPLIT' in execution_status['StateChangeReason']:
                    raise HiveCannotOpenSplit()
                else:
                    raise Exception('Query FAILED')
            time.sleep(2)
        resultado_athena = queryStart['QueryExecutionId']
    except athena_client.exceptions.TooManyRequestsException:
        time.sleep(15)
        resultado_athena = set_script_athena(region, db_athena, script_athena, path_temp_get_athena)
    except InternalErrorQueryEngine:
        time.sleep(15)
        resultado_athena = set_script_athena(region, db_athena, script_athena, path_temp_get_athena)
    except HiveCannotOpenSplit:
        time.sleep(15)
        resultado_athena = set_script_athena(region, db_athena, script_athena, path_temp_get_athena)
    return resultado_athena

def delete_folter(BUCKET_NAME, filter_table_name):
    """ *** """
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(BUCKET_NAME)
    for key in bucket.objects.filter(Prefix=filter_table_name):
        key.delete()

def control_mallas(db_tabla, schema_tabla,tabla, estado_ejecucion, observacion, aws_region):
    lambda_client = boto3.client('lambda',region_name=aws_region)
    dict_input = {
        "NOMBRE_BD": db_tabla,
        "ESQUEMA_BD": schema_tabla,
        "NOMBRE_TABLA": tabla,
        "ESTADO_EJECUCION": estado_ejecucion,
        "OBSERVACION": observacion
    }
    lambda_params = json.dumps(dict_input)
    response = lambda_client.invoke(FunctionName='UE1COMPRODLMBPAN040_DTL',
                                    InvocationType='RequestResponse',
                                    Payload=lambda_params)
    payload_response = response['Payload']

def insertar_ejecucion_malla(db_tabla, schema_tabla, tabla, aws_region):
    lambda_client = boto3.client('lambda',region_name=aws_region)
    dict_input = {
        "NOMBRE_BD": db_tabla,
        "ESQUEMA_BD": schema_tabla,
        "NOMBRE_TABLA": tabla,
    }
    lambda_params = json.dumps(dict_input)
    response = lambda_client.invoke(FunctionName='UE1COMPRODLMBPAN036',
                                    InvocationType='RequestResponse',
                                    Payload=lambda_params)
    payload_response = response['Payload']
        
def lambda_handler(event, context):
    # TODO implement
    BUCKET_NAME = "ue1stgprodas3dtl001"
    REGION_NAME = "us-east-1"

    athena_client = boto3.client('athena',
                      region_name=REGION_NAME
                      )
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    #raise
    #pprint(event)
    
    key_file = None
    BUCKET_NAME = None
    #sample = "InputSerialization"
    for record in event['Records']:
        BUCKET_NAME = record['s3']['bucket']['name']
        key_file = record['s3']['object']['key']
    print("inti",BUCKET_NAME,key_file)
    #key_file = event['key']
    partition = "idmes" #event['partition']
    

    BUCKET_NAME = "ue1stgprodas3dtl001"
    sample = "RAW/F685H/FULL-LOAD/F685H.TXT"

    dir_, name_file = os.path.split(key_file)
    name, extension = name_file.split(".",1)
    bd, esquema, tabla, periodo = name.split("__")

    insertar_ejecucion_malla("DATALAKE", "DATALAKE", tabla.upper(), REGION_NAME)
    control_mallas("DATALAKE", "DATALAKE", tabla.upper(), "STARTED", "", REGION_NAME)

    input_serialization = {'CSV': {
                "FileHeaderInfo": "USE", 'AllowQuotedRecordDelimiter': True}
            }
    if 'gz' in extension:
        input_serialization['CompressionType'] = 'GZIP'

    resp = s3_client.select_object_content(
            Bucket=BUCKET_NAME,
            Key=key_file,
            ExpressionType='SQL',
            Expression="select * from s3object s limit 1",
            InputSerialization=input_serialization,
            OutputSerialization={'JSON': {}}
        )
    #pprint(resp)
    #InputSerialization = {'CompressionType': 'GZIP', 'JSON': {'Type': 'DOCUMENT'}},
    columns = None
    for event in resp['Payload']:
        #pprint(event)
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            records = json.loads(records)
            #pprint(columns)
            columns = records.keys()
    #pprint(columns)
    table_columns = ''
    cont = 0
    if columns == None:
        return {
            'statusCode': 200,
            'body': json.dumps('Periodo sin informacion ' +  tabla + ' perido ' + str(periodo))
        }
        
    col = ""
    for col in columns:
        cont += 1
        if cont == len(columns):
            table_columns += '`{}` string'.format(col.lower())
        else:
            table_columns += '`{}` string,\n'.format(col.lower())
            
    dir_from = "TMP" + "/"  + dir_ + "/" + periodo
    
    # Mover a un periodo  el archivo
    #TMP/periodo/ .... .csv.gz

    delete_folter(BUCKET_NAME, dir_from)

    key_to = dir_from + "/" + name_file
    copy_source = {
        'Bucket': BUCKET_NAME,
        'Key': key_file
        }
    print(copy_source,BUCKET_NAME, key_to)
    s3_client.copy(copy_source, BUCKET_NAME, key_to)
    #crear la tabla en datalake_ods
    
    SCHEMA_DB_ATHENA = "datalake_ods"
    TABLE_NAME = tabla
    ddl = """
    CREATE EXTERNAL TABLE IF NOT EXISTS `{SCHEMA_DB_ATHENA}`.`{TABLE_NAME}_{PERIODO}_csv`(
     {table_columns})
    ROW FORMAT SERDE 
      'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ( 
      'quoteChar'='\"', 
      'separatorChar'=',') 
    STORED AS INPUTFORMAT 
      'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 
      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
    's3://{BUCKET_NAME}/{DIR_TABLE}'
    TBLPROPERTIES (
    "skip.header.line.count"="1")
    """.format(SCHEMA_DB_ATHENA=SCHEMA_DB_ATHENA, TABLE_NAME=TABLE_NAME,PERIODO=periodo,
        table_columns=table_columns, BUCKET_NAME=BUCKET_NAME, DIR_TABLE=dir_from)
    #TBLPROPERTIES ('compressionType'='gzip')

    ############
    path_temp_get_athena = "s3://{BUCKET_PROCESS}/{FOLDER_TMP}".format(BUCKET_PROCESS=BUCKET_NAME, FOLDER_TMP="JOSE/TMP")
    set_script_athena( REGION_NAME, SCHEMA_DB_ATHENA, ddl, path_temp_get_athena)
    ##############
    order_columns = list(columns)
    
    
    #order_columns.sort(key=partition.__eq__)
    

    join_order_columns = ",".join(order_columns)

    #join_order_columns = join_order_columns + ", date_format(date_parse(fecha, '%d-%b-%y'), '%Y%m') as " + partition #idmes"
    
    ##############

    
    dir_to = "TMP/MATRIX/{TABLE_NAME}/{PARTITION}={PERIODO}".format(TABLE_NAME=TABLE_NAME, PARTITION=partition, PERIODO=periodo)
    BUCKET_NAME = "ue1stgprodas3dtl001"
    external_location = 's3://{BUCKET_NAME}/{DIR_TABLE}'.format(BUCKET_NAME=BUCKET_NAME, DIR_TABLE=dir_to)
    delete_folter(BUCKET_NAME, dir_to)
    create_table = """
    create table {TABLE_NAME}_{PERIODO}_pqt
        with(
        format = 'Parquet',
        parquet_compression = 'SNAPPY',
        external_location = '{external_location}'
        ) as 
        SELECT  {COLUMNS} FROM {TABLE_NAME}_{PERIODO}_csv
    """.format(SCHEMA_DB_ATHENA=SCHEMA_DB_ATHENA, external_location=external_location,PERIODO=periodo,
                BUCKET_NAME=BUCKET_NAME,  COLUMNS=join_order_columns,TABLE_NAME=TABLE_NAME)
    set_script_athena( REGION_NAME, SCHEMA_DB_ATHENA, create_table, path_temp_get_athena)
    #print(TABLE_NAME)
    drop_table_csv = "DROP TABLE {SCHEMA_DB_ATHENA}.{TABLE_NAME}_{PERIODO}_csv ".format(SCHEMA_DB_ATHENA=SCHEMA_DB_ATHENA,PERIODO=periodo, 
                                                                        TABLE_NAME=TABLE_NAME)

    drop_table_pqt = "DROP TABLE {SCHEMA_DB_ATHENA}.{TABLE_NAME}_{PERIODO}_pqt ".format(SCHEMA_DB_ATHENA=SCHEMA_DB_ATHENA,PERIODO=periodo, 
                                                                        TABLE_NAME=TABLE_NAME)
    set_script_athena( REGION_NAME, SCHEMA_DB_ATHENA, 
                                drop_table_csv, path_temp_get_athena)
    set_script_athena( REGION_NAME, SCHEMA_DB_ATHENA, 
                                drop_table_pqt, path_temp_get_athena)
    

    if bd.upper() == "DATA_ENTRY":
        db_schema_x = "datalake" #esquema.lower()
    else:
        db_schema_x = bd.lower() + "_" +   esquema.lower()
    
    query_addpart_table = "ALTER TABLE {0}.{1} ADD IF NOT EXISTS PARTITION ({2}='{3}') location '{4}' ".format(db_schema_x,TABLE_NAME,
                    partition, periodo, external_location)
    set_script_athena( REGION_NAME, db_schema_x, query_addpart_table, path_temp_get_athena)

    control_mallas("DATALAKE", "DATALAKE", tabla.upper(), "FINALIZED", "", REGION_NAME)
    return {
        'statusCode': 200,
        'body': json.dumps('Tabla actualizada ' +  TABLE_NAME + ' perido ' + str(periodo))
    }
