import json
import boto3
import os
import time
import cfnresponse

CATALOG = os.environ['CATALOG']
DATABASE_NAME = os.environ['DATABASE_NAME']
TABLE_NAME = os.environ['TABLE_NAME']
ATHENA_OUTPUT_BUCKET = os.environ['ATHENA_OUTPUT_BUCKET']
ATHENA_OUTPUT_PREFIX = os.environ['ATHENA_OUTPUT_PREFIX']
TABLE_DATA_LOCATION = os.environ['TABLE_DATA_LOCATION']
POLL_INTERVAL_SEC = int(os.environ['POLL_INTERVAL_SEC'])


def execute_and_wait(athena_client, q):
    response = athena_client.start_query_execution(
        QueryString=q,
        QueryExecutionContext={
            'Database': f"`{DATABASE_NAME}`",
            'Catalog': CATALOG
        },
        ResultConfiguration={
            'OutputLocation': f"s3://{ATHENA_OUTPUT_BUCKET}/{ATHENA_OUTPUT_PREFIX}"
        }
    )
    query_execution_id = response['QueryExecutionId']
    print(f"{query_execution_id}: {q}")
    
    is_executing = True
    while is_executing:
        time.sleep(POLL_INTERVAL_SEC)
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        query_status = response['QueryExecution']['Status']
        query_state = query_status['State']
        print(f"{query_execution_id}: {query_state}")
        if query_state != 'QUEUED' and query_state != 'RUNNING':
            is_executing = False
        if 'StateChangeReason' in query_status:
            print(f"{query_execution_id}: Reason ${query_status['StateChangeReason']}")
            
    if query_state == 'SUCCESS':
        response = athena_client.get_query_result(
            QueryExecutionId=query_execution_id
        )
        print(f"{query_execution_id} success: ${response}")
        return True
        
    if query_state == 'FAILED':
        raise Exception(f"Query '{q}' failed")

def handler(event, context):
    request_type = event['RequestType']
    print(event)
    athena_client = boto3.client('athena')
    
    cfn_status = cfnresponse.FAILED
    cfn_reason = None
    try:
        drop_table_query = f"DROP TABLE IF EXISTS `{TABLE_NAME}`"
        execute_and_wait(athena_client, drop_table_query)
        
        if request_type == 'Create' or request_type == 'Update':
            create_table_query = f"CREATE EXTERNAL TABLE `{TABLE_NAME}` (pub_date string, headline string, relevance_ranking int, section_name string, subsection_name string, web_url string, type_of_material string, document_type string, word_count int) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' LOCATION '{TABLE_DATA_LOCATION}'" 
            execute_and_wait(athena_client, create_table_query)
    
        cfn_status = cfnresponse.SUCCESS
    except Exception as e:
        print(f"Error: {e}")
        cfn_status = cfnresponse.FAILED
        cfn_reason = f"{e}"
        
    cfnresponse.send(event, context, cfn_status, None, 'AthenaTableCustomResource', reason=cfn_reason)
