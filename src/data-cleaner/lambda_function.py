import boto3
import datetime
import json
import os
import re

S3_BUCKET = os.environ['S3_BUCKET']
S3_RAW_DATA_PREFIX = os.environ['RAW_DATA_PREFIX']
S3_CLEANED_DATA_PREFIX = os.environ['CLEANED_DATA_PREFIX']
SITE_BUCKET = os.environ['SITE_BUCKET']
CHART_DATA_KEY = os.environ['CHART_DATA_KEY']

s3Client = boto3.client('s3')

# Event format
# {
#   "Records": [ {
#     "eventName": "ObjectCreated:Put",
#     ...
#     "s3": {
#       "bucket": {
#         "name": "example-bucket",
#         ...
#       },
#       "object": {
#         "key": "path/to/obj.json"
#         ...

def validate_record(record):
    if record['eventName'] != 'ObjectCreated:Put':
        return False
    if 's3' not in record:
        return False
    s3Record = record['s3']
    if 'bucket' not in s3Record or 'object' not in s3Record:
        return False
    if s3Record['bucket']['name'] != S3_BUCKET:
        return False
    if not s3Record['object']['key'].startswith(S3_RAW_DATA_PREFIX):
        return False
    return True

def validateArticleData(rawArticleData):
    try:
        rawArticleJson = json.loads(rawArticleData)
        if 'headline' not in rawArticleJson or 'main' not in rawArticleJson['headline']:
            print(json.dumps({'invalidArticle': 'No headline'}))
            return False
        if 'pub_date' not in rawArticleJson:
            print(json.dumps({
                'headline': rawArticleJson['headline']['main'],
                'invalidArticle': 'No pub_date'
            }))
            return False
        return True
    except Exception as e:
        message = None
        if hasattr(e, 'message'):
            message = e.message
        print(json.dumps({
            'invalidArticle': rawArticleData,
            'message': message
        }))
        return False

def cleanArticleData(rawArticleData):
    rawArticleJson = json.loads(rawArticleData)

    # Required fields
    cleanedArticleJson = dict()
    cleanedArticleJson['pub_date'] = rawArticleJson['pub_date']
    cleanedArticleJson['headline'] = rawArticleJson['headline']['main']

    # Optional fields
    optionalFields = [
            'section_name',
            'subsection_name',
            'type_of_material',
            'document_type',
            'web_url'
    ]
    for optionalField in optionalFields:
        if optionalField in rawArticleJson:
            cleanedArticleJson[optionalField] = rawArticleJson[optionalField]

    # Useful data from keywords, if present
    if 'keywords' in rawArticleJson:
        for keyword in rawArticleJson['keywords']:
            if keyword['rank'] == 1:
                cleanedArticleJson['top_keyword_type'] = keyword['name']
                cleanedArticleJson['top_keyword_value'] = keyword['value']
            if keyword['name'] == 'persons' and keyword['value'] == 'Trump, Donald J':
                cleanedArticleJson['trump_person_keyword_rank'] = keyword['rank']

    return f"{json.dumps(cleanedArticleJson)}\r\n"

# s3_key will look like json-clean/2021/03/01.json
def update_chart_data(s3_key, article_count):
    match = re.search(r'(\d{4})\/(\d{2})\/(\d{2})', s3_key)
    chart_date = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    get_response = s3Client.get_object(
        Bucket=SITE_BUCKET,
        Key=CHART_DATA_KEY
    )
    existing_chart_data = json.loads(get_response['Body'].read().decode('utf-8'))
    new_data_point = [ chart_date, article_count ]
    inserted = False
    for i in range(len(existing_chart_data['chartData'])):
        existing_data_point = existing_chart_data['chartData'][i]
        item_date = existing_data_point[0]
        if chart_date == item_date:
            print(json.dumps({
                'dataPoint': {
                    'newDataPoint': new_data_point,
                    'overwrittenDataPoint': existing_data_point
                }
            }))
            existing_chart_data['chartData'][i] = new_data_point
            inserted = True
            break
        if chart_date < item_date:
            print(json.dumps({
                'dataPoint': {
                    'newDataPoint': new_data_point,
                    'beforeDataPoint': existing_data_point
                }
            }))
            existing_chart_data['chartData'].insert(i, new_data_point)
            inserted = True
            break
    if not inserted:
        print(json.dumps({
            'dataPoint': {
                'newDataPoint': new_data_point
            }
        }))
        existing_chart_data['chartData'].append(new_data_point)

    print(json.dumps({'chartDataPointCount': len(existing_chart_data['chartData'])}))
    s3Client.put_object(
        Bucket=SITE_BUCKET,
        Key=CHART_DATA_KEY,
        Body=json.dumps(existing_chart_data)
    )
    print(json.dumps({'updatedChartData': CHART_DATA_KEY}))



             

def lambda_handler(event, context):
    print(json.dumps({'inputEvent': event}))
    
    totalArticles = 0
    for record in event['Records']:
        if not validate_record(record):
            print(json.dumps({'skippedRecord': record}))
            continue

        bucket = record['s3']['bucket']['name']
        rawKey = record['s3']['object']['key']
        
        getResponse = s3Client.get_object(
            Bucket=bucket,
            Key=rawKey
        )

        cleanedArticles = [cleanArticleData(rawArticleData.decode('utf-8')) for rawArticleData in getResponse['Body'].iter_lines() if validateArticleData(rawArticleData.decode('utf-8'))]
        cleanedKey = re.sub(f"^{S3_RAW_DATA_PREFIX}", S3_CLEANED_DATA_PREFIX, rawKey)
        print(json.dumps({
            'inputKey': rawKey,
            'outputKey': cleanedKey,
            'articleCount': len(cleanedArticles)
        }))

        cleanedBody = "".join(cleanedArticles)
        s3Client.put_object(
            Bucket=bucket,
            Key=cleanedKey,
            Body=cleanedBody
        )

        totalArticles += len(cleanedArticles)
        update_chart_data(cleanedKey, totalArticles)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'objectCount': len(event['Records']),
            'articleCount': totalArticles
        })
    }

