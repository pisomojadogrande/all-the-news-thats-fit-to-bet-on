import boto3
import datetime
import json
import os
import re

S3_BUCKET = os.environ['S3_BUCKET']
S3_RAW_DATA_PREFIX = os.environ['RAW_DATA_PREFIX']
S3_CLEANED_DATA_PREFIX = os.environ['CLEANED_DATA_PREFIX']

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
             

def lambda_handler(event, context):
    print(json.dumps({'inputEvent': event}))
    
    s3Client = boto3.client('s3')
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
    return {
        'statusCode': 200,
        'body': json.dumps({
            'objectCount': len(event['Records']),
            'articleCount': totalArticles
        })
    }

