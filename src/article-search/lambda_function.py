import boto3
import datetime
import json
import os
import time
import urllib3

API_KEY = os.environ['API_KEY']
S3_BUCKET = os.environ['S3_BUCKET']
S3_PREFIX = os.environ['RAW_DATA_PREFIX']

def lambda_handler(event, context):

    print(json.dumps({'inputEvent': event}))
    eventTime = datetime.datetime.strptime(event['time'], "%Y-%m-%dT%H:%M:%SZ")
    yesterday = eventTime - datetime.timedelta(hours=24)
    beginEndDate = datetime.datetime.strftime(yesterday, "%Y%m%d")

    http = urllib3.PoolManager()
    requestUrlFmt = "https://api.nytimes.com/svc/search/v2/articlesearch.json?begin_date={}&end_date={}&facet=true&facet_fields=section_name&page={}&q=Trump&api-key={}"
    requestHeaders = {
        "Accept": "application/json"
    }

    s3Client = boto3.client('s3')

    pageNum = 0
    hasMoreResults = True
    statusCode = 200
    allDocs = []

    while hasMoreResults:    
        requestUrl = requestUrlFmt.format(beginEndDate, beginEndDate, pageNum, API_KEY)
        
        response = http.request('GET', requestUrl, headers=requestHeaders)
        
        statusCode = int(response.status)
        
        if statusCode == 200:
            responseObj = json.loads(response.data.decode('utf-8'))
            meta = responseObj['response']['meta']
            totalHits = int(meta['hits'])
            offset = int(meta['offset'])
            headlines = [doc['headline']['main'] for doc in responseObj['response']['docs']]
            print(json.dumps({
                'date': beginEndDate,
                'pageNum': pageNum,
                'offset': offset,
                'totalHits': totalHits,
                'headlines': headlines
            }))

            if 'docs' in responseObj['response'] and len(responseObj['response']['docs']) > 0:
                docRows = [f"{json.dumps(doc)}\r\n" for doc in responseObj['response']['docs']]
                allDocs.extend(docRows)     

            hasMoreResults = ((offset + len(headlines)) < totalHits)
            pageNum += 1

            if hasMoreResults:
                time.sleep(6)

        else:
            print(json.dumps({
                'date': beginEndDate,
                'pageNum': pageNum,
                'errorCode': statusCode
            }))
            hasMoreResults = False
        

    s3Key = f"{S3_PREFIX}{datetime.datetime.strftime(yesterday, '%Y/%m/%d')}.json"
    body = "".join(allDocs)
    print(json.dumps({
        's3Bucket': S3_BUCKET,
        's3Key': s3Key,
        'size': len(body)
    }))
    s3Client.put_object(
        Bucket=S3_BUCKET,
        Key=s3Key,
        Body=body
    )

    return {
        'statusCode': statusCode,
        'body': json.dumps({"totalHits": totalHits})
    }
