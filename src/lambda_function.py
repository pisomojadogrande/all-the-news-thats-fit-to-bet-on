import boto3
import datetime
import json
import os
import urllib3

API_KEY = os.environ['API_KEY']
S3_BUCKET = os.environ['S3_BUCKET']

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
            s3Key = f"{beginEndDate}-{offset}.json"
            print(json.dumps({
                'date': beginEndDate,
                'pageNum': pageNum,
                'offset': offset,
                'totalHits': totalHits,
                'headlines': headlines,
                's3key': s3Key
            }))

            s3Client.put_object(
                Bucket=S3_BUCKET,
                Key=s3Key,
                Body=response.data
            )

            hasMoreResults = ((offset + len(headlines)) < totalHits)
            pageNum += 1
        else:
            print(json.dumps({
                'date': beginEndDate,
                'pageNum': pageNum,
                'errorCode': statusCode
            }))
            hasMoreResults = False
        
    return {
        'statusCode': statusCode,
        'body': json.dumps({"totalHits": totalHits})
    }
