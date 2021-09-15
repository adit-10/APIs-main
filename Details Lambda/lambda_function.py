import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime
import time
import sys
import logging
import decimal
import pymysql

def lambda_handler(event, context):
    
    print("================Handler===========")
    print(event)
    print("================Handle2r===========")
    outputVar = {}
    try:
        placesAPIRef = event["queryStringParameters"]["placesAPIRef"]
    except Exception as e:
        outputVar["error"] = "Incorrect input:placesApi: "+str(e)
        return { 
        "statusCode": 500,
            "headers": {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            },
            "body": json.dumps(outputVar),
            "isBase64Encoded": False
        }
    user = ""
    try:
        user = event["queryStringParameters"]["user"]
    except Exception as e:
        print(e)
    finalList = getplacesData(placesAPIRef,user)
    outputVar = { 
            "results": finalList,
        }
    print(outputVar)
    return  {
        "statusCode": 200,
        "headers": {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        },
        "body": json.dumps(outputVar, cls=DecimalEncoder),
        "isBase64Encoded": False
    }
        
        

def getplacesData(placesAPIRef,user):
    
    rds_host  = "database-1.cpkxgzx7jurt.us-east-1.rds.amazonaws.com"
    name = "admin"
    password = "abc!12345"
    db_name = "TrendMap"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)

    data = {}
    dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
    table = dynamodb.Table('yelp_data')
    response = table.query(
                        KeyConditionExpression=Key('placesAPIRef').eq(placesAPIRef)
                    )
    if len(response['Items'])>=1:
        item = response['Items'][0]
        data['yelp'] = item
        
    table = dynamodb.Table('Instagram_data')
    response = table.query(
                        KeyConditionExpression=Key('placesAPIRef').eq(placesAPIRef)
                    )
                    
    if len(response['Items'])>=1:
        item = response['Items'][0]
        data['insta'] = item
    if user:
        print("hello")
        with conn.cursor() as cur:
            query = "select count(1) from users_fav inner join users on users_fav.user_id = users.user_id where users.email = '"+str(user)+"' and fav = 1"
            print(query)
            
            
            cur.execute(query)
            
            keys = []
            for column in cur.description:
                keys.append(column[0])
            key_number = len(keys)
            
            for row in cur:
                data['favorite'] = true
                print(row)
    return data
    
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)