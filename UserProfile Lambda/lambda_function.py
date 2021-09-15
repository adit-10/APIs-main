##Ading comment for Advanced Software Engineering HW0

import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime
import time
import pymysql
import sys
import logging

def lambda_handler(event, context):
    
    print("================Handler===========")
    print(event)
    print("================Handle2r===========")
    outputVar = {}
    try:
        user = event["queryStringParameters"]["user"]
    except Exception as e:
        outputVar["error"] = "Incorrect input:did not find user to fetch for: "+str(e)
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
    
    finalList = findListOfFav(user)
    outputVar = { 
            "results": finalList,
        }
    return  {
        "statusCode": 200,
        "headers": {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        },
        "body": json.dumps(outputVar),
        "isBase64Encoded": False
    }
        
        
#find the list of favorites
def findListOfFav(user):
    resultList = []
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
        sys.exit()

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    item_count = 0

    with conn.cursor() as cur:
        query = "select Places.* from Places inner join (SELECT placesAPIRef FROM users_fav inner join users on users_fav.user_id = users.user_id where users.email = '"+str(user)+"') users_fav on users_fav.placesAPIRef = Places.placesAPIRef"
        print(query)
        
        
        cur.execute(query)
        
        keys = []
        for column in cur.description:
            keys.append(column[0])
        key_number = len(keys)
        
        for row in cur:
            item_count += 1
            logger.info(row)
            item = dict()
            json_data=[]
            for q in range(key_number):
                item[keys[q]] = str(row[q])
            json_data.append(item)

            resultList.append(item)
            print(row)
    
    print(resultList)
    
    return resultList
