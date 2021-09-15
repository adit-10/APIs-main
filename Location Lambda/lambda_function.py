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
        lat = float(event["queryStringParameters"]["lat"])
            
        long = float(event["queryStringParameters"]["long"])
    except Exception as e:
        outputVar["error"] = "Incorrect input:lat or long issues: "+str(e)
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
    
    paramBounds = findLatLongBounds(lat,long)
    finalList = findLocations(paramBounds)
    outputVar = { 
            "boundsChosen":paramBounds,
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
        
        
   
#used to find left and right most lat and long
def findLatLongBounds(lat,long):
    movVar = 0.007
    startLat = lat - movVar
    startLong = long - movVar
    endLat = lat + movVar
    endLong = long + movVar
    paramBounds = {
        "startLat":startLat,
        "startLong":startLong,
        "endLat":endLat,
        "endLong":endLong
    }
    
    return paramBounds
      

def findLocations(paramBounds):
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
        minLat = paramBounds["endLat"] if paramBounds["startLat"]>paramBounds["endLat"] else paramBounds["startLat"]
        maxLat = paramBounds["endLat"] if paramBounds["startLat"]<=paramBounds["endLat"] else paramBounds["startLat"]
        
        minLong = paramBounds["endLong"] if paramBounds["startLong"]>paramBounds["endLong"] else paramBounds["startLong"]
        maxLong = paramBounds["endLong"] if paramBounds["startLong"]<=paramBounds["endLong"] else paramBounds["startLong"]
        
        query = "select Places.*,Place_to_type.types from Places left outer join (SELECT placesAPIRef , GROUP_CONCAT(DISTINCT Place_to_type.types separator '|') as 'types' FROM Place_to_type GROUP BY placesAPIRef) Place_to_type on Place_to_type.placesAPIRef = Places.placesAPIRef where lat BETWEEN "+str(minLat)+" and "+str(maxLat)+" and Places.long BETWEEN "+str(minLong)+" and "+str(maxLong)
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
