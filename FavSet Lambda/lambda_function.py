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
        placesAPIRef = event["queryStringParameters"]["placesAPIRef"]
        isFav = event["queryStringParameters"]["isFav"]
    except Exception as e:
        outputVar["error"] = "Incorrect input:did not find user or placesAPIRef to mark: "+str(e)
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
        
    userID = doesUserExist(user,conn,1)
    status = markAsFav(userID,user,placesAPIRef,isFav,conn)
    outputVar = { 
            "user": userID,
            "markAsFav": isFav,
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
        
        
        
#Check if user exists if not then create 
def doesUserExist(user,conn,call):
    userId = ""
    with conn.cursor() as cur:
        query = "select user_id from users where users.email = '"+str(user)+"'"
        print(query)
        
        
        cur.execute(query)
        
        keys = []
        for column in cur.description:
            keys.append(column[0])
        key_number = len(keys)
        for row in cur:
            print(row)
            userId = str(row[0])
            print(userId)
        if not userId:
            query = "INSERT INTO users (`name`, `email`) VALUES ('dummy', '"+user+"');"
            print(query)
            
            cur.execute(query)
            print("====================")
            print(cur)
            print("====================")
            conn.commit();
            if call == 1:
                userId = doesUserExist(user,conn,2)
            
    return userId
#mark as favorite
def markAsFav(userID,user,placesAPIRef,isFav,conn):
    userId = ""
    with conn.cursor() as cur:
            query = "INSERT INTO users_fav (`user_id`, `placesAPIRef`, `isfav`) VALUES ('"+userID+"','"+placesAPIRef+"','"+isFav+"');"
            print(query)
            cur.execute(query)
            print("2====================")
            print(cur)
            print("2====================")
            
    return userId