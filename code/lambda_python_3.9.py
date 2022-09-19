#!/usr/bin/env python
# coding: utf-8

import logging
import json
import boto3
from botocore.exceptions import ClientError
import pandas as pd 
import awswrangler as wr
import datetime

logger = logging.getLogger(__name__)
client = boto3.client('sqs')

#SQS queue url
queueUrl = 'https://sqs.us-east-1.amazonaws.com/521981389199/sp-traffic-mock'

#Max batch size for a sqs call
max_num = 10

#A period of time during which Amazon SQS prevents other consumers from receiving and processing the message
visibilityTimeout = 90

#A period of time call waits for a message to arrive in the queue before returning
waitTimeSeconds = 0

#S3 bucket location for cleaned data
s3_location = 's3://aaa-api-bucket/'


# Retrieve Data from SQS
def receive_messages(queueUrl, max_num, visibilityTimeout, waitTimeSeconds):
    try:
        messages = client.receive_message(
            QueueUrl = queueUrl,
            MessageAttributeNames = ['All'],
            MaxNumberOfMessages = max_num,
            VisibilityTimeout = visibilityTimeout,
            WaitTimeSeconds = waitTimeSeconds
        )
    except ClientError as error:
        logger.exception("Couldn't receive messages from queue: %s", queueUrl)
        raise error
    else:
        return messages

# Common logic - Unpack Data 
def unpack_messages(messages):
    message_bodys = []
    try: 
        for msg in messages['Messages']:
            message_bodys.append(msg['Body'])
    except:
        pass
    return message_bodys

def check_queue_empty(queueUrl):
    response = client.get_queue_attributes(
    QueueUrl= queueUrl,
    AttributeNames=[
        'ApproximateNumberOfMessages'
        ]
    )
    return int(response['Attributes']['ApproximateNumberOfMessages'])

def delete_messages(queueUrl, messages, flag):
    try:
        entries = [{
            'Id': str(ind),
            'ReceiptHandle': msg['ReceiptHandle']
        } for ind, msg in enumerate(messages['Messages'])]
        response = client.delete_message_batch(QueueUrl=queueUrl,Entries=entries)
        if 'Successful' in response:
            for msg_meta in response['Successful']:
                logger.info("Deleted %s", messages['Messages'][int(msg_meta['Id'])]['ReceiptHandle'])
        if 'Failed' in response:
            for msg_meta in response['Failed']:
                logger.warning(
                    "Could not delete %s",
                    messages['Messages'][int(msg_meta['Id'])]['ReceiptHandle']
                )
        return True
    except:
        if check_queue_empty(queueUrl) == 0:
            logger.info("Queue is empty now.")
            return False
        else:
            delete_messages(queueUrl, messages, flag)

def create_df(messages):
    try:
        if json.loads(messages[0][0])['dataset_id'] == 'sp-traffic':
            df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in {'idempotency_id': 'str', 
                                                              'dataset_id': 'str', 
                                                              'marketplace_id': 'str', 
                                                              'currency': 'str', 
                                                              'advertiser_id': 'str', 
                                                              'campaign_id': 'str', 
                                                              'ad_group_id': 'str', 
                                                              'ad_id': 'str', 
                                                              'keyword_id': 'str', 
                                                              'keyword_text': 'str', 
                                                              'match_type': 'str', 
                                                              'placement': 'str', 
                                                              'time_window_start': 'str', 
                                                              'clicks': 'int', 
                                                              'impressions': 'int', 
                                                              'cost': 'float'}.items()})
    except:
        return None
    # TODO other stream type
    #if json.loads(messages[0])['dataset_id'] == 'sp-conversion':
    #if json.loads(messages[0])['dataset_id'] == 'budget-usage':
    return df

def lambda_handler(event, context):
 
    more_messages = True
    messages = []
    while more_messages:
        sqs_receive_messages = receive_messages(queueUrl, max_num, visibilityTimeout, waitTimeSeconds)
        messages.append(unpack_messages(sqs_receive_messages))

        if sqs_receive_messages:
            more_messages = delete_messages(queueUrl, sqs_receive_messages, more_messages)  

        else:
            more_messages = False
    
    df = create_df(messages)
    if not df:
        return {    'statusCode': 400,
                    'body': json.dumps('SQS is Empty!') }
    
    for msg in messages:
        for x_msg in msg:
            to_json = json.loads(x_msg)
            try : 
                if to_json['time_window_start']:
                    df_msg = pd.json_normalize(json.loads(x_msg))
                    df = pd.concat([df, df_msg], ignore_index=True)
            except:
                pass

    df = df.drop_duplicates()

    year = datetime.datetime.now().year
    month = datetime.datetime.now().month
    day = datetime.datetime.now().day

    df.insert(len(df.columns) ,'year', year)
    df.insert(len(df.columns) ,'month', month)
    df.insert(len(df.columns) ,'day', day)

    databases = wr.catalog.databases()

    sp_traffic_db = 'stream-sp-traffic'
    sp_traffic_table = 'sp-traffic'

    if sp_traffic_db not in databases.values:
        wr.catalog.create_database(sp_traffic_db)
        print(wr.catalog.databases())
    else:
        print("Database stream-sp-traffic already exists")

    wr.s3.to_parquet(
        df=df,
        path=s3_location,
        dataset=True,
        database=sp_traffic_db,
        table=sp_traffic_table,
        partition_cols=['year','month','day']
    )

#    client.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Completed!')
    }

