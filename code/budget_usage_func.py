#!/usr/bin/env python
# coding: utf-8

import logging
import json
import boto3
import datetime 
import pandas as pd 
import awswrangler as wr

#S3 bucket location for cleaned data
s3_location = 's3://aaa-api-bucket/'

def create_df():
    df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in {'advertiser_id': 'str', 
                                                              'marketplace_id': 'str', 
                                                              'dataset_id': 'str', 
                                                              'budget_scope_id': 'str', 
                                                              'budget_scope_type': 'str', 
                                                              'advertising_product_type': 'str', 
                                                              'budget': 'float', 
                                                              'budget_usage_percentage': 'float', 
                                                              'usage_updated_timestamp': 'str'}.items()})
    return df

def lambda_handler(event, context):
    df = create_df()
    print(event)
    
    for record in event['Records']:
        payload = record["body"]
        df_msg = pd.json_normalize(json.loads(payload))
        df = pd.concat([df, df_msg], ignore_index=True)
        df
    
    df = df.drop_duplicates()

    year = str(datetime.datetime.now().year)
    month = str(datetime.datetime.now().month)
    day = str(datetime.datetime.now().day)

    df.insert(len(df.columns) ,'year', year)
    df.insert(len(df.columns) ,'month', month)
    df.insert(len(df.columns) ,'day', day)

    databases = wr.catalog.databases()

    budget_usage_db = 'stream_budget_usage'
    budget_usage_table = 'budget_usage'

    if budget_usage_db not in databases.values:
        wr.catalog.create_database(budget_usage_db)
        logger.info("created stream_budget_usage database")

    wr.s3.to_parquet(
        df=df,
        path=s3_location,
        dataset=True,
        database=budget_usage_db,
        table=budget_usage_table,
        partition_cols=['year','month','day']
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Completed!')
    }
