import awswrangler as wr
import boto3

my_sns_arn = 'arn:aws:sns:us-east-1:XXXX:ads-stream-sns'


def publish_to_sns(sub, msg):
    topic_arn = my_sns_arn
    sns = boto3.client("sns")
    response = sns.publish(
        TopicArn=topic_arn,
        Message=msg,
        Subject=sub
    )
    
def lambda_handler(event, context):
    
    # search CPC value
    
    df = wr.athena.read_sql_query('''
        SELECT campaign_id, 
            SUM(cost) AS cost, 
            SUM(impressions) AS impressions, 
            SUM(clicks) AS clicks,
            SUM(cost) / SUM(clicks)   AS cpc
        FROM 
            sp_traffic
        WHERE 
            match_type IN('TARGETING_EXPRESSION_PREDEFINED','TARGETING_EXPRESSION') 
            AND month = cast(extract(MONTH from CURRENT_DATE) as varchar)
            AND cast(from_iso8601_timestamp(time_window_start) as date) >  date_add('day',-5,current_date) 
        GROUP BY 
            campaign_id
        HAVING
            SUM(cost) / SUM(clicks) > 0.6''', database="stream_sp_traffic")
        
    if not df.empty:
        sub = "SP Report"
        response = publish_to_sns(sub,str(df.to_dict('records')))
        return {
        'statusCode': 200
    }
    
    return {
        'statusCode': 404,
        'body': "EMPTY"
    }
