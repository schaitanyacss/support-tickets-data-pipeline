# import required libraries
import json
import boto3
import pandas as pd
import re
import io
import os
import pyarrow as pa
import pyarrow.parquet as pq

# ---------- UTILITY FUNCTIONS ----------

# read log file from s3 bucket as log_data
def read_log_from_s3(bucket, key):
   s3 = boto3.client('s3')
   obj = s3.get_object(Bucket=bucket, Key=key)
   log_data = obj['Body'].read().decode('utf-8')
   return log_data

# save dataframe as parquet file to s3 bucket
def save_parquet_to_s3(df, bucket, key):
   # convert dataframe to parquet format
   table = pa.Table.from_pandas(df, preserve_index=False)
   parquet_buffer = io.BytesIO()
   pq.write_table(table, parquet_buffer)

   # upload to s3
   s3 = boto3.client('s3')
   s3.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())
   print("File saved to s3://" + bucket + "/" + key)


# ---------- LAMBDA FUNCTION ----------
def lambda_handler(event, context):
    
   # read data from bucket
   # get bucket and object key from the s3 event trigger
   record = event['Records'][0]
   bucket_name = record['s3']['bucket']['name']
   input_key = record['s3']['object']['key']
 
   print("Triggered by: s3://" + bucket_name + "/" + input_key)

   # read raw log data
   raw_logs = read_log_from_s3(bucket_name, input_key)

   # parse log file
   entries = [entry.strip() for entry in raw_logs.split("---") if entry != '']

   # define regex pattern to parse log entries
   log_pattern = re.compile(
      r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+'
      r'\[(?P<log_level>[A-Z0-9]+)\]\s+'
      r'(?P<component>[\w\.]+)\s+-\s+'
      r'TicketID=(?P<ticket_id>\w+)\s+'
      r'SessionID=(?P<session_id>\w+)\s*\n'
      r'IP=(?P<ip>[\d\.]+)\s*\|\s*'
      r'ResponseTime=(?P<response_time>\d+ms)\s*\|\s*'
      r'CPU=(?P<cpu>[\d\.]+%)\s*\|\s*'
      r'EventType=(?P<event_type>\w+)\s*\|\s*'
      r'Error=(?P<error>\w+)\s*\n'
      r'UserAgent="(?P<user_agent>[^"]+)"\s*\n'
      r'Message="(?P<message>[^"]*)"\s*\n'
      r'Debug="(?P<debug>[^"]+)"\s*\n'
      r'TraceID=(?P<trace_id>\w+)',
      re.MULTILINE
   )

   parsed_entries = []
   for entry in entries:
      match = log_pattern.search(entry)
      if match:
         parsed_entries.append(match.groupdict())

   # create dataframe and clean data
   df = pd.DataFrame(parsed_entries)

   # data type conversion
   df['timestamp'] = pd.to_datetime(df['timestamp'])
   df['response_time'] = df['response_time'].str.replace('ms', '').astype(int)
   df['cpu'] = df['cpu'].str.replace('%', '').astype(float)
   df['error'] = df['error'].str.lower().map({'true': True, 'false': False})
   df = df.drop(columns='trace_id')

   # data cleansing
   df['log_level'] = df['log_level'].replace({
      'INF0': 'INFO',
      'DEBG': 'DEBUG',
      'warnING': 'WARNING',
      'EROR': 'ERROR'
   })

   # remove duplicate records
   df = df.drop_duplicates()

   print(df.shape)
   print(df.head())

   # upload cleaned data to s3 in processed folder as parquet files
   match = re.search(r'support_logs_(\d{4}-\d{2}-\d{2})\.log', input_key)
   date_str = match.group(1) if match else 'unknown'
   output_key = f"support-logs/processed/support_logs_{date_str}.parquet"
   save_parquet_to_s3(df, bucket_name, output_key)