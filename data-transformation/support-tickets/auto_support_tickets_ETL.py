import json
import boto3

glue = boto3.client('glue')

# ---------- LAMBDA FUNCTION ----------
def lambda_handler(event, context):
   print(f"Received_event: {json.dumps(event)}")

   if 'Records' not in event:
      raise ValueError("Invalid event structure")

   bucket_name = event['Records'][0]['s3']['bucket']['name']
   input_key = event['Records'][0]['s3']['object']['key']

   s3_input_path = f"s3://{bucket_name}/{input_key}"

   print(f'New file detected: {s3_input_path}, Triggering Glue job')

   try:
      glue.start_job_run(
         # glue job name and input file path
         JobName='ETL_support_tickets_auto',
         Arguments = {
               '--input_file_path': s3_input_path
         }
      )
   except Exception as e:
      print(f"error starting Glue job: {str(e)}")
      raise e