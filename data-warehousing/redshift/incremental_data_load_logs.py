import psycopg2
import os

# ---------- .ENV ----------
# Redshift Serverless configuration
REDSHIFT_HOST = os.getenv("host")
REDSHIFT_PORT = os.getenv("port")
REDSHIFT_DATABASE = 'careplus_db'
REDSHIFT_USER = os.getenv("user")
REDSHIFT_PASSWORD = os.getenv("pass")
REDSHIFT_TABLE = 'public.support_logs'
REDSHIFT_REGION = os.getenv("region")
IAM_ROLE = os.getenv("IAM_role")

# ---------- LAMBDA FUNCTION ----------
def lambda_handler(event, context):
   # 1: read data from the bucket
   # Get bucket and object key from the S3 event trigger
   record = event['Records'][0]
   bucket_name = record['s3']['bucket']['name']
   input_key = record['s3']['object']['key']

   print(f"📥 Triggered by: s3://{bucket_name}/{input_key}")

   s3_input_path = f's3://{bucket_name}/{input_key}'

   # Connect to Redshift Serverless using psycopg2
   conn = psycopg2.connect(
      host=REDSHIFT_HOST,
      port=REDSHIFT_PORT,
      dbname=REDSHIFT_DATABASE,
      user=REDSHIFT_USER,
      password=REDSHIFT_PASSWORD
   )

   cursor = conn.cursor()

   # COPY SQL query to load data from S3 into the Redshift table
   copy_sql = f"""
      COPY {REDSHIFT_TABLE}
      FROM '{s3_input_path}'
      IAM_ROLE '{IAM_ROLE}'
      FORMAT AS PARQUET
      REGION {REDSHIFT_REGION};
      """

   # Execute the query
   cursor.execute(copy_sql)

   # Commit the changes (important for COPY operations)
   conn.commit()

   # Log success
   print(f"Data successfully copied from {s3_input_path} to {REDSHIFT_TABLE}")


   # Close the cursor and the connection
   cursor.close()
   conn.close()