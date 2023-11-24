import configparser 
import boto3 
import redshift_connector

config = configparser.ConfigParser()
config.read('.env')


from utils.helper import create_bucket, load_data_to_s3
from utils.constants import file_paths, file_names
from sql_statements.create import raw_data_tables, transformed_tables
from sql_statements.transform import transformation_queries


['AWS']
access_key = config['AWS']['access_key']
secret_key = config['AWS']['secret_key']
bucket_name = config['AWS']['bucket_name']
region = config['AWS']['region']
role = config['AWS']['role']


['DWH_CONN']
dwh_host = config['DWH_CONN']['host']
dwh_user = config['DWH_CONN']['user']
dwh_password = config['DWH_CONN']['password']
dwh_database = config['DWH_CONN']['database']


s3_path = 's3://{}/{}.csv'


#============ CREATE S3 BUCKET (DATA LAKE)
create_bucket(access_key, secret_key, bucket_name, region)



#============ Load transformed datasets to s3 bucket
for file_path, file_name in zip(file_paths, file_names):
    load_datasets = load_data_to_s3(access_key, secret_key, file_path, file_name, bucket_name)
    print('datasets loaded')



# #============ Create connection to data warehouse using redshift_connector
conn_details = {'host':dwh_host, 'database': dwh_database, 'user':dwh_user, 'password': dwh_password}
dwh_conn = redshift_connector.connect(**conn_details)
print('connection successful')

cursor = dwh_conn.cursor()


dev_schema = 'raw_data'
staging_schema = 'staging'



#========================== Create the dev schema
create_dev_schema = f'CREATE SCHEMA {dev_schema};'
cursor.execute(create_dev_schema)
dwh_conn.commit()
print('created')



#========================== Create the tables for the database in the dev schema
for query in raw_data_tables:
    print(f'----------------------------{query[:50]}')
    cursor.execute(query)
    dwh_conn.commit()



#========================Copy the data from s3 into the s3 bucket
for table in raw_data_tables:
    query = f'''
        copy {dev_schema}.{table} 
        from '{s3_path.format(bucket_name, table)}'
        iam_role '{role}'
        delimiter ','
        ignoreheader 1;
    '''
    cursor.execute(query)
    dwh_conn.commit()



#========================== Create transformed/staging schema
create_staging_schema = f'CREATE SCHEMA {staging_schema};'
cursor.execute(create_staging_schema)
dwh_conn.commit()



#===========================Create the tables for the staging schema
for query in transformed_tables:
    print(f'''------------- {query[:50]}''')
    cursor.execute(query)
    dwh_conn.commit()



#===========================Load data into staging schema
for query in transformation_queries:
    print(f'''------------- {query[:50]}''')
    cursor.execute(query)
    dwh_conn.commit()



#============================= Data quality check
query = 'SELECT COUNT (*) FROM staging.{}'

for table in transformed_tables:
    cursor.execute(query.format(table))
    print(f'Table {table} has {cursor.fetchall()}')

cursor.close()
dwh_conn.close()