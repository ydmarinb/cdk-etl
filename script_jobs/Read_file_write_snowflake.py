import snowflake.connector
# import os
import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel
from snowflake.connector.pandas_tools import write_pandas


class Customers(BaseModel):
    CustomerID: int
    FullName: str
    EmailAddress: str
    PhoneNumber: str
    Address: str
    City: str
    State: str
    PostalCode: str
    Country: str


def validate_df(df):
    customers_data = df.to_dict(orient='records')
    for single_customer in customers_data:
        model = Customers(**single_customer)

def get_secret(secret_name, region_name):

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    return json.loads(secret)
    


### ----------------------------------------------------
input_bucket = 'etl-glue-snowflake-blank'
input_file = 'data_source/data.csv'
input_path = f"s3://{input_bucket}/{input_file}"
output_table_name = 'CUSTOMERS'
secret_name = "secret_snowflake_pwd"
region_name = "us-east-2"
### ----------------------------------------------------


# snowflake_pwd = os.getenv('snowflake_pwd')
secret = get_secret(secret_name, region_name)
snowflake_pwd = secret['snowflake_pwd']

conn_info = {
    'account': 'PKTRUBN-PK64038',
    'user': 'jvalencia',
    'password': snowflake_pwd,
    'warehouse': 'XLAB_JVALENCIA__WH',
    'database': 'DEMO_DB',
    'schema': 'JOTA_SCHEMA',
    'role': 'XLAB_JVALENCIAWH_USAGE',
}

df = pd.read_csv(input_path, dtype={'PostalCode': 'object'})

# df = pd.read_csv('/Users/juan.valencia/Documents/Scripts/Python/Faker/data.csv', dtype={'PostalCode': 'object'})
validate_df(df)

## Transformations
df[['FirstName', 'LastName']] = df.FullName.str.split(' ', expand=True)
df['PhoneNumber'] = df['PhoneNumber'].str.replace(r'^001', '+1', regex=True)
df['PhoneNumber'] = df['PhoneNumber'].str.replace(r'[()-.]', '', regex=True)

columns_reordering = ['CustomerID', 'FirstName', 'LastName', 'EmailAddress', 'PhoneNumber', 'Address', 'City', 'State', 'PostalCode', 'Country']
df = df[columns_reordering]
df.columns = df.columns.str.upper()

conn = snowflake.connector.connect(**conn_info)

success, nchunks, nrows, _ = write_pandas(conn, df, output_table_name)

conn.close()

print('Finished')
