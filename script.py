"""
    Main objectives are as follows:
        1. Read from the staging database
        2. Clean this data
        3. Send cleaned data into the production database
"""
import dotenv, os
import boto3
import psycopg2
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

dotenv.load_dotenv(override = True)

AWS_ENDPOINT = os.environ['AWS_ENDPOINT']
AWS_PORT = os.environ['AWS_PORT']
AWS_USERNAME = os.environ['AWS_USERNAME']
AWS_PASSWORD = os.environ['AWS_PASSWORD']
AWS_DATABASE = os.environ['AWS_DATABASE']

AWS_STAGING_SCHEMA = 'week4_alex_staging'
AWS_STAGING_TABLE = 'staging_ecommerce'

AWS_PRODUCTION_SCHEMA = 'week4_alex_production'
AWS_PRODUCTION_TABLE = 'production_ecommerce'

DATAFRAME_COLUMNS = ["order_number", "toothbrush_type", "order_date", "customer_age",
    "order_quantity", "delivery_postcode", "billing_postcode", "is_first",
    "dispatch_status", "dispatched_date", "delivery_status", "delivery_date"
]

def connection(host=AWS_ENDPOINT, port=AWS_PORT, database=AWS_DATABASE, user=AWS_USERNAME, password=AWS_PASSWORD):
    return psycopg2.connect(
        host = host,
        port = port,
        database = database,
        user = user,
        password = password,
    )

def execute_query(rds_connection, schema_name=AWS_STAGING_SCHEMA, table_name=AWS_STAGING_TABLE):
    cur = rds_connection.cursor()
    cur.execute(f"""
        SELECT * FROM {schema_name}.{table_name}
    """)
    return cur.fetchall()

"""Panda cleaning of the dataframe"""
def columns_to_datetime(dataframe):
    datetime_columns = [col for col in dataframe.columns if 'date' in col]
    for column in datetime_columns:
        dataframe[column] = pd.to_datetime(dataframe[column], errors = 'coerce')
    return f"Columns {datetime_columns} now in datetime"

"""First capitalise every letter in the postcode"""
def postcode_correction(dataframe):
    postcode_columns = ["delivery_postcode", "billing_postcode"]
    for postcodes in postcode_columns:
        #dataframe[postcodes] = map(lambda x: str(x).upper(), dataframe[postcodes])
        dataframe[postcodes] = dataframe[postcodes].apply(lambda x : x.upper())
    return "Postcodes capitalised"

def create_future_date():
    return datetime.strptime('Dec 31 2099 12:00AM', '%b %d %Y %I:%M%p')

def make_connection(db_uri):
    try:
        engine = create_engine(db_uri)
        return engine
    except:
        raise Exception("Could not connect to database and create engine")

def create_table(database_engine, dataframe, table_name = "production_ecommerce"):
    try:
        return dataframe.to_sql(con = database_engine, name = table_name, schema = AWS_PRODUCTION_SCHEMA, if_exists = "replace", index = False)
    except:
        raise Exception("Already created table")

def view_table(database_engine, schema_table_name):
    result = database_engine.execute(f'SELECT * FROM {schema_table_name}')
    print(result, "Successfully uploaded table")
    i = 0
    for r in result:
        if i == 5:
            break
        print(r)
        i += 1

def lambda_handler(event, context):
    try:
        """Read from staging database"""
        conn = connection()
        if conn is not None:
            print(f"Connected to {AWS_ENDPOINT}")

        results = execute_query(conn)

        if results is not None:
            print("Successfully retrieved table")
        else:
            print("Empty!")

        """Convert result into Dataframe, to be cleaned"""
        """Bit of a cheat but declare column names myself"""

        df = pd.DataFrame(results)
        df.columns = DATAFRAME_COLUMNS

        """Cleaning process and reformatting"""
        columns_to_datetime(df)
        postcode_correction(df)

        """Dropped 'is_first' column since all equal to 1"""
        df = df.drop('is_first', axis = 1)

        """How to deal with null values"""
        df["delivery_status"] = df["delivery_status"].fillna("In Process")
        future_date = create_future_date()
        df["delivery_date"] = df["delivery_date"].fillna(future_date)

        """Upload into the production schema"""
        DB_URI = f"postgresql+psycopg2://{AWS_USERNAME}:{AWS_PASSWORD}@{AWS_ENDPOINT}:{AWS_PORT}/{AWS_DATABASE}"
        production_engine = make_connection(DB_URI)

        create_table(production_engine, df)
        view_table(production_engine, f'{AWS_PRODUCTION_SCHEMA}.{AWS_PRODUCTION_TABLE}')

    except Exception as e:
        print(f"Database could not connect: {e}")

    return "Success!"

event, context = {}, {}
print(lambda_handler(event, context))