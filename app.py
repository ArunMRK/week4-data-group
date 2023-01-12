from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import psycopg2
import dotenv, os

app = Dash(__name__)

colours = {
    'background_page': '#1F0660',
    'background_boxes': '#2C1565',
    'title_text': '#A6A2B2',
    'text': '#E5E2ED'
}

dotenv.load_dotenv(override = True)

AWS_ENDPOINT = os.environ['AWS_ENDPOINT']
AWS_PORT = os.environ['AWS_PORT']
AWS_USERNAME = os.environ['AWS_USERNAME']
AWS_PASSWORD = os.environ['AWS_PASSWORD']
AWS_DATABASE = os.environ['AWS_DATABASE']

AWS_PRODUCTION_SCHEMA = 'week4_alex_production'
AWS_PRODUCTION_TABLE = 'production_ecommerce'

PRODUCTION_DATAFRAME_COLUMNS = ["order_number", "toothbrush_type", "order_date", "customer_age",
    "order_quantity", "delivery_postcode", "billing_postcode",
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

def execute_query(rds_connection, schema_name=AWS_PRODUCTION_SCHEMA, table_name=AWS_PRODUCTION_TABLE):
    cur = rds_connection.cursor()
    cur.execute(f"""
        SELECT * FROM {schema_name}.{table_name}
    """)
    return cur.fetchall()



"""Dashboard work"""

def age_distribution(df, toothbrush:str):
    """Bin to dataframe, english sentence to explain what this function/code is doing, called docstrings, for every function""" 
    if toothbrush == "Both":
        df_toothbrush = df
    else:
        df_toothbrush = df[(df["toothbrush_type"] == f"{toothbrush}")]
        
    BIN_PARTITION = range(0, 120, 5)

    age_ranges = ["0-5", "5-10", "10-15", "15-20", "20-25", "25-30", "30-35", "35-40",
    "40-45", "45-50", "50-55", "55-60", "60-65", "65-70", "70-75", "75-80",
    "80-85", "85-90", "90-95", "95-100", "100-105", "105-110", "110-115"
    ]

    df_toothbrush["age_range"] = pd.cut(df_toothbrush['customer_age'], bins=BIN_PARTITION, labels=age_ranges)
    df_test = df_toothbrush.groupby('age_range').count().reset_index()

    return df_test[['age_range', 'order_date']]

def time_distribution(df, toothbrush:str):
    if toothbrush == "Both":
        DF = df
    else:
        DF = df[(df["toothbrush_type"] == f"{toothbrush}")]

    time_ranges = ['12am-\n 3am', '3am-\n 6am', '6am-\n 9am', '9am-\n 12pm',
    '12pm-\n 3pm', '3pm-\n 6pm', '6pm-\n 9pm', '9pm-\n 12am']

    DF["hour"] = DF["order_date"].dt.hour
    DF["time_range"] = pd.cut(DF['hour'], bins=8, labels=time_ranges)
    DF = DF.groupby("time_range").count().reset_index()

    return DF[['time_range', 'order_date']]

def toothbrush_data(df, product_name):
    return df[(df["toothbrush_type"] == f"{product_name}")]

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

        df = pd.DataFrame(results)

        df.columns = PRODUCTION_DATAFRAME_COLUMNS
        #df.columns = STAGING_DATAFRAME_COLUMNS

        """Time to run the dashboard"""

        #print(df)

    except Exception as e:
        print(f"Database could not connect: {e}")
    return df
    return "Success!"

event = {}
context = {}
df = lambda_handler(event, context)

app.layout = html.Div(
    style={'backgroundColor': colours['background_page']},
    children=[
        html.H1(
            children="Dashboard summary page - Toothbrush XYZ",
            style={
                'textAlign': 'center',
                'color': colours['title_text']
            }
        ),
        html.H2(
            children='Quick sentence - This page highlights all key findings',
            style={
                'textAlign': 'center',
                'color': colours['title_text']
            }
        ),
        html.Div(
            children=[
                html.Div(
                    style={'backgroundColor': colours['background_page']},
                    children = [
                        dcc.Graph(
                            id='age-distribution',
                            style={
                                'display': 'inline-block'
                            }
                        ),
                        dcc.Graph(
                            id='time-distribution',
                            style={
                                'display': 'inline-block'
                            }
                        )
                    ]
                ),
                html.Div(
                    style={'backgroundColor': colours['background_page']},
                    children=[
                        dcc.Dropdown(
                            ["Toothbrush 2000", "Toothbrush 4000", "Both"],
                            "Toothbrush 2000",
                            id="toothbrush-model"
                        )
                    ]
                )    
            ]
        ),
        
        html.Div(
            style={'backgroundColor': colours['background_page']},
            children=[
                html.Div(
                    [
                        dcc.Dropdown(
                            df['toothbrush_type'].unique(),
                            "Toothbrush 2000",
                            id='data-input'
                        ),
                    ]
                ),
            dcc.Graph(id='graph-output'),
        ]),
        html.Div(
            children=[
                html.Div(
                    children = [

                    ]
                )
            ]
        )
    ]
)

@app.callback(
    Output("age-distribution", "figure"),
    Input("toothbrush-model", "value")
)
def change_model_age(input_value, dataframe = df):
    condensed_df = age_distribution(dataframe, input_value)
    if input_value == "Toothbrush 2000":
        return px.bar(condensed_df, x="age_range", y="order_date", title="Test")
    elif input_value == "Both":
        return px.bar(condensed_df, x="age_range", y="order_date")
    return px.bar(condensed_df, x="age_range", y="order_date")

@app.callback(
    Output("time-distribution", "figure"),
    Input("toothbrush-model", "value")
)
def change_model_time(input_value, dataframe = df):
    condensed_df = time_distribution(dataframe, input_value)
    if input_value == "Toothbrush 2000" or input_value == "Both":
        return px.bar(condensed_df, x="time_range", y="order_date")
    elif input_value == "Both":
        return px.bar(condensed_df, x="age_range", y="order_date")
    return px.bar(condensed_df, x="time_range", y="order_date")

@app.callback(
    Output('graph-output','figure'),
    Input('data-input','value')
)
def update_graph(input_value):
    data = toothbrush_data(df, input_value)
    return px.bar(data, x="customer_age", y="order_quantity", barmode="group")


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", debug=True, port=8080)