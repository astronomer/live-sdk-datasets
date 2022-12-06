from pendulum import datetime
from airflow.models import DAG
from airflow import Dataset
from pandas import DataFrame
from astro import sql as aql
from astro.sql.table import Table

# Define constants for interacting with external systems
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_ORDERS = "orders_table"

# Define datasets
orders_table_dataset = Table(conn_id=SNOWFLAKE_CONN_ID, name=SNOWFLAKE_ORDERS)
#orders_table_dataset = Dataset("astro://snowflake_default@?table=orders_table")

# Define a function for transforming tables to dataframes
@aql.dataframe
def transform_dataframe(df: DataFrame):
    amounts = df.loc[:, "amount"]
    print("Total amount:", amounts.sum())
    return amounts.sum()

with DAG(
    dag_id="consumer_dag_1",
    start_date=datetime(2019, 1, 1),
    schedule=[orders_table_dataset],
    catchup=False,
):

    # Transform the reporting table into a dataframe
    sum_orders = transform_dataframe(
        Table(
            name=SNOWFLAKE_ORDERS,
            conn_id=SNOWFLAKE_CONN_ID,
        )
    )