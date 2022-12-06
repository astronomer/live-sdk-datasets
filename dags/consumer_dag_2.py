from pendulum import datetime
from airflow.models import DAG
from airflow import Dataset
from pandas import DataFrame
from astro import sql as aql
from astro.sql.table import Table

# Define constants for interacting with external systems
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_ORDERS = "orders_table"
SNOWFLAKE_REPORTING = "reporting_table"

# Define datasets
orders_table_dataset = Table(conn_id=SNOWFLAKE_CONN_ID, name=SNOWFLAKE_ORDERS)
reporting_table_dataset = Table(conn_id=SNOWFLAKE_CONN_ID, name=SNOWFLAKE_REPORTING)
#orders_table_dataset = Dataset("astro://snowflake_default@?table=orders_table")
#reporting_table_dataset = Dataset("astro://snowflake_default@?table=reporting_table")

# Define a function for transforming tables to dataframes
@aql.dataframe
def transform_dataframe(df_1: DataFrame, df_2: DataFrame):
    amounts_orders = df_1.loc[:, "amount"]
    amounts_reporting = df_2.loc[:, "amount"]
    print("Total amounts in Orders:", amounts_orders.sum())
    print("Total amounts in Reporting:", amounts_reporting.sum())
    return amounts_orders.sum(), amounts_reporting.sum()

with DAG(
    dag_id="consumer_dag_2",
    start_date=datetime(2019, 1, 1),
    schedule=[orders_table_dataset, reporting_table_dataset],
    catchup=False
):

    # Transform the reporting table into a dataframe
    sum_amounts = transform_dataframe(
        Table(
            name=SNOWFLAKE_ORDERS,
            conn_id=SNOWFLAKE_CONN_ID,
        ),
        Table(
            name=SNOWFLAKE_REPORTING,
            conn_id=SNOWFLAKE_CONN_ID,
        ),
    )