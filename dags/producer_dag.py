from pendulum import datetime
from airflow.models import DAG
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

# Define constants for interacting with external systems
S3_FILE_PATH = "s3://sdk-live-bucket"
S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_ORDERS = "orders_table"
SNOWFLAKE_CUSTOMERS = "customers_table"
SNOWFLAKE_REPORTING = "reporting_table"

# transform step for joining tables
@aql.transform
def join_orders_customers(filtered_orders_table: Table, customers_table: Table):
    return """SELECT c.customer_id, customer_name, order_id, purchase_date, amount, type
    FROM {{filtered_orders_table}} f JOIN {{customers_table}} c
    ON f.customer_id = c.customer_id"""

with DAG(
    dag_id="producer_dag",
    start_date=datetime(2019, 1, 1),
    schedule="@daily",
    catchup=False,
):

    # Task 1: Load data from S3 into Snowflake
    # creates and updates Dataset("astro://snowflake_default@?table=orders_table")
    orders_data = aql.load_file(
        input_file=File(
            path=S3_FILE_PATH + "/orders_data_header.csv", conn_id=S3_CONN_ID
        ),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
            name=SNOWFLAKE_ORDERS
        )
    )

    # Task 2: joins orders data and customer data into a temporary table, no datasets
    joined_data = join_orders_customers(
        orders_data, 
        customers_table = Table(
            name=SNOWFLAKE_CUSTOMERS,
            conn_id=SNOWFLAKE_CONN_ID,
        )
    )

    # Task 3: appends data from the temporary table into the reporting table
    # creates and updates Dataset("astro://snowflake_default@?table=reporting_table")
    reporting_table = aql.append(
        target_table=Table(
            name=SNOWFLAKE_REPORTING,
            conn_id=SNOWFLAKE_CONN_ID,
        ),
        source_table=joined_data,
    )

    # clean up temporary tables 
    aql.cleanup()