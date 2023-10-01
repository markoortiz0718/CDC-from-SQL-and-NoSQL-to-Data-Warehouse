import os

import redshift_connector


# aws_redshift.CfnCluster(...).attr_id (for cluster name) is broken, so using endpoint address instead
REDSHIFT_HOST = os.environ["REDSHIFT_ENDPOINT_ADDRESS"].split(":")[0]
REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_PASSWORD = os.environ["REDSHIFT_PASSWORD"]
REDSHIFT_DATABASE_NAME = os.environ["REDSHIFT_DATABASE_NAME"]
REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC = os.environ[
    "REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC"
]
REDSHIFT_TABLE_NAME_FOR_DYNAMODB_CDC = os.environ[
    "REDSHIFT_TABLE_NAME_FOR_DYNAMODB_CDC"
]


def lambda_handler(event, context) -> None:
    sql_statements = [
        f'CREATE SCHEMA IF NOT EXISTS "{REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC}";',
        f"""CREATE TABLE IF NOT EXISTS
            "{REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC}"."{REDSHIFT_TABLE_NAME_FOR_DYNAMODB_CDC}" (
                id varchar(30) UNIQUE NOT NULL,
                details super,
                price float,
                shares integer,
                ticker varchar(10),
                ticket varchar(10),
                time super
            );""",  # hard coded columns
    ]
    conn = redshift_connector.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE_NAME,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )
    with conn, conn.cursor() as cursor:
        for sql_statement in sql_statements:
            cursor.execute(sql_statement)
            conn.commit()
            print(f"Finished executing the following SQL statement: {sql_statement}")
