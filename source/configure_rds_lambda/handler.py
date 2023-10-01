import csv
import os

import pymysql

CSV_FILENAME = os.environ["CSV_FILENAME"]
RDS_HOST = os.environ["RDS_HOST"]
RDS_USER = os.environ["RDS_USER"]
RDS_PASSWORD = os.environ["RDS_PASSWORD"]
RDS_DATABASE_NAME = os.environ["RDS_DATABASE_NAME"]
RDS_TABLE_NAME = os.environ["RDS_TABLE_NAME"]


def lambda_handler(event, context) -> None:
    """Currently only works with MySQL variant of RDS"""
    conn = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        passwd=RDS_PASSWORD,
        db=RDS_DATABASE_NAME,
        connect_timeout=5,
        autocommit=True,  # needs be True for the statements to run successfully
    )
    with conn, conn.cursor() as cursor, open(CSV_FILENAME) as f:
        cursor.execute("call mysql.rds_show_configuration;")
        print("original `binlog retention hours`:", cursor.fetchone())

        cursor.execute(
            "call mysql.rds_set_configuration('binlog retention hours', 24);"
        )
        cursor.execute("call mysql.rds_show_configuration;")
        print("new `binlog retention hours`:", cursor.fetchone())

        csv_reader = csv.reader(f)
        column_names = next(csv_reader)
        column_names = [
            column_name.replace(" ", "_").lower() for column_name in column_names
        ]
        csv_data = [tuple(row) for row in csv_reader]
        # print(csv_data)
        cursor.execute(
            "CREATE TABLE if not exists `{rds_database_name}`.`{rds_table_name}` ({column_name_and_types});".format(
                rds_database_name=RDS_DATABASE_NAME,
                rds_table_name=RDS_TABLE_NAME,
                column_name_and_types=", ".join(
                    f"{column_name} varchar(40)" for column_name in column_names
                ),
            )  # did not define a primary key
        )
