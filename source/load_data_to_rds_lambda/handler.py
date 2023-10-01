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
    conn = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        passwd=RDS_PASSWORD,
        db=RDS_DATABASE_NAME,
        connect_timeout=5,
    )
    with conn, conn.cursor() as cursor, open(CSV_FILENAME) as f:
        csv_reader = csv.reader(f)
        column_names = next(csv_reader)
        column_names = [
            column_name.replace(" ", "_").lower() for column_name in column_names
        ]
        csv_data = [tuple(row) for row in csv_reader]
        cursor.executemany(
            """
            INSERT INTO `{rds_database_name}`.`{rds_table_name}` ({column_names})
            VALUES ({column_types});""".format(
                rds_database_name=RDS_DATABASE_NAME,
                rds_table_name=RDS_TABLE_NAME,
                column_names=", ".join(column_names),
                column_types=", ".join(["%s"] * len(column_names)),
            ),
            csv_data,
        )
        conn.commit()
