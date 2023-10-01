import json
import os

import boto3
import redshift_connector

dms_client = boto3.client("dms")
DMS_REPLICATION_TASK_ARN = os.environ["DMS_REPLICATION_TASK_ARN"]
PRINT_RDS_AND_REDSHIFT_NUM_ROWS = json.loads(
    os.environ["PRINT_RDS_AND_REDSHIFT_NUM_ROWS"]
)
if PRINT_RDS_AND_REDSHIFT_NUM_ROWS:
    import pymysql

    RDS_HOST = os.environ["RDS_HOST"]
    RDS_USER = os.environ["RDS_USER"]
    RDS_PASSWORD = os.environ["RDS_PASSWORD"]
    RDS_DATABASE_NAME = os.environ["RDS_DATABASE_NAME"]
    RDS_TABLE_NAME = os.environ["RDS_TABLE_NAME"]

    REDSHIFT_HOST = os.environ["REDSHIFT_ENDPOINT_ADDRESS"].split(":")[0]
    REDSHIFT_USER = os.environ["REDSHIFT_USER"]
    REDSHIFT_PASSWORD = os.environ["REDSHIFT_PASSWORD"]
    REDSHIFT_DATABASE_NAME = os.environ["REDSHIFT_DATABASE_NAME"]


def count_rds_table_num_rows():
    """Currently only works with MySQL variant of RDS"""
    conn = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        passwd=RDS_PASSWORD,
        db=RDS_DATABASE_NAME,
        connect_timeout=5,
    )
    with conn, conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM rds_cdc_table")
        print(
            f"RDS table `{RDS_DATABASE_NAME}.{RDS_TABLE_NAME}` "
            f"has {cursor.fetchone()[0]} rows."
        )


def count_redshift_table_num_rows():
    conn = redshift_connector.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE_NAME,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )
    with conn, conn.cursor() as cursor:
        sql_statement = "SELECT COUNT(*) FROM {}.{}.{};".format(
            REDSHIFT_DATABASE_NAME, RDS_DATABASE_NAME, RDS_TABLE_NAME
        )
        cursor.execute(sql_statement)
        conn.commit()
        print(f"Finished executing the following SQL statement: {sql_statement}")


def lambda_handler(event, context):
    response = dms_client.describe_replication_tasks(
        Filters=[{"Name": "replication-task-arn", "Values": [DMS_REPLICATION_TASK_ARN]}]
    )["ReplicationTasks"]
    assert len(response) == 1, "There should be exactly 1 replication task ARN"
    status = response[0]["Status"]
    assert status in ["ready", "stopped", "running"], f"Unexpected status: {status}"
    if status in ["ready", "stopped"]:
        response = dms_client.start_replication_task(
            ReplicationTaskArn=DMS_REPLICATION_TASK_ARN,
            StartReplicationTaskType="start-replication",
        )
        print(f"Started DMS Replication Task. Here is the response: {response}")
    elif status == "running":
        print("DMS Replication Task is already running, so do no extra action.")
        if PRINT_RDS_AND_REDSHIFT_NUM_ROWS:
            count_rds_table_num_rows()
            count_redshift_table_num_rows()
    else:
        raise
