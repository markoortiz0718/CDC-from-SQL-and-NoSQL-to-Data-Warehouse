import os

import boto3
import redshift_connector

AWS_REGION = os.environ["AWSREGION"]

s3_client = boto3.client("s3")
S3_BUCKET_FOR_DYNAMODB_STREAM_TO_REDSHIFT = os.environ[
    "S3_BUCKET_FOR_DYNAMODB_STREAM_TO_REDSHIFT"
]
UNPROCESSED_DYNAMODB_STREAM_FOLDER = os.environ["UNPROCESSED_DYNAMODB_STREAM_FOLDER"]
PROCESSED_DYNAMODB_STREAM_FOLDER = os.environ["PROCESSED_DYNAMODB_STREAM_FOLDER"]

# aws_redshift.CfnCluster(...).attr_id (for cluster name) is broken, so using endpoint address instead
REDSHIFT_HOST = os.environ["REDSHIFT_ENDPOINT_ADDRESS"].split(":")[0]
REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_PASSWORD = os.environ["REDSHIFT_PASSWORD"]
REDSHIFT_ROLE_ARN = os.environ["REDSHIFT_ROLE_ARN"]
REDSHIFT_DATABASE_NAME = os.environ["REDSHIFT_DATABASE_NAME"]
REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC = os.environ[
    "REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC"
]
REDSHIFT_TABLE_NAME_FOR_DYNAMODB_CDC = os.environ[
    "REDSHIFT_TABLE_NAME_FOR_DYNAMODB_CDC"
]


def move_s3_file(s3_bucket: str, old_s3_filename: str, new_s3_filename) -> None:
    s3_client.copy_object(
        Bucket=s3_bucket,
        Key=new_s3_filename,
        CopySource={"Bucket": s3_bucket, "Key": old_s3_filename},
    )
    s3_client.delete_object(
        Bucket=s3_bucket,
        Key=old_s3_filename,
    )
    print(
        f"Moved s3://{s3_bucket}/{old_s3_filename} to "
        f"s3://{s3_bucket}/{new_s3_filename}"
    )


def lambda_handler(event, context) -> None:
    dynamodb_stream_s3_files = s3_client.list_objects_v2(
        Bucket=S3_BUCKET_FOR_DYNAMODB_STREAM_TO_REDSHIFT,
        Prefix=f"{UNPROCESSED_DYNAMODB_STREAM_FOLDER}/",
        Delimiter="/",
    ).get("Contents", [])
    dynamodb_stream_s3_files = [dct["Key"] for dct in dynamodb_stream_s3_files]
    if dynamodb_stream_s3_files:
        for s3_file in dynamodb_stream_s3_files:
            if "__inserted_or_modified_records.json" in s3_file:  # hard coded suffix
                sql_statement = f"""
                    COPY {REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC}.{REDSHIFT_TABLE_NAME_FOR_DYNAMODB_CDC}
                    FROM 's3://{S3_BUCKET_FOR_DYNAMODB_STREAM_TO_REDSHIFT}/{s3_file}'
                    REGION '{AWS_REGION}'
                    iam_role '{REDSHIFT_ROLE_ARN}'
                    format as json 'auto';
                """
                conn = redshift_connector.connect(
                    host=REDSHIFT_HOST,
                    database=REDSHIFT_DATABASE_NAME,
                    user=REDSHIFT_USER,
                    password=REDSHIFT_PASSWORD,
                )
                with conn, conn.cursor() as cursor:
                    cursor.execute(sql_statement)
                    conn.commit()
                    print(f"Finished executing the following SQL statement: {sql_statement}")
                move_s3_file(
                    s3_bucket=S3_BUCKET_FOR_DYNAMODB_STREAM_TO_REDSHIFT,
                    old_s3_filename=s3_file,
                    new_s3_filename=s3_file.replace(
                        UNPROCESSED_DYNAMODB_STREAM_FOLDER,
                        PROCESSED_DYNAMODB_STREAM_FOLDER,
                    ),
                )
            elif (
                "__no_inserted_or_modified_records.txt" in s3_file
            ):  # hard coded suffix
                move_s3_file(
                    s3_bucket=S3_BUCKET_FOR_DYNAMODB_STREAM_TO_REDSHIFT,
                    old_s3_filename=s3_file,
                    new_s3_filename=s3_file.replace(
                        UNPROCESSED_DYNAMODB_STREAM_FOLDER,
                        PROCESSED_DYNAMODB_STREAM_FOLDER,
                    ),
                )
            else:
                raise  ### figure out useful message in exception
    else:
        print(
            "No DynamoDB stream files in "
            f"s3://{S3_BUCKET_FOR_DYNAMODB_STREAM_TO_REDSHIFT}/"
            f"{UNPROCESSED_DYNAMODB_STREAM_FOLDER}/ folder"
        )
