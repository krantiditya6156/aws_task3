import json
import os

import boto3
from boto3.dynamodb.conditions import Key

TABLE_NAME = os.environ["TABLE_NAME"]
ACCOUNT_ID = os.environ["ACCOUNT_ID"]
REGION_NAME = os.environ["REGION_NAME"]

dynamodb = boto3.resource("dynamodb")


def read_from_dynamodb(table_name, key):
    try:
        table = dynamodb.Table(table_name)
        response = table.query(KeyConditionExpression=Key("object_key").eq(key))
    except Exception as e:
        print(e)
        raise

    return response["Items"]


def get_glue_job_name(content_type, file_size):
    if content_type == "image/jpeg":
        if file_size <= 100000:
            glue_job_name = "gluejob_image_100kb"
        elif file_size > 100000 and file_size <= 200000:
            glue_job_name = "gluejob_image_100_200kb"
        else:
            glue_job_name = "gluejob_image_other"

    elif content_type == "text/plain":
        if file_size <= 5000:
            glue_job_name = "glue_job_text_5kb_" + REGION_NAME + "_" + ACCOUNT_ID
        elif file_size > 5000 and file_size <= 10000:
            glue_job_name = "glue_job_text_5_10kb_" + REGION_NAME + "_" + ACCOUNT_ID
        else:
            glue_job_name = "glue_job_text_other_" + REGION_NAME + "_" + ACCOUNT_ID

    elif content_type == "application/json":
        if file_size <= 5000:
            glue_job_name = "glue_job_json_5kb_" + REGION_NAME + "_" + ACCOUNT_ID
        elif file_size > 5000 and file_size <= 10000:
            glue_job_name = "glue_job_json_5_10kb_" + REGION_NAME + "_" + ACCOUNT_ID
        else:
            glue_job_name = "glue_job_json_other_" + REGION_NAME + "_" + ACCOUNT_ID

    elif content_type == "text/csv":
        if file_size <= 5000:
            glue_job_name = "glue_job_csv_5kb_" + REGION_NAME + "_" + ACCOUNT_ID
        elif file_size > 5000 and file_size <= 10000:
            glue_job_name = "glue_job_csv_5_10kb_" + REGION_NAME + "_" + ACCOUNT_ID
        else:
            glue_job_name = "glue_job_csv_other_" + REGION_NAME + "_" + ACCOUNT_ID

    return glue_job_name


def lambda_handler(event, context):

    try:
        print(event)

        sns = event["Records"][0]["Sns"]
        sns_message = json.loads(sns["Message"])
        print("------------------------")
        print(sns_message)
        file_name = sns_message["Records"][0]["s3"]["object"]["key"]

        records = read_from_dynamodb(table_name=TABLE_NAME, key=file_name)

        if records:
            latest_record = records[0]
            for record in records:
                if record["last_modified_data"] > latest_record["last_modified_data"]:
                    latest_record = record
            # print(f"version_id: {record['version_id']}, last_modified_data: {record['last_modified_data']}, content_type: {record['content_type']}, file_size: {record['file_size']}")
            # print("----")

        print(latest_record)

        content_type = latest_record["content_type"]
        file_size = latest_record["file_size"]

        glue_job_name = get_glue_job_name(content_type, file_size)

        print(f"glue_job_name: {glue_job_name}")

        glue_client = boto3.client("glue")
        glue_client.start_job_run(JobName=glue_job_name)
        print(f"Glue job {glue_job_name} started")

    except Exception as e:
        print(e)
        raise

    # TODO implement
    return {"statusCode": 200, "body": json.dumps("Success")}
