import json
import os

import boto3

s3_client = boto3.client("s3")
dynamodb_client = boto3.resource("dynamodb")
TABLE_NAME = os.environ["TABLE_NAME"]


def lambda_handler(event, context):
    print(event)

    sns = event["Records"][0]["Sns"]
    sns_message = json.loads(sns["Message"])

    print(sns_message)
    bucket_name = sns_message["Records"][0]["s3"]["bucket"]["name"]
    file_name = sns_message["Records"][0]["s3"]["object"]["key"]

    response = s3_client.head_object(Bucket=bucket_name, Key=file_name)

    tags = s3_client.get_object_tagging(Bucket=bucket_name, Key=file_name)

    table = dynamodb_client.Table(TABLE_NAME)
    table.put_item(
        Item={
            "object_key": file_name,
            "version_id": response["VersionId"],
            "metadata": response["Metadata"],
            "tags": tags["TagSet"],
            "file_size": response["ContentLength"],
            "content_type": response["ContentType"],
            "last_modified_data": response["LastModified"].strftime(
                "%m/%d/%Y, %H:%M:%S"
            ),
        }
    )

    return {"statusCode": 200, "body": json.dumps("saved into dynamodb table")}
