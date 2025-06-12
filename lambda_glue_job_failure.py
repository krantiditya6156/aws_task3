import json
import os

import boto3

TOPIC_ARN = os.environ["TOPIC_ARN"]


def lambda_handler(event, context):
    try:
        print(event)
        job_name = event["detail"]["jobName"]
        state = event["detail"]["state"]
        message = event["detail"]["message"]

        client = boto3.client("sns")

        if state == "FAILED":
            print(f"{job_name} failed")
            print(f"{message}")
            client.publish(
                TopicArn=TOPIC_ARN, Message=message, Subject=f"{job_name} failed"
            )

    except Exception as e:
        print(e)
        raise e

    # TODO implement
    return {"statusCode": 200, "body": json.dumps("Success")}
