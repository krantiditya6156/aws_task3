import json
import os
import time

import boto3

ATHENA_OUTPUT = os.environ["ATHENA_OUTPUT_BUCKET_URI"]
DATABASE = os.environ["GLUE_DATABASE_NAME"]
ACCOUNT_ID = os.environ["ACCOUNT_ID"]
REGION_NAME = os.environ["REGION_NAME"]


def run_crawler(crawler_name):
    try:
        client = boto3.client("glue")
        client.start_crawler(Name=crawler_name)
        print(f"{crawler_name} started")

        while True:
            if get_crawler_state(crawler_name) == "READY":
                print("Crawler completed successfully.")
                break
            print("Crawler is running...")
            time.sleep(10)
    except Exception as e:
        print(e)
        raise e


def get_crawler_state(crawler_name):
    try:
        client = boto3.client("glue")
        response = client.get_crawler(Name=crawler_name)
        crawler_state = response["Crawler"]["State"]
        return crawler_state
    except Exception as e:
        print(e)
        raise e


def get_crawler_name(job_name):
    try:
        if "csv" in job_name:
            crawler_name = "csvfiles_crawler_" + REGION_NAME + "_" + ACCOUNT_ID
        elif "json" in job_name:
            crawler_name = "jsonfiles_crawler_" + REGION_NAME + "_" + ACCOUNT_ID
        elif "text" in job_name:
            crawler_name = "textfiles_crawler_" + REGION_NAME + "_" + ACCOUNT_ID
        return crawler_name
    except Exception as e:
        print(e)
        raise e


def get_table_name(crawler_name):
    try:
        client = boto3.client("glue")
        response = client.get_crawler(Name=crawler_name)
        print(response)
        table_name = response["Crawler"]["Targets"]["S3Targets"][0]["Path"].split("/")[
            -2
        ]
        return table_name
    except Exception as e:
        print(e)
        raise e


def run_athena_query(query, database):
    try:
        client = boto3.client("athena", region_name="ap-south-1")
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )
        print(response)
        return response
    except Exception as e:
        print(e)
        raise e


def lambda_handler(event, context):
    try:
        print(event)

        job_name = event["detail"]["jobName"]
        state = event["detail"]["state"]

        if state == "SUCCEEDED":
            print(f"{job_name} succeeded")

            crawler_name = get_crawler_name(job_name)

            max_retries = 30
            retry_count = 0

            while True:
                if get_crawler_state(crawler_name) == "READY":
                    run_crawler(crawler_name)
                    break
                else:
                    print(
                        f"{crawler_name} is not ready, retrying {retry_count + 1}/{max_retries}"
                    )
                    time.sleep(10)
                    retry_count += 1

            table_name = get_table_name(crawler_name)
            query = f"SELECT * FROM {table_name} ;"
            print(f"Query: {query}")

            response = run_athena_query(query, DATABASE)

            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                query_execution_id = response["QueryExecutionId"]
                print(f"Query execution id is {query_execution_id}")
                print(
                    f"Query executed successfully and result is saved in s3 bucket {ATHENA_OUTPUT} "
                )
            else:
                print("Query execution failed.")
                raise Exception("Query execution failed.")

    except Exception as e:
        print(e)
        raise e

    # TODO implement
    return {"statusCode": 200, "body": json.dumps("Success")}
