import json
import time

import boto3

ATHENA_OUTPUT = "s3://athena-query-results-bucket005/"
DATABASE = "glue_database"


def run_crawler(crawler_name):
    try:
        client = boto3.client("glue")
        client.start_crawler(Name=crawler_name)
        print(f"{crawler_name} started")

        while True:
            if get_crawler_state(crawler_name) == "READY":
                print("Crawler completed successfully.")
                break
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
            crawler_name = "csvfiles_crawler"
        elif "json" in job_name:
            crawler_name = "jsonfiles_crawler"
        elif "txt" in job_name:
            crawler_name = "txtfiles_crawler"
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

            if get_crawler_state(crawler_name) == "READY":
                run_crawler(crawler_name)
            else:
                raise Exception("Crawler is not ready.")

            table_name = get_table_name(crawler_name)
            query = f"SELECT * FROM {table_name} LIMIT 10;"
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
