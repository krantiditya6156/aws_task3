import os
import zipfile
from os.path import basename, dirname, join

import boto3

script_dir = dirname(__file__)


class Deploy:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name + "-" + REGION + "-" + account_id

    def create_code_bucket(self):
        buckets = [bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"]]
        if self.bucket_name in buckets:
            print(f"Bucket {self.bucket_name} exists")
        else:
            print(f"Bucket {self.bucket_name} donot exist, creating...")
            s3_client.create_bucket(
                Bucket=self.bucket_name,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )

    def zip_file(self, filename):
        zip_file_path = join(script_dir, filename + ".zip")
        lambda_file_path = join(script_dir, filename + ".py")
        with zipfile.ZipFile(zip_file_path, "w") as zipf:
            zipf.write(lambda_file_path, arcname=basename(lambda_file_path))
        print(f"{filename}.zip created")
        return zip_file_path

    def upload_zipfile(self, zip_file_path, filename):
        s3_client.upload_file(zip_file_path, self.bucket_name, filename + ".zip")
        print(f"{filename}.zip  uploaded to s3 bucket {self.bucket_name}\n")
        os.remove(zip_file_path)

    def upload_lambda_files(self, filenames):

        for filename in filenames:
            zipped_file_path = self.zip_file(filename)
            self.upload_zipfile(zipped_file_path, filename)

    def upload_glue_scripts(self):
        script_path = join(script_dir, "glue_scripts")

        for file_name in os.listdir(script_path):
            local_path = os.path.join(script_path, file_name)
            s3_client.upload_file(
                local_path, self.bucket_name, f"glue_scripts/{file_name}"
            )
            print(
                f"Uploaded {file_name} to s3://{self.bucket_name}/glue_scripts/{file_name}"
            )

    def deploy_cloudformation_template(self):

        template_path = join(script_dir, TEMPLATE_FILE)
        with open(template_path, "r") as file:
            stack = file.read()

        params = [
            {
                "ParameterKey": "SourceBucketName",
                "ParameterValue": SOURCE_BUCKET_NAME,
            },
            {
                "ParameterKey": "AthenaOutputBucketName",
                "ParameterValue": ATHENA_OUTPUT_BUCKET_NAME,
            },
            {
                "ParameterKey": "EmailAddress",
                "ParameterValue": EMAIL_ADDRESS,
            },
            {
                "ParameterKey": "CodeBucketName",
                "ParameterValue": self.bucket_name,
            },
        ]

        try:
            cftclient.create_stack(
                StackName=STACK_NAME,
                TemplateBody=stack,
                Parameters=params,
                Capabilities=["CAPABILITY_IAM"],
            )
            print("creating stack...")
        except cftclient.exceptions.AlreadyExistsException:
            cftclient.update_stack(
                StackName=STACK_NAME,
                TemplateBody=stack,
                Parameters=params,
                Capabilities=["CAPABILITY_IAM"],
            )
            print("updating stack")
        except Exception as e:
            print(e)
            raise


if __name__ == "__main__":

    REGION = "ap-southeast-1"
    CODE_BUCKET_NAME = "codebucket"
    STACK_NAME = "Glue-stack"
    TEMPLATE_FILE = "cf_template.yaml"

    SOURCE_BUCKET_NAME = "files-landing-zone5677"
    ATHENA_OUTPUT_BUCKET_NAME = "athena-output"
    EMAIL_ADDRESS = "myemail@gmail.com"

    LAMBDA_FILE_NAMES = [
        "lambda_save_s3_config",
        "lambda_run_glue_jobs",
        "lambda_glue_job_success",
        "lambda_glue_job_failure",
    ]

    sts_client = boto3.client("sts")
    account_id = sts_client.get_caller_identity().get("Account")
    s3_client = boto3.client("s3", region_name=REGION)
    cftclient = boto3.client("cloudformation", region_name=REGION)

    obj = Deploy(CODE_BUCKET_NAME)
    obj.create_code_bucket()
    obj.upload_lambda_files(LAMBDA_FILE_NAMES)
    obj.upload_glue_scripts()
    obj.deploy_cloudformation_template()
