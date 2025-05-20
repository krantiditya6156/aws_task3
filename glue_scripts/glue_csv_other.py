import sys

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "SOURCE_BUCKET_NAME", "CRAWLER_BUCKET_NAME", "FILE_NAME"]
)

source_bucket_name = args["SOURCE_BUCKET_NAME"]
crawler_bucket_name = args["CRAWLER_BUCKET_NAME"]
file_name = args["FILE_NAME"].split(".")[0]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


s3_path_input = f"s3://{source_bucket_name}/{file_name}" + ".csv"

s3_path_output = f"s3://{crawler_bucket_name}/output/"


dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path_input]},
    format="csv",
    format_options={"withHeader": True},
)

df = dyf.toDF()

COUNTRY_NAME_TO_COUNTRY_CODE = {
    "Abkhazia": "AB",
    "Afghanistan": "AF",
    "Albania": "AL",
    "Algeria": "DZ",
    "American Samoa": "AS",
    "Andorra": "AD",
    "Angola": "AO",
    "Anguilla": "AI",
    "Antigua and Barbuda": "AG",
    "Argentina": "AR",
    "Armenia": "AM",
    "Aruba": "AW",
    "Australia": "AU",
    "Austria": "AT",
    "Azerbaijan": "AZ",
    "Bahamas": "BS",
    "Bahrain": "BH",
    "Bangladesh": "BD",
    "Barbados": "BB",
    "Belarus": "BY",
    "Belgium": "BE",
    "Belize": "BZ",
    "Benin": "BJ",
    "Bermuda": "BM",
    "Bhutan": "BT",
    "Bolivia": "BO",
    "Bonaire": "BQ",
    "Bosnia and Herzegovina": "BA",
    "Botswana": "BW",
    "Bouvet Island": "BV",
    "Brazil": "BR",
    "British Indian Ocean Territory": "IO",
    "British Virgin Islands": "VG",
    "Virgin Islands, British": "VG",
    "Brunei": "BN",
    "Brunei Darussalam": "BN",
    "Bulgaria": "BG",
    "Burkina Faso": "BF",
    "Burundi": "BI",
    "Cambodia": "KH",
    "Cameroon": "CM",
    "Canada": "CA",
}


def get_country_code(country_name):
    return COUNTRY_NAME_TO_COUNTRY_CODE.get(country_name.strip(), "Unknown")


udf_get_country_code = udf(get_country_code, StringType())
new_df = df.withColumn("Country_code", udf_get_country_code(col("Country")))
dyf_updated = DynamicFrame.fromDF(new_df, glueContext)

glueContext.write_dynamic_frame.from_options(
    frame=dyf_updated,
    connection_type="s3",
    connection_options={"path": s3_path_output},
    format="csv",
)

s3 = boto3.client("s3")
output_prefix = "output/"

response = s3.list_objects_v2(Bucket=crawler_bucket_name, Prefix=output_prefix)

latest_file = None
if "Contents" in response:
    latest_file = max(response["Contents"], key=lambda obj: obj["LastModified"])["Key"]

if latest_file:
    destination_key = f"csvdata/transformed_{file_name}.csv"

    s3.copy_object(
        Bucket=crawler_bucket_name,
        CopySource=f"{crawler_bucket_name}/{latest_file}",
        Key=destination_key,
    )

    print(f"File copied and renamed to {destination_key}")


job.commit()
