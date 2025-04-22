# aws_task3

File landing in S3 bucket triggers a lambda function that reads a small configuration from dynamodb and depending on file size and file type runs appropriate glue job.  Try to focus on resiliency of messages in case of concurrent or multiple files of same type landing in s3. On successful glue job run, lambda should be automatically triggered that runs a crawler and runs Athena query on the table created to create a view. On failure of glue job, the lambda should send an alert.
Note: Use object oriented code and modularise the code as much as possible. Use previous tasks efficiently, not necessary though.
