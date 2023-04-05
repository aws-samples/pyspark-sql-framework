## emr_pyspark_framework

Metadata driven framework built using Pyspark to support SQL executions on EMR. 
Framework uses DynamoDB tables for defining the configuration for each Job. Each job can be further divided into Steps and each Step could have N number of SQL for performing ETL. 
DynamoDB has the details of the sql's  and their execution order for each step inside the job. 
Logging of job execution is on DynamoDB where we can clearly see the status of each and every sql for a step/job.

In case of migration from Hive + Oozie to Spark, this framework can be used in conjunction with automation(https://github.com/aws-samples/oozie-job-parser-extract-hive-sql) which would help generate the sql's and dynamoDB entries used by this framework.

## Key highlights:

* Avoids boiler plate code to create a new JOB for any new pipeline, Less Maintenance of code.
* On-boarding of any new pipeline needs only Metadata setup - DynamoDB entries and SQL to be placed in S3 path
* Any common modifications for code can be done at one place in Framework code which will be reflected at all Jobs
* One place view using DynamoDB Metadata table of all jobs present in Environment and their optimized runtime Parameters
* For each run Framework will persist each SQLs start time , end point and status in a dynamodb log table to identify issues and analyze runtime.


## Setup: 

1. Install AWS CLI 
2. Create Metadata and log DynamoDB Tables. 

**Metadata table**

```shell 
aws dynamodb create-table --region us-east-1 --table-name dw-etl-metadata --attribute-definitions '[ { "AttributeName": "id","AttributeType": "S" } , { "AttributeName": "step_id","AttributeType": "S" }]' --key-schema '[{"AttributeName": "id", "KeyType": "HASH"}, {"AttributeName": "step_id", "KeyType": "RANGE"}]' --billing-mode PROVISIONED --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
```

**Logging table**
```shell 
aws dynamodb create-table --region us-east-1 --table-name dw-etl-pipelinelog --attribute-definitions '[ { "AttributeName":"job_run_id", "AttributeType": "S" } , { "AttributeName":"step_id", "AttributeType": "S" } ]' --key-schema'[{"AttributeName": "job_run_id", "KeyType": "HASH"},"AttributeName": "step_id", "KeyType": "RANGE"}]' --billing-mode PROVISIONED --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
```

3. S3 bucket for storing the code which would be referred in EMR step.

```shell 
export s3_bucket_name=unique-code-bucket-name # Change unique-code-bucket-name to a valid bucket name
aws s3api create-bucket --bucket $s3_bucket_name
```

4. Clone project locally 
git clone https://github.com/aws-samples/pyspark-sql-framework.git

5. Create a ZIP File and upload to the code bucket created earlier:
```shell 
cd pyspark-sql-framework/code
zip code.zip -r *
aws s3 cp ./code.zip s3://$s3_bucket_name/framework/code.zip
```

6. Upload drive code to S3 bucket 
```shell
cd $OLDPWD/pyspark-sql-framework
aws s3 cp ./code/etl_driver.py s3://$s3_bucket_name/framework/
```

7. Upload Sample job sqls to S3 

```shell
aws s3 cp ./sample_oozie_job_name/ s3://$s3_bucket_name/DW/sample_oozie_job_name/ --recursive
```


8. Add sample step(./sample_oozie_job_name/step1/step1.json) to dynamodb using the AWS Console approach shared here(https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/getting-started-step-2.html)

```json
{
  "name": "step1.q",
  "step_name": "step1",
  "sql_info": [
    {
      "sql_load_order": 5,
      "sql_parameters": {
        "DATE": "${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd')}",
        "HOUR": "${coord:formatTime(coord:nominalTime(), 'HH')}"
      },
      "sql_active_flag": "Y",
      "sql_path": "5.sql"
    },
    {
      "sql_load_order": 10,
      "sql_parameters": {
        "DATE": "${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd')}",
        "HOUR": "${coord:formatTime(coord:nominalTime(), 'HH')}"
      },
      "sql_active_flag": "Y",
      "sql_path": "10.sql"
    }
  ],
  "id": "emr_config",
  "step_id": "sample_oozie_job_name#step1",
  "sql_base_path": "sample_oozie_job_name/step1/",
  "spark_config": {
    "spark.sql.parser.quotedRegexColumnNames": "true"
  }
}
```

9. Create database “base” using Athena
```sql 
create database base;
```

10. Copy sample data files from framework repo to s3
```shell
aws s3 cp ./sample_data/us_current.csv s3://$s3_bucket_name/covid-19-testing-data/base/source_us_current/;
aws s3 cp ./sample_data/states_current.csv s3://$s3_bucket_name/covid-19-testing-data/base/source_states_current/;
```

11. Create the source tables in “base” database created above. Run the DDLs present in framework repo on Athena

  - Run ./sample_data/ddl/states_current.q file by modifying the S3 path to bucket create in step 3
  - Run ./sample_data/ddl/us_current.q file by modifying the S3 path to bucket create in step 3


12. ETL Driver file implements the Spark Driver logic. It can be invoked locally or on EMR
  instance.
  - Launch EMR cluster.

      Please make sure to check/select the box for “Use for Spark table metadata” under “AWS Glue Data Catalog settings”

      
  - Add below step for installing boto3 package. Update the statement to refer the cluster created above.

```shell 
aws emr add-steps \
--cluster-id <<cluster id>>  \
--steps Type=CUSTOM_JAR,Name="test",ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=[bash,-c,"pip install boto3"] 
```

  - Add below spark step

```shell 
spark-submit --deploy-mode client --master yarn --name sample_oozie_job_name --conf spark.sql.shuffle.partitions=5760 --conf spark.dynamicAllocation.maxExecutors=30  --py-files s3://unique-code-bucket-name/framework/code.zip s3://unique-code-bucket-name/framework/etl_driver.py --step_id 'sample_oozie_job_name#states_daily' --job_run_id 'sample_oozie_job_name#states_daily#2022-01-01-12-00-01'  --code_bucket=s3://unique-code-bucket-name/DW --metadata_table=dw-etl-metadata --log_table_name=dw-etl-pipelinelog --sql_parameters DATE=2022-02-02::HOUR=12

