import datetime
from string import Template
import time

from step_config import StepConfig
from dynamo_interface import DynamoInterface
from datetime import datetime
from config_utils import generate_run_id, get_expiry_time
from custom_logger import CustomLogger, STAT_LEVEL
from env_info import AWS_REGION, PLATFORM_PREFIX
from pyspark.sql import SparkSession
from pyspark import SparkConf
from custom_functions import *


class EtlRunner:
    def __init__(self, *, job_run_id: str, step_id: str, code_bucket: str, metadata_table: str, log_table_name: str) -> None:
        self.log = CustomLogger().set_logger().log
        self.task_run_id = str(generate_run_id())
        self.extras = {"dag_id": job_run_id, "run_id": self.task_run_id,
                       "step_id": step_id}

        self.code_bucket, self.root_prefix = code_bucket.replace("s3://", "").split("/")
        self.metadata_table = metadata_table
        self.log_table_name = log_table_name

        self.log = CustomLogger().set_adapter(
            extras=self.extras).adapter
        self.step_config = StepConfig(dynamo_table_name=self.metadata_table,
                                      step_id=step_id, region_name=AWS_REGION)
        self.log_table = DynamoInterface(self.log_table_name,  AWS_REGION)
        self.job_run_id = job_run_id
        self.step_id = step_id


    def default_configs(self) -> dict:
        """ Get the global spark configs"""
        default_global_configs = {
            "spark.sql.broadcastTimeout":"10000",
            "spark.sql.legacy.parquet.int96RebaseModeInRead":"LEGACY",
            "spark.sql.legacy.parquet.int96RebaseModeInWrite":"LEGACY",
            "spark.sql.legacy.parquet.datetimeRebaseModeInRead":"LEGACY",
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite":"LEGACY",
            "spark.sql.parser.quotedRegexColumnNames": "true",
            "spark.sql.legacy.timeParserPolicy":"LEGACY",
            "spark.hadoop.fs.s3.maxRetries":25
            }
        self.log.info(
                f"Default global configs: {default_global_configs}")
        return default_global_configs


    def build_spark_conf(self, job_config: dict) -> SparkConf:
        spark_configs = self.default_configs()
        spark_configs.update(job_config)
        self.log.info(f"Spark Configs for the step: {spark_configs}")
        spark_conf = SparkConf().setAll(pairs=spark_configs.items())
        return spark_conf


    def preprocessing(self, **run_kwargs):

        self.log.info(
            f"Started execution of  step_id: {self.step_id}")
        self.spark = SparkSession.builder.appName(self.step_id).config(
            conf=self.build_spark_conf(job_config=self.step_config.step_configs)).enableHiveSupport().getOrCreate()
        self.log.info("Registering the UDF's")
        register_functions()
        self.dynamic_step_metadata = {}
        if self.step_config.pre_config:
            self.log.info("Started Pre processing step")
            meta_sql = self.step_config.pre_config['meta_sql']
            meta_sql_template = Template(meta_sql)
            self.log.info("Fetching metadata")
            sql = meta_sql_template.safe_substitute(run_kwargs['extra_params'])
            self.log.info(f"Executing sql: {sql}")
            self.dynamic_step_metadata[self.step_config.pre_config['key_name']] = self.spark.sql(sql).first()[self.step_config.pre_config['key_name']]
            self.log.info(f"Metadata output: {self.dynamic_step_metadata}")
            self.log.info("Metadata fetch completed")
        else:
            self.dynamic_step_metadata = None


    def start(self, **run_kwargs):
        if self.step_config.pre_config:
            self.log.info("Run time args: {0}".format(run_kwargs))
            self.log.info("pre processing output: {0}".format(self.dynamic_step_metadata) )
            run_kwargs['extra_params'].update(self.dynamic_step_metadata)
        try:
            for sql, sql_seq in self.step_config.get_next_sql(bucket=self.code_bucket, root_prefix=self.root_prefix, **run_kwargs):
                try:
                    end_time = start_time = datetime.now()
                    sql_status = {"job_run_id": self.job_run_id,
                                  "id": self.task_run_id + "-" + str(sql_seq),
                                  "step_id": self.step_id, "sql_seq": sql_seq, "status": "STARTED",
                                  "error_description": None,
                                  "start_time": datetime.strftime(start_time, format='%Y-%m-%d_%H:%M:%S.%f'),
                                  "end_time": None}
                    self.log_table.put_item(sql_status)
                    self.log.info(
                        f"Starting the execution of the sql for step: {self.step_id}, sql_id: {sql_seq}")
                    self.spark.sql(sql)
                    self.log.info(
                        f"Execution of the sql for step_id: {self.step_id}, sql_id: {sql_seq} completed successfully")
                except Exception as e:
                    sql_status['status'] = "FAILED"
                    sql_status['error_description'] = str(e)
                    end_time = datetime.now()
                    sql_status['end_time'] = datetime.strftime(end_time, format='%Y-%m-%d_%H:%M:%S.%f')
                    sql_status['expire'] = int(time.time() + get_expiry_time())
                    self.log.error(
                        f"Execution of the sql for step_id: {self.step_id}, sql_id: {sql_seq} failed")
                    self.log_table.put_item(sql_status)

                    message_body = "\n".join(
                        [k + "\t:\t" + str(v).replace("\n", " ") for k, v, in sql_status.items()])
                    self.log.log(STAT_LEVEL,
                                 f"SQL_SEQ: {sql_seq} , START_TIME: {start_time} , END_TIME: {end_time}, EXECUTION_TIME: {(end_time - start_time).total_seconds()}, STATUS: FAILED")
                    raise e
                else:
                    end_time = datetime.now()
                    sql_status['status'] = "SUCCEEDED"
                    sql_status['error_description'] = None
                    sql_status['end_time'] = datetime.strftime(end_time, format='%Y-%m-%d_%H:%M:%S.%f')
                    sql_status['expire'] = int(time.time() + get_expiry_time())
                    self.log_table.put_item(sql_status)
                    self.log.log(STAT_LEVEL,
                                 f"SQL_SEQ: {sql_seq} , START_TIME: {start_time} , END_TIME: {end_time} , EXECUTION_TIME: {(end_time - start_time).total_seconds()}, STATUS: SUCCEEDED")

        except Exception as e:
            self.log.info(
                f"Task Failed for step_id: {self.step_id}")
            status = {"job_run_id": self.job_run_id, "id": self.task_run_id,
                      "step_id": self.step_id, "status": "FAILED",
                      "error_description": str(e),
                      "time": datetime.strftime(datetime.now(), format='%Y-%m-%d_%H:%M:%S.%f'),
                      }
            message_body = "\n".join([k + "\t:\t" + str(v).replace("\n", " ") for k, v, in status.items()])
            raise e
