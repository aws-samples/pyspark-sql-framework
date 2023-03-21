import logging
from typing import Iterator, Dict
from dynamo_interface import DynamoInterface
from config_utils import s3_read_object
from custom_logger import CustomLogger
from string import Template
import boto3
from os import path

JOB_IDENTIFIER = 'emr_config'

class StepConfig:

    def __init__(self, dynamo_table_name: str, step_id: str , region_name: str) -> None:
        self.log = CustomLogger().adapter
        self.log.info(msg="Retrieving table configuration details for the specific job")
        dyn_table_config = DynamoInterface(table_name=dynamo_table_name, region_name= region_name)
        self.__config = dyn_table_config.get_item(key={"id": JOB_IDENTIFIER, "step_id": step_id})
        self.step_name = step_id
        self.sql_base_path = self.__config.get("sql_base_path")
        self.sqls = self.__config.get('sql_info')
        self.log.info(msg="Successfully retrieved the group + table configuration details")
        self.pre_config = self.__config.get("pre_config", None)
        self.s3_client = boto3.resource("s3")

    def get_active_sql_paths(self) -> Iterator:
        self.log.info(msg=self.sqls)
        return map(lambda x: (self.sql_base_path + x['sql_path'], x['sql_load_order'], x.get('sql_parameters', {})),
                   sorted(filter(lambda x: x.get('sql_active_flag').upper() == 'Y', self.sqls),
                          key=lambda x: x['sql_load_order']))

    def get_next_sql(self, bucket: str, root_prefix: str, extra_params: dict = {}):
        self.log.info(msg="Retriving the SQL's from S3")
        active_sqls = self.get_active_sql_paths()
        for sql_file_key, sql_load_order, sql_parameters in active_sqls:
            self.log.info(
                msg=f"Reading the sql file {sql_file_key} for the table {self.step_name} and sql id {sql_load_order}")
            sql_content = s3_read_object(bucket, path.join(root_prefix, sql_file_key), s3_client=self.s3_client)
            # sql_content = sql_file_key
            self.log.debug(msg=f"SQL content is {sql_content}")
            self.log.info(msg=f"Read the sql for the table {self.step_name} and sql id {sql_load_order}")
            sql_template = Template(sql_content.read())
            template_parameters = {**sql_parameters, **extra_params}
            sql = sql_template.safe_substitute(template_parameters)
            self.log.info(sql)
            yield sql, sql_load_order

    @property
    def step_configs(self) -> dict:
        return self.__config.get("spark_config")


if __name__ == '__main__':
    tb_cfg = StepConfig(dynamo_table_name="table_config", step_id="first_table")
    print(tb_cfg.step_name)
    print(tb_cfg.sqls)
    for sql, id in tb_cfg.get_next_sql(bucket="sample-table-config-test"):
        print(sql)
        print(id)
