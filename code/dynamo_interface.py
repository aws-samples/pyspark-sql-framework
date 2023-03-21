import boto3
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from custom_logger import CustomLogger


class DynamoInterface:

    def __init__(self, table_name: str, region_name: str) -> None:
        self.log = CustomLogger().adapter
        self.log.info("Creating the Dynamo Resource using Boto3")
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.dynamodb.Table(table_name)

    def get_item(self, key):
        try:
            self.log.info(f"Retrieving item from Dynamo using the key {key} and with ConsistenRead set to True")
            item = self.table.get_item(Key=key, ConsistentRead=True)['Item']
            self.log.info("Item retrieved")
            self.log.debug(f"item: {item}")
        except ClientError as e:
            msg = 'Error getting item from {} table'.format(self.table)
            self.log.error(msg, exc_info=e)
            raise e
        return item

    def query_items(self, key_field_name, key_value):
        items = self.table.query(
            KeyConditionExpression=Key(key_field_name).eq(key_value)
        )
        return items['Items']

    def put_item(self, item):
        try:
            self.log.info(f"Saving the item {item} on DynamoDB")
            self.table.put_item(Item=item)
            self.log.info("Item saved successfully")
        except ClientError as e:
            msg = 'Error putting item {} into {} table'.format(item, self.table)
            self.log.error(msg, exc_info=e)
            raise e


if __name__ == '__main__':
    dyn = DynamoInterface("app_etl_log_table_dev")
    log_entries_for_dag = dyn.query_items(key_field_name="job_run_id", key_value='abc-def-ghi-39')
    log_entries_for_current_table = list(
        sorted(filter(lambda x: x['status'] == "FAILED" and x['table_name'] == "first_table", log_entries_for_dag),
               key=lambda x: x['sql_seq']))
    print(log_entries_for_current_table)
    if log_entries_for_dag and log_entries_for_current_table:
        print(log_entries_for_current_table[0]['sql_seq'])
    elif log_entries_for_dag and not log_entries_for_current_table:
        print("NO pending sqls")
    else:
        print("new start")
