import boto3
import uuid

from io import StringIO
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus
from custom_logger import CustomLogger

EXPIRATION_DAYS = 35


def get_expiry_time():
    return 24 * 60 * 60 * EXPIRATION_DAYS


def get_account_id():
    sts_client = boto3.client("sts")
    account_id = sts_client.get_caller_identity()['Account']
    return account_id


def get_region():
    return boto3.session.Session().region_name


def generate_run_id() -> uuid.UUID:
    # TODO: add logger
    return uuid.uuid4()


def s3_read_object(bucket, key, s3_client):
    log = CustomLogger().adapter
    key = unquote_plus(key)
    log.info("Reading object from {}/{}".format(bucket, key))
    data = StringIO()
    try:
        obj = s3_client.Object(bucket, key)
        for line in obj.get()["Body"].iter_lines():
            data.write('{}\n'.format(line.decode("UTF-8")))
        data.seek(0)
        log.info("Successfully read the SQL file on S3")
    except ClientError as e:
        msg = 'Error reading object: {}/{}'.format(bucket, key)
        log.error(msg)
        raise e
    return data


if __name__ == '__main__':
    account_id = get_account_id()
    print(account_id)
    region = get_region()
    print(region)
