# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from etl_runner import EtlRunner
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ETL Driver")
    parser.add_argument("--step_id", required=True, type=str, dest="step_name",
                        help="Step id (which is combination of job_name and step_name) within the job which needs to be executed")
    parser.add_argument("--job_run_id", required=True, type=str, dest="job_run_id",
                        help="Unique execution identifier from job runner")
    parser.add_argument("--sql_parameters", required=False, type=str, dest="params",
                        help="Optional string in the format of key=value,key=value of parameters to pass to spark SQL")
    parser.add_argument("--code_bucket", required=False, type=str, dest="code_bucket",
                        help="Name of the bucket specific to environment where the job sql's, libraries are stored")
    parser.add_argument("--metadata_table", required=False, type=str, dest="metadata_table",
                        help="DynamoDB Table name specific to environment which stores the step level info for a job")
    parser.add_argument("--log_table_name", required=False, type=str, dest="log_table_name",
                        help="DynamoDB Table which stores the execution details of all the SQL's in a job.")
    args = parser.parse_args()

    default_params = {}

    if "params" in args:
        for keypair in args.params.split("::"):
            valarr = keypair.split("=")
            if len(valarr) != 2:
                raise ValueError(f"The parameter set {keypair} is not valid. Ensure it meets the 'key=value' format")
            default_params[valarr[0]] = valarr[1]

    etlrunner = EtlRunner(job_run_id=args.job_run_id, step_id=args.step_name, code_bucket= args.code_bucket, metadata_table=args.metadata_table, log_table_name=args.log_table_name)
    etlrunner.preprocessing(extra_params = default_params)
    etlrunner.start(extra_params=default_params)
