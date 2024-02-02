# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver

import boto3
import os 
import botocore
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import json


"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


if __name__ == '__main__':

    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
	        driver.run(sys.argv[1], 'vcf')

        result_bucket = 'gas-results'
        # example of argv[1]: '/home/ubuntu/jobs/userX~12234566~filename'
        arguments = str(sys.argv[1]).split('/')
        job_name = str(arguments[-1])
        job_id = str(job_name.split('~')[1])
        file_name = str(job_name.split('~')[2])
        bucket_prefix = 'yueqil/'
        user_id='userX'
        job_prefix = job_name.partition('.')[0]

        # example of result dir: yueqil/userX
        result_dir = bucket_prefix+user_id

        # example of jobs_dir: /home/ubuntu/jobs
        jobs_dir = './jobs'

        # Validate job details
        if not job_id or not job_name or not job_prefix:
            print('Invalid file path')
            sys.exit(1)

        s3_client = boto3.client('s3', region_name='us-east-1')

        # File names for results and logs
        result_file_name = f"{job_prefix}.annot.vcf"
        log_file_name = f"{job_prefix}.vcf.count.log"

        # Define S3 keys
        s3_key_result_file = f"{result_dir}/{result_file_name}"
        s3_key_log_file = f"{result_dir}/{log_file_name}"

        # 1. Upload the files to S3 results bucket
        try:
            s3_client.upload_file(f"{jobs_dir}/{result_file_name}", result_bucket, s3_key_result_file)
            s3_client.upload_file(f"{jobs_dir}/{log_file_name}", result_bucket, s3_key_log_file)
        except ClientError as e:
            print(f"Failed uploading result file: {e}")
            sys.exit(1)

        # 2. Update DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('yueqil_annotations')

        try:
            complete_time = int(time.time())
            response = table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET job_status = :status, s3_results_bucket = :rb, s3_key_result_file = :rf, s3_key_log_file = :lf, complete_time = :ct',
                ExpressionAttributeValues={
                    ':status': 'COMPLETED',
                    ':rb': result_bucket,
                    ':rf': s3_key_result_file,
                    ':lf': s3_key_log_file,
                    ':ct': complete_time
                }
            )
        except ClientError as e:
            print(f"Failed to update DynamoDB: {e}")
            sys.exit(1)

        # 3. Clean up local job files
        try:
            os.remove(f"{jobs_dir}/{job_name}")
            os.remove(f"{jobs_dir}/{result_file_name}")
            os.remove(f"{jobs_dir}/{log_file_name}")
        except OSError as e:
            print(f"Error during file cleanup: {e}")

    else:
        print("A valid .vcf file must be provided as input to this program.")

