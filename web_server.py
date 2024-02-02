from flask import Flask, request, jsonify, render_template, redirect, url_for
import subprocess
import uuid
import os
import json
import time
import threading
import shutil
import boto3
import datetime 
import requests
from botocore.client import Config
from botocore.exceptions import ParamValidationError
from botocore.exceptions import ClientError
from botocore.exceptions import BotoCoreError


app = Flask(__name__)

@app.route('/annotate', methods=['GET'])
def annotate():
    # AWS Credentials
    aws_region = 'us-east-1'
    bucket_name = 'gas-inputs'
    user_id = 'userX'
    bucket_prefix = 'yueqil/'

    # Create an S3 client with SgiV4 configuration
    s3_client = boto3.client(
        's3',
        region_name = aws_region,
        config=Config(signature_version='s3v4')
    )

    # Generate a unique ID for the file upload
    unique_id = str(uuid.uuid4())

    # S3 key
    key_name = f"{bucket_prefix}{user_id}/{unique_id}~${{filename}}"

    # Generate the base URL dynamically
    base_url = request.url_root
    redirect_url = url_for('annotate_job', _external=True)  # Dynamically constructs redirect URL

    try:
        expiration = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)  # 5 minute expiry
        conditions = [
            {"bucket": bucket_name},
            {"acl": "private"},
            ["starts-with", "$key", f"{bucket_prefix}{user_id}/{unique_id}~"],
            {"success_action_redirect": redirect_url},
            ["content-length-range", 0, 10485760],  # 10 MB limit
        ]

        post_data = s3_client.generate_presigned_post(
            Bucket= bucket_name,
            Key=key_name,
            Fields={"acl": "private", "success_action_redirect": redirect_url},
            Conditions = conditions,
            ExpiresIn = 600 # Valid for 5 minutes
        )
    except ParamValidationError as e:
        print("Parameter validation failed in 'generate_presigned_post()':{}".format(e))
        return jsonify({"code":400, "status": "error", "message": "Parameter validation error: " + str(e)}), 400
    except ClientError as e:
        print("A client side error occurred:{}".format(e))
        return jsonify({"code":500, "status": "error", "message": "Failed to generate signed request: " + str(e)}), 500
    except Exception as e:
        print("An unexpected error occured:{}".format(e))
        return jsonify({"code":500, "status": "error", "message": "Failed to generate signed request: " + str(e)}), 500

    return render_template("annotate.html", data=post_data, bucket_name=bucket_name)

@app.route("/annotate/job", methods=['GET'])
def annotate_job():

    # Extract query parameters from the redirect URL
    bucket_name = request.args.get('bucket')
    key = request.args.get('key')

    # DynamoDB setup
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('yueqil_annotations')

    # Convert current time to epoch time (in seconds)
    submit_time = int(time.time())

    # Create a job item for DynamoDB
    item ={
        "job_id":key.split('/')[2].split('~')[0],
        "user_id":key.split('/')[1],
        "input_file_name": key.split('/')[-1],
        "s3_key_input_file" : key,
        "submit_time": submit_time,
        "job_status":"PENDING"
    }

    try:
        table.put_item(Item = item)
    except Exception as e:
        return jsonify({"code": 500, "status": "error", "message": str(e)}), 500

    data ={
        "job_id":key.split('/')[2],
        "bucket": bucket_name,
        "key": key
    }

    # POST job request to the annotator
    ann_url = 'http://yueqil-a8-ann.ucmpcs.org:5000/annotations'
    ann_job_response = requests.post(
        ann_url,
        json=data,
        timeout=10
    )

    # Check if the response from annotator is OK
    if ann_job_response.ok:
        return (
            jsonify(
                {
                    "code": 201,
                    "status": "success",
                    "data": ann_job_response.json()["data"]
                }
            ),
            201,
        )
    else:
        # Handle unsuccessful response
        return (
            jsonify(
                {
                    "code": 500,
                    "status": "error",
                    "message": "Annotator service failed",
                    "details": ann_job_response.text  # or .json() as appropriate
                }
            ),
            500,
        )

  
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)



