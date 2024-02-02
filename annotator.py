from flask import Flask, request, jsonify, render_template
import subprocess
import uuid
import os
import json
import time
import threading
import shutil
import boto3
import datetime 
from botocore.client import Config
from botocore.exceptions import ParamValidationError
from botocore.exceptions import ClientError
from botocore.exceptions import BotoCoreError
from botocore.exceptions import NoCredentialsError

app = Flask(__name__)

annotator = os.path.expanduser("~/anntools/run.py")
data_path = os.path.expanduser("~/anntools/data/")
jobs_path= "./jobs/"


@app.route('/annotations',methods=['POST'])
def annotations():  
    data = request.json
    if not data:
        return jsonify({'code': 400, 'status': 'error', 'message': 'No data provided.'}), 400
    
    # Extract job parameters from request body
    bucket_name = data.get('bucket')
    s3_key = data.get('key')
    job_id = data.get('job_id')

    if not bucket_name or not s3_key or not job_id:
        return jsonify({'code': 400, 'status': 'error', 'message': 'Missing required data in request.'}), 400

    s3_client = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yueqil_annotations')

    # Extract 'userX' from the S3 key
    parts = s3_key.split('/')
    if len(parts) < 3:
        return jsonify({'code': 400, 'status': 'error', 'message': 'Invalid S3 key format.'}), 400
  
    user_part = parts[1] # This should be 'userX'
    file_name = user_part + '~' + parts[-1] # 'userX~[unique-id]~[filename]'
    job_id = parts[-1].split('~')[0] #[unique-id]
    input_file = parts[-1].split('~')[1] # filename

    # Get the input file S3 object and copy it to a local file
    try:
        # Define the download path
        download_path = os.path.join(jobs_path, file_name)

        # Create jobs directory if it doesn't exist 
        if not os.path.exists(jobs_path):
            os.makedirs(jobs_path)

        # Download the file from S3
        s3_client.download_file(bucket_name,s3_key,download_path)
    except (NoCredentialsError, ClientError) as e:
        return jsonify({'code': 500, 'status': 'error', 'message': str(e)}), 500

    # Launch annotation job as a background process
    # Run the AnnTools command
    try:
        ann_process = subprocess.Popen(["python", annotator, jobs_path + file_name])
        
        # Update job status in DynamoDB conditionally
        response = table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET job_status = :val',
            ConditionExpression='job_status = :status',
            ExpressionAttributeValues={':val': 'RUNNING',':status': 'PENDING'},
            ReturnValues="UPDATED_NEW"
        )
    except BotoCoreError as e:
        # Handle the specific DynamoDB error (e.g., ConditionalCheckFailedException)
        return jsonify({'code': 500, 'status': 'error', 'message': 'DynamoDB error: ' + str(e)}), 500
    except subprocess.CalledProcessError:
        return jsonify({'code': 500, 'status': 'error', 'message': 'An error occurred in the annotator process. Please review the input data and try again.'}), 500
    except Exception as e:
        return jsonify({'code': 500, 'status': 'error', 'message': str(e)}), 500


    # Return response to notify user of successful submission
    return jsonify({
        'code':201,
        'status':'success',
        'data':{
            'job_id':job_id,
            'input_file':input_file
        }
    }), 201

# Run the app server
app.run(host = '0.0.0.0', debug = True)