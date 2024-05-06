import boto3
import json
import requests
import logging

# Initialize SQS client
sqs = boto3.client('sqs')
queue_url = 'QUEUE_URL'

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


base_url = 'API_BASE_URL'

# Mapping of method types to corresponding functions
METHOD_MAPPING = {
    'PUT_TABLE_BLOBID': send_put_request,
    'DELETE_BLOB': send_delete_request,
    'CREATE_TABLE': send_create_table_request,
    'DELETE_TABLE': send_delete_table_request,
    'PURGE_TABLE': send_purge_table_request,
    'SET_TABLE_ATTRIBUTE': send_set_table_attribute_request
}

# Lambda handler function
def lambda_handler(event, context):
    try:
        messages = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=60,
            WaitTimeSeconds=20
        )

        if 'Messages' in messages:
            for message in messages['Messages']:
                try:
                    body = json.loads(message['Body'])
                    method = body.get('method')
                    if method:
                        process_message(method, body)
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
    except Exception as e:
        logger.error(f"Error receiving messages from SQS: {e}")

# Function to process message based on method type
def process_message(method, message):
    try:
        if method in METHOD_MAPPING:
            METHOD_MAPPING[method](message)
        else:
            logger.error(f"Unsupported method: {method}")
    except Exception as e:
        logger.error(f"Error processing message with method '{method}': {e}")

# Function to send PUT request
def send_put_request(message):
    try:
        table = message['table']
        blobId = message['blobId']
        attributes = message['attributes']
        base64Data = message['data']
        # Implement logic to send PUT request
        logger.info(f"PUT request sent for blobId: {blobId}")
    except Exception as e:
        logger.error(f"Error sending PUT request: {e}")

# Function to send DELETE request
def send_delete_request(message):
    try:
        table = message['table']
        blobId = message['blobId']
        # Implement logic to send DELETE request
        logger.info(f"DELETE request sent for blobId: {blobId}")
    except Exception as e:
        logger.error(f"Error sending DELETE request: {e}")

# Function to send CREATE TABLE request
def send_create_table_request(message):
    try:
        table = message['table']
        options = message['options']
        attributes = message['attributes']
        audit = message['audit']
        # Implement logic to send CREATE TABLE request
        logger.info(f"CREATE TABLE request sent for table: {table}")
    except Exception as e:
        logger.error(f"Error sending CREATE TABLE request: {e}")

# Function to send DELETE TABLE request
def send_delete_table_request(message):
    try:
        table = message['table']
        audit = message['audit']
        # Implement logic to send DELETE TABLE request
        logger.info(f"DELETE TABLE request sent for table: {table}")
    except Exception as e:
        logger.error(f"Error sending DELETE TABLE request: {e}")

# Function to send PURGE TABLE request
def send_purge_table_request(message):
    try:
        table = message['table']
        audit = message['audit']
        # Implement logic to send PURGE TABLE request
        logger.info(f"PURGE TABLE request sent for table: {table}")
    except Exception as e:
        logger.error(f"Error sending PURGE TABLE request: {e}")

# Function to send SET TABLE ATTRIBUTE request
def send_set_table_attribute_request(message):
    try:
        table = message['table']
        attributes = message['attributes']
        audit = message['audit']
        # Implement logic to send SET TABLE ATTRIBUTE request
        logger.info(f"SET TABLE ATTRIBUTE request sent for table: {table}")
    except Exception as e:
        logger.error(f"Error sending SET TABLE ATTRIBUTE request: {e}")
