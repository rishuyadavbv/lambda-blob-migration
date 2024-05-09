import boto3
import json
import requests
import logging

# Initialize SQS client
sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/549050352176/blobMigrationQueue'

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

# Base URL for image storage service
base_url = 'https://image-storage-service.us-east-1.qa.internal.curalate.com/v1'

# Function to process message based on method type
def process_message(method: str, message: dict) -> None:
    """Process message based on method type."""
    try:
        if method in METHOD_MAPPING:
            METHOD_MAPPING[method](message)
        else:
            logger.error(f"Unsupported method: {method}")
    except Exception as e:
        logger.error(f"Error processing message with method '{method}': {e}")

# Function to send PUT request
def send_put_request(message: dict) -> None:
    """Send PUT request."""
    try:
        # Extract required data from message
        request_url = message.get('requestUrl')
        tenant_name = message.get('tenantName')
        blob_id = message.get('blobId')
        table = message.get('table')
        attributes = message.get('attributes')
        base64_data = message.get('data')

        if not (request_url and tenant_name and blob_id):
            raise ValueError("Missing required data in the message")

        # Request body
        payload = {
            "url": request_url,
            "tenantName": tenant_name,
            "bypassCache": True,
            "includeMetadata": True,
            "overrideHeaders": {},
            "preferProxy": True
        }
        endpoint = f"{base_url}/digestUrl"
        # Send POST request
        response = requests.post(endpoint, json=payload)

        # Check response status
        if response.status_code == 200:
            logger.info(f"PUT request sent for blobId: {blob_id}")
        else:
            logger.error(f"Failed to send PUT request. Status code: {response.status_code}, Reason: {response.text}")

    except ValueError as ve:
        logger.error(f"Error sending PUT request: {ve}")
    except requests.RequestException as re:
        logger.error(f"Error sending PUT request: {re}")
    except Exception as e:
        logger.error(f"Error sending PUT request: {e}")

# Function to send DELETE request
def send_delete_request(message: dict) -> None:
    """Send DELETE request."""
    try:
        table = message['table']
        blobId = message['blobId']
        table_parts = table.split(':')
        if len(table_parts) != 2:
            raise ValueError("Invalid table format. Must be in the format 'clientname:tablename'")
        
        # Extract the table name and client name
        table_name = table_parts[0].strip()
        client_name = table_parts[1].strip()
        
        # Implement logic to send DELETE request
        params = {'tableName': table_name, 'clientName': client_name, 'blobId': blobId}
        response = requests.delete(base_url, params=params)
        
        if response.status_code == 200:
            logger.info(f"DELETE request sent successfully for blobId: {blobId}")
        else:
            logger.error(f"Failed to send DELETE request. Status code: {response.status_code}, Response: {response.text}")
    
    except ValueError as e:
        logger.error("ValueError: {}".format(e))
    except Exception as e:
        logger.error(f"Error sending DELETE request: {e}")

# Function to send CREATE TABLE request
def send_create_table_request(message: dict) -> None:
    """Send CREATE TABLE request."""
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
def send_delete_table_request(message: dict) -> None:
    """Send DELETE TABLE request."""
    try:
        table = message['table']
        audit = message['audit']

        # Implement logic to send DELETE TABLE request

        table_parts= table.split(':')
        table_name = table_parts[0].strip()
        client_name = table_parts[1].strip()
        params= {'tableName': table_name, 'clientName': client_name} 
        response = requests.delete(base_url, params=params)
        if(response.status_code ==200):
            logger.info(f"DELETE TABLE request sent for table: {table}")
        else:
            logger.error(f"Failed to send DELETE TABLE request. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logger.error(f"Error sending DELETE TABLE request: {e}")

# Function to send PURGE TABLE request
def send_purge_table_request(message: dict) -> None:
    """Send PURGE TABLE request."""
    try:
        table = message['table']
        audit = message['audit']
        # Implement logic to send PURGE TABLE request
        logger.info(f"PURGE TABLE request sent for table: {table}")
    except Exception as e:
        logger.error(f"Error sending PURGE TABLE request: {e}")

# Function to send SET TABLE ATTRIBUTE request
def send_set_table_attribute_request(message: dict) -> None:
    """Send SET TABLE ATTRIBUTE request."""
    try:
        table = message['table']
        attributes = message['attributes']
        audit = message['audit']
        # Implement logic to send SET TABLE ATTRIBUTE request
        logger.info(f"SET TABLE ATTRIBUTE request sent for table: {table}")
    except Exception as e:
        logger.error(f"Error sending SET TABLE ATTRIBUTE request: {e}")


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
        logger.info("Messages fetched from SQS")

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

# Entry point
if __name__ == "__main__":
    lambda_handler(None, None)
