import json
import boto3
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter, Retry
import os
from typing import Dict, Any, Optional
import sys
import logging

# import requests


s3 = boto3.client('s3')
ssm = boto3.client('ssm')
BUCKET = ssm.get_parameter(Name='/infura_integration/bucket')['Parameter']['Value']
INFURA_KEY = ssm.get_parameter(Name='/infura_integration/infura_key', WithDecryption=True)['Parameter']['Value']
KEY = "last_block.json"
HEADERS = {"Content-Type": "application/json"}
INFURA_URL = "https://mainnet.infura.io/v3/" 
MAX_SIZE_BYTES = 20 * 1024 * 1024  # 50 MB in bytes
s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504, 429])
s.mount('https://', HTTPAdapter(max_retries=retries))
def get_last_block() -> int:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=KEY)
        return json.loads(obj['Body'].read())['last_block']
    except s3.exceptions.NoSuchKey:
        return None
def save_last_block(block_number: int):
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=json.dumps({'last_block': block_number}))
    
def save_blocks_data(block_data: dict, file_name):
    s3.put_object(Bucket=BUCKET, Key=file_name, Body=json.dumps(block_data))
    
def get_size(obj, seen=None):
    """Recursively calculate the size of an object in bytes."""
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)

    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        size += sum(get_size(k, seen) + get_size(v, seen) for k, v in obj.items())
    elif isinstance(obj, (list, tuple, set)):
        size += sum(get_size(item, seen) for item in obj)
    return size

def safe_hex_to_int(hex_str: Optional[str], default: Any = None) -> Optional[int]:
    """Safely convert a hex string to an integer, returning default if None."""
    return int(hex_str, 16) if hex_str is not None else default

def fetch_latest_block_number(url: str, headers: Dict[str, str]) -> int:
    """Fetch the latest Ethereum block number from the Infura API."""
    payload = {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}
    response = s.post(url, data=json.dumps(payload), headers=headers)
    response.raise_for_status()  # Raise an exception for HTTP errors
    data = response.json()
    if "result" not in data:
        raise ValueError("No 'result' in response: ", data)
    return safe_hex_to_int(data["result"])

def fetch_block_data_with_rate_limit_retry(url, block_number_hex, headers, session):
    while True:
        try:
            return fetch_block_data(url, block_number_hex, headers)
        except HTTPError as e:
            if e.response.status_code == 429:
                reset_time_str = e.response.headers.get('X-RateLimit-Reset')
                if reset_time_str:
                    reset_time = int(reset_time_str)
                    current_time = int(time.time())
                    wait_time = reset_time - current_time
                    if wait_time > 0:
                        logging.info(f"Rate limit reset in {wait_time} seconds. Waiting...")
                        time.sleep(wait_time)
                    continue
                else:
                    logging.info("Rate limit hit, waiting 60 seconds...")
                    time.sleep(60)
                    continue
            else:
                raise
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise

def fetch_block_data(
    url: str, block_number_hex: str, headers: Dict[str, str]
) -> Dict[str, Any]:
    """Fetch block data by block number from the Infura API, including transactions."""
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [block_number_hex, True],  # True includes full transaction objects
        "id": 1,
    }
    response = s.post(url, data=json.dumps(payload), headers=headers)
    response.raise_for_status()
    data = response.json()
    if "result" not in data:
        raise ValueError("No 'result' in response: ", data)
    return data["result"]




def lambda_handler(event, context):
    url = INFURA_URL + INFURA_KEY
    blocks_data = []
    transactions_data = []
    blocks_file_index = 1
    transactions_file_index = 1
    try:
        job_start_time = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
        previous_block_number = get_last_block()
        latest_block_number = fetch_latest_block_number(url, HEADERS)
        logging.info(f"Previous block number: {previous_block_number}")
        logging.info(f"Latest block number: {latest_block_number}")
        if not previous_block_number:
            logging.info(
                "No checkpoint found!\n Getting data from latest block instead."
            )
            current_block_number = latest_block_number
        else:
            current_block_number = previous_block_number + 1

        while current_block_number <= latest_block_number:
            logging.info(f"Getting block {current_block_number}")
            block_data = fetch_block_data_with_rate_limit_retry(url, hex(current_block_number), HEADERS, s)
            blocks_data.append(block_data)
            blocks_size = get_size(blocks_data)
            logging.info(f"Current block data size {blocks_size / (1024 * 1024):.2f} MB")
            if blocks_size >= MAX_SIZE_BYTES:
                file_name = f"blocks/blocks_{job_start_time}_{blocks_file_index}.json"
                save_blocks_data(blocks_data, file_name)
                logging.info(
                    f"Saved blocks_{job_start_time}_{blocks_file_index}.json ({blocks_size / (1024 * 1024):.2f} MB)"
                )
                save_last_block(current_block_number)
                blocks_data = []
                blocks_file_index += 1
            current_block_number += 1
        # Save any remaining data
        if blocks_data:
            file_name = f"blocks/blocks_{job_start_time}_{blocks_file_index}.json"
            save_blocks_data(blocks_data, file_name)
        save_last_block(latest_block_number)
        return {
            'statusCode': 200,
            'body': json.dumps({'last_block': latest_block_number})
        }
    except HTTPError as e:
        status_code = e.response.status_code
        if status_code == 429:
            logging.error(f"Rate limit exceeded: {e}")
            return {
                'statusCode': 429,
                'body': json.dumps({'error': 'Rate limit exceeded', 'message': str(e)})
            }
        else:
            logging.error(f"HTTP error {status_code}: {e}")
            return {
                'statusCode': status_code,
                'body': json.dumps({'error': f'HTTP error {status_code}', 'message': str(e)})
            }
    except RequestException as e:
        logging.error(f"Request error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Request error', 'message': str(e)})
        }
    except ValueError as e:
        logging.error(f"Error processing response: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error processing response', 'message': str(e)})
        }
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Unexpected error', 'message': str(e)})
    }