from datetime import datetime, timezone
import requests
import json
import duckdb
import os
from typing import Dict, Any, Optional
import sys
import logging

# Constants
INFURA_URL = "https://mainnet.infura.io/v3/"  # Add your Infura project ID here
HEADERS = {"Content-Type": "application/json"}
CHECKPOINT_FILE = "checkpoint.duckdb"
MAX_SIZE_BYTES = 100 * 1024 * 1024  # 100 MB in bytes


# Configure basic logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Define output format
)


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


def initialize_checkpoint_db():
    # Connect to DuckDB and create a table if it doesn't exist
    conn = duckdb.connect(CHECKPOINT_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS checkpoint (
            block_number INTEGER
        )
    """)
    # Check if table is empty, insert 0 if so
    result = conn.execute("SELECT COUNT(*) FROM checkpoint").fetchone()[0]
    if result == 0:
        conn.execute("INSERT INTO checkpoint (block_number) VALUES (0)")
    conn.close()


def save_checkpoint(max_id):
    # Save the new max_id, replacing the old value
    conn = duckdb.connect(CHECKPOINT_FILE)
    conn.execute("DELETE FROM checkpoint")  # Clear existing data
    conn.execute("INSERT INTO checkpoint (block_number) VALUES (?)", (max_id,))
    conn.close()


def load_checkpoint():
    # Load the max_id, default to 0 if not found
    conn = duckdb.connect(CHECKPOINT_FILE)
    result = conn.execute("SELECT block_number FROM checkpoint").fetchone()
    conn.close()
    return result[0] if result else None


def safe_hex_to_int(hex_str: Optional[str], default: Any = None) -> Optional[int]:
    """Safely convert a hex string to an integer, returning default if None."""
    return int(hex_str, 16) if hex_str is not None else default


def fetch_latest_block_number(url: str, headers: Dict[str, str]) -> int:
    """Fetch the latest Ethereum block number from the Infura API."""
    payload = {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    response.raise_for_status()  # Raise an exception for HTTP errors
    data = response.json()
    if "result" not in data:
        raise ValueError("No 'result' in response: ", data)
    return safe_hex_to_int(data["result"])


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
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    response.raise_for_status()
    data = response.json()
    if "result" not in data:
        raise ValueError("No 'result' in response: ", data)
    return data["result"]


def convert_transaction(tx: Dict[str, str]) -> Dict[str, Any]:
    """Convert transaction data to a human-readable format."""
    return {
        "accessList": tx.get("accessList", []),
        "blockHash": tx.get("blockHash"),
        "blockNumber": safe_hex_to_int(tx.get("blockNumber")),
        "chainId": safe_hex_to_int(tx.get("chainId")),
        "from": tx["from"],
        "gas": safe_hex_to_int(tx["gas"]),
        "gasPrice": safe_hex_to_int(tx.get("gasPrice")),
        "gasPrice_gwei": safe_hex_to_int(tx.get("gasPrice")) / 1e9
        if tx.get("gasPrice")
        else None,
        "hash": tx["hash"],
        "input": tx["input"],
        "maxFeePerGas": safe_hex_to_int(tx.get("maxFeePerGas")),
        "maxFeePerGas_gwei": safe_hex_to_int(tx.get("maxFeePerGas")) / 1e9
        if tx.get("maxFeePerGas")
        else None,
        "maxPriorityFeePerGas": safe_hex_to_int(tx.get("maxPriorityFeePerGas")),
        "maxPriorityFeePerGas_gwei": safe_hex_to_int(tx.get("maxPriorityFeePerGas"))
        / 1e9
        if tx.get("maxPriorityFeePerGas")
        else None,
        "nonce": safe_hex_to_int(tx["nonce"]),
        "r": tx["r"],
        "s": tx["s"],
        "to": tx.get("to"),
        "transactionIndex": safe_hex_to_int(tx.get("transactionIndex")),
        "type": safe_hex_to_int(tx["type"]),
        "v": safe_hex_to_int(tx["v"]),
        "value": safe_hex_to_int(tx["value"]),
        "value_eth": safe_hex_to_int(tx["value"]) / 1e18
        if tx["value"] is not None
        else None,
        "yParity": safe_hex_to_int(tx.get("yParity")),
    }


def convert_block_data(block_data: Dict[str, str]) -> Dict[str, Any]:
    """Convert block data to a human-readable format."""
    logging.info(block_data.keys())
    return {
        "baseFeePerGas": safe_hex_to_int(block_data["baseFeePerGas"]),
        "baseFeePerGas_gwei": safe_hex_to_int(block_data["baseFeePerGas"]) / 1e9,
        "blobGasUsed": safe_hex_to_int(block_data["blobGasUsed"]),
        "difficulty": safe_hex_to_int(block_data["difficulty"]),
        "excessBlobGas": safe_hex_to_int(block_data["excessBlobGas"]),
        "extraData": bytes.fromhex(block_data["extraData"][2:]).decode(
            "ascii", errors="ignore"
        ),
        "gasLimit": safe_hex_to_int(block_data["gasLimit"]),
        "gasUsed": safe_hex_to_int(block_data["gasUsed"]),
        "hash": block_data["hash"],
        "logsBloom": block_data["logsBloom"],
        "miner": block_data["miner"],
        "mixHash": block_data["mixHash"],
        "nonce": safe_hex_to_int(block_data["nonce"]),
        "number": safe_hex_to_int(block_data["number"]),
        "parentBeaconBlockRoot": block_data["parentBeaconBlockRoot"],
        "parentHash": block_data["parentHash"],
        "receiptsRoot": block_data["receiptsRoot"],
        "sha3Uncles": block_data["sha3Uncles"],
        "size": safe_hex_to_int(block_data["size"]),
        "stateRoot": block_data["stateRoot"],
        "timestamp": safe_hex_to_int(block_data["timestamp"]),
        "timestamp_readable": datetime.fromtimestamp(
            safe_hex_to_int(block_data["timestamp"]), tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "transactions": [convert_transaction(tx) for tx in block_data["transactions"]],
    }


def save_to_json(data: Dict[str, Any], file_path: str = "data.json") -> None:
    """Save data to a JSON file with indentation."""
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        logging.info("Drirectory does not exist, created new!")
        os.makedirs(directory, exist_ok=True)

    with open(file_path, "w") as file:
        json.dump(data, file, indent=4)


def main():
    try:
        job_start_time = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
        blocks_data = []
        transactions_data = []
        blocks_file_index = 1
        transactions_file_index = 1

        initialize_checkpoint_db()
        url = (
            INFURA_URL + ""
        )  # Replace with your Infura Project ID

        latest_block_number = fetch_latest_block_number(url, HEADERS)
        previous_block_number = load_checkpoint()
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
            block_data = fetch_block_data(url, hex(current_block_number), HEADERS)
            readable_data = convert_block_data(block_data)

            # Append transactions and block data
            transactions_data.extend(readable_data["transactions"])
            del readable_data["transactions"]
            blocks_data.append(readable_data)  # Changed to append single block

            # Check sizes and save if necessary
            blocks_size = get_size(blocks_data)
            transactions_size = get_size(transactions_data)

            if blocks_size >= MAX_SIZE_BYTES:
                save_to_json(blocks_data, f"blocks_{blocks_file_index}.json")
                logging.info(
                    f"Saved blocks_{blocks_file_index}.json ({blocks_size / (1024 * 1024):.2f} MB)"
                )
                blocks_data = []
                blocks_file_index += 1

            if transactions_size >= MAX_SIZE_BYTES:
                save_to_json(
                    transactions_data, f"transactions_{transactions_file_index}.json"
                )
                logging.info(
                    f"Saved transactions_{transactions_file_index}.json ({transactions_size / (1024 * 1024):.2f} MB)"
                )
                transactions_data = []
                transactions_file_index += 1

            current_block_number += 1

        # Save any remaining data
        if blocks_data:
            save_to_json(
                blocks_data,
                f"./output/{job_start_time}/blocks_{blocks_file_index}.json",
            )
            logging.info(f"Saved blocks_{blocks_file_index}.json")
        if transactions_data:
            save_to_json(
                transactions_data,
                f"./output/{job_start_time}/transactions_{transactions_file_index}.json",
            )
            logging.info(f"Saved transactions_{transactions_file_index}.json")

        save_checkpoint(latest_block_number)
        logging.info("Checkpoint updated")

    except requests.RequestException as e:
        logging.info(f"Error fetching data from Infura: {e}")
    except ValueError as e:
        logging.info(f"Error processing response: {e}")
    except Exception as e:
        logging.info(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
