# ETH Flow Data Pipeline

## Overview
The **ETH Flow Data Pipeline** project aims to make Ethereum (ETH) transaction data more accessible to end users by transforming raw blockchain data into user-friendly formats like JSON or Parquet. While Ethereum transaction data is theoretically open-access, retrieving it requires setting up an archive node, which can be complex. This project simplifies that process by providing both raw data and analytically prepared metrics for independent analysis.

Beyond raw data, this pipeline processes and aggregates ETH transaction data into meaningful metrics to assess network performance and transaction activity (see [Business Requirements](#business-requirements)).

## Objectives
- Provide ETH transaction and block data in accessible formats (e.g., JSON, Parquet).
- Deliver analytically prepared datasets with calculated metrics for deeper insights.
- Enable users to perform their own analyses on Ethereum network activity.

## Business Requirements
The pipeline focuses on two main areas of analysis:

### 1. Network Performance
- **Number of daily transactions**: Total transactions processed per day.
- **Number of active wallets**: Count of unique wallet addresses interacting with the network daily.
- **Top miner**: The miner with the most blocks mined in a given period.
- **Daily miner dominance**: Percentage of blocks mined by the top miner daily.

### 2. Transaction Analysis
- **Transaction types**: Breakdown of transactions by type (e.g., standard transfers, contract interactions).
- **Popular contract addresses**: Identify frequently interacted contract addresses (if feasible).
- **Average transactions per address**: Mean number of transactions per active wallet.
- **Average transaction value**: Mean value (in ETH) per transaction.
- **Number of new contracts deployed**: Count of newly created smart contracts.
- **Total gas used**: Aggregate gas consumed by transactions.
- **Average gas price per transaction/block**: Mean gas price in wei.
- **Largest transaction (ETH)**: Transaction with the highest ETH value transferred.

## Data Source
The pipeline retrieves data exclusively from the Ethereum blockchain via the **Infura API**. Specifically:
- **Endpoint**: `eth_getBlockByNumber`
- **Configuration**: The "show transaction details" flag is set to `true` to fetch both block and transaction data in a single request, optimizing resource use.
- **API Documentation**:
  - [Block Data](https://docs.metamask.io/guide/ethereum-api.html#eth-getblockbynumber) (`eth_getBlockByNumber`)
  - [Transaction Data](https://docs.metamask.io/guide/ethereum-api.html#eth-gettransactionbyhash) (`eth_getTransactionByHash`)

## Data Structure
The pipeline processes two primary data types: **Block Data** and **Transaction Data**.

### Block Data
Contains metadata about Ethereum blocks. Key fields retained for analysis:

| Field         | Type           | Description                           | Retained |
|---------------|----------------|---------------------------------------|----------|
| `hash`        | 32 bytes       | Unique block hash                    | Yes      |
| `number`      | Integer        | Block number                         | Yes      |
| `timestamp`   | Unix timestamp | Block creation time                  | Yes      |
| `gasLimit`    | Hexadecimal    | Maximum gas allowed                  | Yes      |
| `gasUsed`     | Hexadecimal    | Total gas used by transactions       | Yes      |
| `miner`       | 20 bytes       | Address of the miner                 | Yes      |
| `parentHash`  | 32 bytes       | Hash of the parent block             | Yes      |
| `size`        | Hexadecimal    | Block size in bytes                  | Yes      |

Fields like `difficulty`, `extraData`, and `logsBloom` are excluded as they are not relevant to the current objectives.

### Transaction Data
Details individual transactions within blocks. Key fields retained:

| Field                  | Type           | Description                           | Retained |
|-----------------------|----------------|---------------------------------------|----------|
| `hash`                | 32 bytes       | Transaction hash                     | Yes      |
| `blockNumber`         | Integer        | Block containing the transaction     | Yes      |
| `from`                | 20 bytes       | Sender address                       | Yes      |
| `to`                  | 20 bytes       | Receiver address (null for contracts)| Yes      |
| `value`               | Integer        | Value transferred (in wei)           | Yes      |
| `gas`                 | Integer        | Gas provided by sender               | Yes      |
| `gasPrice`            | Integer        | Gas price (in wei)                   | Yes      |
| `input`               | String         | Transaction data payload             | Yes      |
| `maxPriorityFeePerGas`| Integer        | Max priority fee (EIP-1559)          | Yes      |
| `maxFeePerGas`        | Integer        | Max total fee (EIP-1559)             | Yes      |
| `transactionIndex`    | Hexadecimal    | Position in block                    | Yes      |
| `type`                | String         | Transaction type                     | Yes      |

Fields like `nonce`, `r`, and `s` (signature components) are excluded as they are not critical for this analysis.

## Metrics
The pipeline generates the following precomputed metrics:
- **`Transaction_count`**: Total number of transactions.
- **`Total_eth_transferred`**: Aggregate ETH value transferred.
- **`Block_count_daily`**: Number of blocks mined per day.
- **`Avg_gas_prices_wei`**: Average gas price in wei.
- **`Top_miners`**: Ranking of miners by blocks mined.
- **`Largest_transactions`**: Transactions with the highest ETH values.
- **`Transactions_by_type`**: Distribution of transaction types.
- **`Total_gas_used`**: Sum of gas consumed across transactions.
- **`Active_wallet`**: Count of unique active wallet addresses.



