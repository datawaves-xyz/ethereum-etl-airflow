CREATE TABLE IF NOT EXISTS {{params.database}}.blocks
(
    number            BIGINT,
    hash              STRING,
    parent_hash       STRING,
    nonce             STRING,
    sha3_uncles       STRING,
    logs_bloom        STRING,
    transactions_root STRING,
    state_root        STRING,
    receipts_root     STRING,
    miner             STRING,
    difficulty        DECIMAL(38, 0),
    total_difficulty  DECIMAL(38, 0),
    size              BIGINT,
    extra_data        STRING,
    gas_limit         BIGINT,
    gas_used          BIGINT,
    timestamp         BIGINT,
    transaction_count BIGINT,
    base_fee_per_gas  BIGINT
) USING json
OPTIONS (
    path "{{params.file_path}}"
)
PARTITIONED BY (block_date STRING);