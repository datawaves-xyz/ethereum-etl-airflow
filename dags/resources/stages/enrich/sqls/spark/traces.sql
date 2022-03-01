SELECT
    traces.transaction_hash,
    traces.transaction_index,
    traces.from_address,
    traces.to_address,
    traces.value,
    traces.input,
    traces.output,
    traces.trace_type,
    traces.call_type,
    traces.reward_type,
    traces.gas,
    traces.gas_used,
    traces.subtraces,
    traces.trace_address,
    traces.error,
    traces.status,
    traces.trace_id,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash,
    TO_DATE('{{ds}}') as dt
FROM {{database_temp}}.blocks_{{ds_in_table}} AS blocks
    JOIN {{database_temp}}.traces_{{ds_in_table}} AS traces ON blocks.number = traces.block_number