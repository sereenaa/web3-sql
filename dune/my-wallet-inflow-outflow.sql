-- get the amount of net transfers of each protocol for my wallet 

with transfers_in AS (
    SELECT 
        "from" AS protocol, 
        sum(cast(value as double)/1e18) AS total_value_in, 
        count(*) AS num_transfers_in
    FROM {{chain}}.traces 
    WHERE to = {{address}}
        AND success=true
        AND type='call'
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
        AND cast(value as double)/1e18 > 0
    GROUP BY 1
), 

transfers_out AS (
    SELECT 
        to AS protocol, 
        sum(cast(value as double)/1e18) AS total_value_out, 
        count(*) as num_transfers_out
    FROM ethereum.traces 
    WHERE "from" = {{address}}
        AND success=true
        AND type='call'
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
        AND cast(value as double)/1e18 > 0
    GROUP BY 1
), 

net_transfers AS (
    SELECT
        COALESCE(tfi.protocol, tfo.protocol) AS protocol, 
        COALESCE(tfi.total_value_in, 0) - COALESCE(tfo.total_value_out, 0) AS net_flow, 
        COALESCE(tfi.num_transfers_in, 0) + COALESCE(tfo.num_transfers_out, 0) AS num_transfers
    FROM transfers_in tfi
    FULL OUTER JOIN transfers_out tfo
        ON tfi.protocol = tfo.protocol 

)
SELECT 
    c.name AS protocol_name, 
    nt.*
FROM net_transfers nt 
LEFT JOIN labels.contracts c ON nt.protocol = c.address and c.blockchain = '{{chain}}'
ORDER BY net_flow DESC
LIMIT 1000

--0x7be8076f4ea4a4ad08075c2508e481d6c946d12b