--0x2C606a9Ab697d6B8fC870317087f8DB1AcAEB7d4
with 
    transfer_in as (
        SELECT 
            sum(cast(value as double)/1e18) as eth_in --107.25
        FROM ethereum.traces
        --you could also use transfers_ethereum.eth or transfers_optimism.eth and only filter on tx_to instead
        WHERE to = {{address}}
        AND type = 'call'
        AND success
        AND (call_type NOT IN ('delegatecall', 'staticcall', 'callcode') OR call_type is null)
        AND cast(value as double) > 0
    )
    
    , transfer_out as (
        SELECT 
            sum(cast(value as double)/1e18) as eth_out --100.89
        FROM ethereum.traces
        --you could also use transfers_ethereum.eth or transfers_optimism.eth and only filter on tx_from instead
        WHERE "from" = {{address}}
        AND type = 'call'
        AND success
        AND (call_type NOT IN ('delegatecall', 'staticcall', 'callcode') OR call_type is null)
        AND cast(value as double) > 0
    )
    
    , gas_spent as (
        SELECT 
            SUM((gas_price_gwei*gas_used)/1e9) as gas_spent
        FROM gas.fees
        WHERE blockchain = 'ethereum'
        AND tx_sender = {{address}}
    )
    
SELECT
*
, eth_in - COALESCE(eth_out,0) - COALESCE(gas_spent,0) as balance
FROM transfer_in tin 
LEFT JOIN transfer_out tout ON 1=1
LEFT JOIN gas_spent gs ON 1=1
