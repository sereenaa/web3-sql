-- select * from nft.transfers 
-- where to = {{address}} 
-- and token_standard = 'erc721'
-- and blockchain = '{{chain}}'
-- limit 1


--We need to order by block_number and evt_index to get the last transfer of the single token (the current person owning it)
with 
    last_held as (
        SELECT 
            tr.*, 
            row_number() over(partition by contract_address, token_id order by tr.block_number desc, evt_index desc) as last_transfer
        FROM nft.transfers tr 
        WHERE blockchain = '{{chain}}'
        AND token_standard = 'erc721'
    ), 
    
    last_held_user as (
        SELECT 
            lh.*
        FROM last_held lh 
        WHERE lh.last_transfer = 1 
        AND lh.to = {{address}}
    )
    
SELECT
    t.symbol, 
    t.name, 
    t.category, 
    lhu.token_id, 
    lhu.contract_address, 
    tx.value/1e18 as acquisition_value_native, 
    case 
        when tx."from" <> {{address}} 
            then 'gifted' --if signer != receiver, that means it was airdropped/transferred 
        when lhu."from" = 0x0000000000000000000000000000000000000000
            then 'minted'
        else 'bought'
        end as acquired_how, 
    lhu.block_number as acquired_on_block_number, 
    lhu.tx_hash
FROM last_held_user lhu
LEFT JOIN {{chain}}.transactions tx ON tx.hash = lhu.tx_hash
LEFT JOIN tokens.nft t ON t.contract_address = lhu.contract_address AND t.blockchain = '{{chain}}'



-- select * from nft.transfers 
-- where to = {{address}} 
-- and token_standard = 'erc721'
-- and blockchain = '{{chain}}'
-- limit 1



with 
    --We need to order by block_number and evt_index to get the last transfer of the single token (the current person owning it)
    last_held as (
        SELECT 
            tr.*, 
            row_number() over(partition by contract_address, token_id order by tr.block_number desc, evt_index desc) as last_transfer
        FROM nft.transfers tr 
        WHERE blockchain = '{{chain}}'
        AND token_standard = 'erc721'
    ), 
    
    last_held_user as (
        SELECT 
            lh.*
        FROM last_held lh 
        WHERE lh.last_transfer = 1 
        AND lh.to = {{address}}
    ), 

    total_amount as (
        SELECT 
            count(tf.amount) as n_items, 
            txn.value/1e18 as total_amount, 
            tf.tx_hash, 
            tf.block_number
        FROM nft.transfers tf 
        LEFT JOIN ethereum.transactions txn on tf.tx_hash = txn.hash
        GROUP BY 2, 3, 4
    ) 
    
SELECT
    t.symbol, 
    t.name, 
    t.category, 
    lhu.token_id, 
    lhu.contract_address, 
    case 
        when tx."from" <> {{address}} 
            then 0
        when lhu."from" = 0x0000000000000000000000000000000000000000
            then ta.total_amount/ta.n_items
        else td.amount_original 
        end as acquisition_value_native, 
    case 
        when tx."from" <> {{address}} and tx."to" = 0x000000000000ad05ccc4f10045630fb830b95127 then 'blur'
        when tx."from" <> {{address}} then 'gifted' --if signer != receiver, that means it was airdropped/transferred 
        when lhu."from" = 0x0000000000000000000000000000000000000000 then 'minted'
        else 'bought'
        end as acquired_how, 
    lhu.block_number as acquired_on_block_number, 
    lhu.tx_hash
FROM last_held_user lhu
LEFT JOIN {{chain}}.transactions tx ON tx.hash = lhu.tx_hash
LEFT JOIN tokens.nft t ON t.contract_address = lhu.contract_address AND t.blockchain = '{{chain}}'
LEFT JOIN nft.trades td ON lhu.tx_hash = td.tx_hash AND lhu.contract_address = td.nft_contract_address AND CAST(lhu.token_id AS VARCHAR) = td.token_id
LEFT JOIN total_amount ta ON ta.tx_hash = lhu.tx_hash AND ta.block_number = lhu.block_number






select * from nft.transfers where tx_hash = 0x1a9f51fa4c46c31b6aefe0d69002122a002405f23a1690e74e5537ba89b1f6aa





with total_amount as (
    select 
        count(tf.amount) as n_items, 
        txn.value/1e18 as total_amount, 
        tf.tx_hash, 
        tf.block_number
    from nft.transfers tf 
    left join ethereum.transactions txn on tf.tx_hash = txn.hash
    where tf.tx_hash = 0x1a9f51fa4c46c31b6aefe0d69002122a002405f23a1690e74e5537ba89b1f6aa
    and tf.block_number = 14084708
    group by 2, 3, 4
) 
SELECT 
    total_amount/n_items as individual_cost, 
    tx_hash, 
    block_number
FROM total_amount
