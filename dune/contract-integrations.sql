--contract integrations - finding contracts that have called another contract
--we don't look at contract reads, only contracts that modify state
--in this example we look at the calls made to Opensea's Seaport contract 

SELECT * FROM ethereum.traces 
WHERE tx_hash = 0x3f9b47d171734422aa375f0216d19d0f829a418051756f61652cb348dcd837b6
AND block_number = 15741695 --make the query run faster
AND to = 0x00000000006c3852cbef3e08e8df289169ede581 --the seaport address


--get all of the times that seaport was called in the past hour 
SELECT * FROM ethereum.traces 
WHERE to = 0x00000000006c3852cbef3e08e8df289169ede581 --the seaport address
AND block_time > now() - interval '1' hour
AND cardinality(trace_address) != 0 --if trace_address is empty, that means the call was made from a wallet, so we want to exclude these calls as we are only looking at contracts 
AND success --get only the successful calls 


--get the number of distinct contracts that called Seaport in the last hour 
SELECT approx_distinct('from') FROM ethereum.traces 
WHERE to = 0x00000000006c3852cbef3e08e8df289169ede581 --the seaport address
AND block_time > now() - interval '1' hour
AND cardinality(trace_address) != 0 --if trace_address is empty, that means the call was made from a wallet, so we want to exclude these calls as we are only looking at contracts 
AND success --get only the successful calls 


--for every week, how many times was each integration called? 
SELECT 
    date_trunc('week', block_time) as week,
    c.name as integration_type, 
    approx_distinct(tx_hash) as times_called
FROM ethereum.traces tr
LEFT JOIN labels.contracts c ON c.address = tr."from"
WHERE to = 0x00000000006c3852cbef3e08e8df289169ede581 --the seaport address
AND block_time > now() - interval '1' month
AND cardinality(trace_address) != 0 --if trace_address is empty, that means the call was made from a wallet, so we want to exclude these calls as we are only looking at contracts 
AND success --get only the successful calls 
AND lower(c.name) NOT LIKE '%gnosis%' AND lower(c.name) NOT LIKE '%argent%' --we dont want wallet or safe contract addresses
AND c.blockchain LIKE 'ethereum'
GROUP BY 1, 2

