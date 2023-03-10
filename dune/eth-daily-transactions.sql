
SELECT
  SUBSTRING(TRY_CAST (block_time AS VARCHAR(10)), 1, 10) AS day,
  COUNT(*)
FROM
  ethereum.transactions
GROUP BY
  1
ORDER BY
  day DESC
LIMIT
  100