WITH DuplicateRows AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY Date, Description, Amount ORDER BY id) AS RowNum
    FROM
        ModifiedTransactions
)
DELETE FROM DuplicateRows
WHERE
    RowNum > 1;