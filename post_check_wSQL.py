import pandas as pd
import sqlite3
import os

# Delete the file if it exists
if os.path.exists("transaction_data_compared.xlsx"):
    os.remove("transaction_data_compared.xlsx")

# Connect to the on-disk SQLite database
conn = sqlite3.connect('keywords.sqlite')

# Construct the SQL query using the retrieved column names
query = f"""
SELECT
  T1."Transaction Detail",
  T1.Category,
  T1."Sub-Category",
  T2.CATEGORY AS EXPECTED_CATEGORY,
  T2.Subcategory AS EXPECTED_SUBCATEGORY,
  T2.Keyword,
  CASE WHEN LOWER(T1.Category) = LOWER(T2.CATEGORY) THEN 'TRUE' ELSE 'FALSE' END AS CATEGORY_MATCH,
  CASE WHEN LOWER(T1."Sub-Category") = LOWER(T2.Subcategory) THEN 'TRUE' ELSE 'FALSE' END AS SUBCATEGORY_MATCH
FROM `transaction-detail` AS T1
INNER JOIN keywords AS T2
  ON LOWER(T2.Keyword) IN (
    SELECT
      LOWER(T2.Keyword)
    FROM keywords AS T2
    WHERE
      T2.CATEGORY <> '' AND T2.Subcategory <> '' AND INSTR(LOWER(T1."Transaction Detail"), LOWER(T2.Keyword)) > 0
  )ORDER BY
  CATEGORY_MATCH,
  SUBCATEGORY_MATCH;
"""

result_df = pd.read_sql_query(query, conn)

# Save the results to a new Excel file
result_df.to_excel("transaction_data_compared.xlsx", index=False)

# Close the database connection
conn.close()