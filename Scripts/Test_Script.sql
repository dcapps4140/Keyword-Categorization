SELECT
  T1.Category ,  -- CATEGORY
  T1."Sub-Category",  -- SUB-CATEGORY
  T2.CATEGORY AS EXPECTED_CATEGORY,
  T2.Subcategory AS EXPECTED_SUBCATEGORY,
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
  );