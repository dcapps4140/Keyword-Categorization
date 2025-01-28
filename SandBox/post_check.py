import pandas as pd
import os

# Delete the file if it exists
if os.path.exists("transaction_data_compared.xlsx"):
    os.remove("transaction_data_compared.xlsx")

# Load the data
df = pd.read_excel("book1.xlsx")

# Strip spaces from column names
df.columns = [col.replace(" ", "") for col in df.columns]

# Convert the `TRANSACTIONDETAIL` column to string
df['TRANSACTIONDETAIL'] = df['TRANSACTIONDETAIL'].astype(str)

# Drop the empty `Unnamed: 3` column
df = df.drop(columns=['Unnamed:3'])

# Create a dictionary from the keyword mapping
keyword_dict = dict(zip(df["Keyword"], zip(df["Category"], df["Subcategory"])))

# Function to categorize transactions
def categorize_transaction(description):
    for keyword, (category, subcategory) in keyword_dict.items():
        if keyword.lower() in description.lower():
            return category, subcategory
    return None, None

# Apply the function to the transaction log
df[["EXPECTED_CATEGORY", "EXPECTED_SUB-CATEGORY"]] = df["TRANSACTIONDETAIL"].apply(lambda x: pd.Series(categorize_transaction(x)))

# Normalize text in `Category` and `EXPECTED_CATEGORY` columns
df['Category'] = df['Category'].str.lower().str.replace('[^a-zA-Z0-9]', '', regex=True).str.strip()
df['EXPECTED_CATEGORY'] = df['EXPECTED_CATEGORY'].str.lower().str.replace('[^a-zA-Z0-9]', '', regex=True).str.strip()
df['Subcategory'] = df['Subcategory'].str.lower().str.strip()
df['EXPECTED_SUB-CATEGORY'] = df['EXPECTED_SUB-CATEGORY'].str.lower().str.strip()

print(df.head(100))
# Remove non-printable characters
df['Category'] = df['Category'].apply(lambda x: ''.join(ch for ch in x if ch.isprintable()))
df['EXPECTED_CATEGORY'] = df['EXPECTED_CATEGORY'].apply(lambda x: ''.join(ch for ch in x if ch.isprintable()))

# Compare the categories
df["CATEGORY_MATCH"] = df["Category"] == df["EXPECTED_CATEGORY"]
df["SUBCATEGORY_MATCH"] = df["Subcategory"] == df["EXPECTED_SUB-CATEGORY"]

# Convert boolean values to text
df["CATEGORY_MATCH"] = df["CATEGORY_MATCH"].map({True: 'TRUE', False: 'FALSE'})
df["SUBCATEGORY_MATCH"] = df["SUBCATEGORY_MATCH"].map({True: 'TRUE', False: 'FALSE'})

# Save the updated data
df.to_excel("transaction_data_compared.xlsx", index=False)