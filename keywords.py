import pandas as pd
import pyodbc
import hashlib


def load_mapping(filename):
    """Loads keyword-category mapping from an Excel file."""
    try:
        df = pd.read_excel(filename) # Load Excel file
        df.columns = df.columns.str.strip() # Strip spaces from column names
    except FileNotFoundError:
        print(f"Error: Mapping file '{filename}' not found.") 
        return None
    except pd.errors.ParserError:
        print(
            f"Error: Could not parse mapping file '{filename}'. Check the file format."
        )
        return None
    except KeyError as e:
        print(f"Error: Required column '{e}' not found in mapping file.")
        return None

    mapping = {} # Initialize empty dictionary
    for index, row in df.iterrows(): # Iterate over rows
        keyword = row["Keyword"].lower() # Convert to lowercase
        category = row["Category"] # Get category
        subcategory = row["Subcategory"] # Get subcategory
        mapping[keyword] = (category, subcategory)  # Use only keyword as key
    return mapping


def categorize_transactions(transactions_df, mapping):
    if "Description" not in transactions_df.columns:
        print("Error: 'Description' column not found in the transactions DataFrame.")
        return transactions_df, False
    transactions_df["Category"] = ""
    transactions_df["Subcategory"] = ""
    updated_mapping = False

    if mapping is None:
        return transactions_df, updated_mapping

    for index, row in transactions_df.iterrows():
        description = row["Description"].lower()
        matched = False
        for keyword, (category, subcategory) in mapping.items():
            if keyword in description:  # Only keyword matching
                transactions_df.at[index, "Category"] = category
                transactions_df.at[index, "Subcategory"] = subcategory  # Updated line
                matched = True
                break

        if not matched:
            print(f"\nNo category found for description: '{row['Description']}'")
            keyword = (
                input("Enter keyword to associate with this description: ")
                .strip()
                .lower()
            )
            category = input("Enter category for this keyword: ").strip()
            subcategory = input("Enter _ for this keyword: ").strip()
            if keyword and category and subcategory:
                mapping[keyword] = (category, subcategory)
                updated_mapping = True
                print("Mapping updated.")
            else:
                print("Mapping not updated, please provide all three values.")
    return transactions_df, updated_mapping


def modify_csv_and_write_to_db(input_file, output_file, mapping_file,
                              server, database, username, password):
    """
    Modifies a CSV file according to specified transformations
    and writes the modified data to an MSSqlserver database.

    Args:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to save the modified CSV file.
        mapping_file (str): Path to the Excel file containing keyword-category mappings.
        server (str): Name of the MSSqlserver server.
        database (str): Name of the database to connect to.
        username (str): Username for database authentication.
        password (str): Password for database authentication.

    Returns:
        None
    """
    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        return
    except pd.errors.ParserError:
        print(
            f"Error: Could not parse input file '{input_file}'. Check the file format."
        )
        return

    # Strip spaces from column names
    df.columns = df.columns.str.strip()

    try:
        # Ensure 'Type' column exists before proceeding
        if "Type" not in df.columns:
            print(f"Error: 'Type' column not found in the input CSV file.")
            return

        df = df[["Date", "Description", "Amount", "Type"]]
    except KeyError as e:
        print(f"Error: Column '{e}' not found in the input file.")
        return

    try:
        df["Month"] = pd.to_datetime(df["Date"], format="%Y-%m-%d").dt.strftime("%B")
    except ValueError:
        print(
            "Error: Could not parse dates. Please ensure the date format is YYYY-MM-DD."
        )
        return

    # Make amounts non-negative
    df["Amount"] = df["Amount"].abs()  # Convert to positive values

    mapping = load_mapping(mapping_file)
    df, updated_map = categorize_transactions(df, mapping)

    if updated_map:
        try:
            mapping_df = pd.DataFrame.from_dict(
                mapping, orient="index", columns=["Category", "Subcategory"]
            )
            mapping_df.index.name = "Keyword"  # set index name
            mapping_df.to_excel(mapping_file, index=True)
            print(f"Mapping file '{mapping_file}' updated successfully.")
        except Exception as e:
            print(f"Error updating mapping file: {e}")

    df.insert(0, "", "")
    df.insert(1, "Month", df.pop("Month"))
    df.insert(4, "$", "$")

    # Rename columns
    df = df[
        ["", "Month", "Date", "Description", "$", "Amount", "Category", "Subcategory"]
    ]

    try:
        df.to_csv(output_file, index=False)
        print(f"File successfully modified and saved as '{output_file}'")
    except Exception as e:
        print(f"An error occurred while saving the file: {e}")

    try:
        # Connect to the MSSqlserver database
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};"
            f"DATABASE={database};UID={username};PWD={password};"
            f"Encrypt=no;TrustServerCertificate=yes"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Define SQL Server table name (adjust as needed)
        table_name = "ModifiedTransactions"

        # Create table and add unique constraint if not exists
        create_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
            BEGIN
                CREATE TABLE {table_name} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    Month VARCHAR(255),
                    Date DATE,
                    Description TEXT,
                    Amount DECIMAL(10, 2),
                    Category VARCHAR(255),
                    Subcategory VARCHAR(255),
                    TransactionHash VARCHAR(255) UNIQUE
                )
            END
        """
        cursor.execute(create_table_sql)

        # Delete existing duplicates (optional, if you've already done it)
        # delete_duplicates_sql = """
        #     WITH DuplicateRows AS (
        #         SELECT
        #             ROW_NUMBER() OVER (PARTITION BY Date, Amount ORDER BY id) AS RowNum
        #         FROM
        #             ModifiedTransactions
        #     )
        #     DELETE FROM DuplicateRows
        #     WHERE
        #         RowNum > 1;
        # """
        # cursor.execute(delete_duplicates_sql)

        # Insert data into the table
        for _, row in df.iterrows():
            month = row["Month"]
            date = row["Date"]
            description = row["Description"]
            amount = row["Amount"]
            category = row["Category"]
            subcategory = row["Subcategory"]

            # Calculate hash
            transaction_str = f"{date}{description}{amount}"
            transaction_hash = hashlib.sha256(transaction_str.encode()).hexdigest()

            cursor.execute(
                f"INSERT INTO {table_name} ([Month], [Date], [Description], [Amount], [Category], [Subcategory], [TransactionHash]) "
                f"VALUES (?,?,?,?,?,?,?)",
                month, date, description, amount, category, subcategory, transaction_hash
            )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Data successfully written to MSSqlserver database.")

    except pyodbc.Error as e:
        # Extract SQLSTATE for more specific error handling
        if len(e.args) > 0:
            sqlstate = e.args[0]
            if sqlstate == '08001':  # Certificate verification error
                print(f"Certificate verification failed: {e}")
                print("Check server certificate or adjust TrustServerCertificate setting.")
            elif sqlstate == '28000':  # Authentication error
                print(f"Authentication failed: {e}")
                print("Verify username and password.")
            elif sqlstate.startswith('08'):  # General connection error
                print(f"Connection error: {e}")
                print("Check server connectivity, network issues, or firewall rules.")
            else:
                print(f"Database error: {e}")
        else:
            print(f"Database error: {e}")


# Example usage (replace with your file names)
input_csv_file = "/home/dcapps/Documents/SourceCode/Keyword-Categorization/transactions.csv"
output_csv_file = "modified_transactions.csv"
mapping_excel_file = "keyword_mapping.xlsx"  # Your mapping file

modify_csv_and_write_to_db(
    input_file=input_csv_file, 
    output_file=output_csv_file, 
    mapping_file=mapping_excel_file, 
    server="naildc-srv1", 
    database="PerFin",
    username="sa", 
    password="sqlserver1!"
)

# End of keywords.py
