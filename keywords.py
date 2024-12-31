import pandas as pd


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
    transactions_df["Sub-Category"] = ""
    updated_mapping = False

    if mapping is None:
        return transactions_df, updated_mapping

    for index, row in transactions_df.iterrows():
        description = row["Description"].lower()
        matched = False
        for keyword, (category, subcategory) in mapping.items():
            if keyword in description:  # Only keyword matching
                transactions_df.at[index, "Category"] = category
                transactions_df.at[index, "Sub-Category"] = subcategory
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
            subcategory = input("Enter sub-category for this keyword: ").strip()
            if keyword and category and subcategory:
                mapping[keyword] = (category, subcategory)
                updated_mapping = True
                print("Mapping updated.")
            else:
                print("Mapping not updated, please provide all three values.")
    return transactions_df, updated_mapping


def modify_csv(input_file, output_file, mapping_file):
    """Modifies a CSV file according to specified transformations."""
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

    df = df[
        ["", "Month", "Date", "Description", "$", "Amount", "Category", "Sub-Category"]
    ]

    try:
        df.to_csv(output_file, index=False)
        print(f"File successfully modified and saved as '{output_file}'")
    except Exception as e:
        print(f"An error occurred while saving the file: {e}")


# Example usage (replace with your file names)
input_csv_file = "/home/dcapps/Documents/SourceCode/Keyword-Categorization/transactions.csv"
output_csv_file = "modified_transactions.csv"
mapping_excel_file = "keyword_mapping.xlsx"  # Your mapping file

modify_csv(input_csv_file, output_csv_file, mapping_excel_file)
# End of keywords.py
