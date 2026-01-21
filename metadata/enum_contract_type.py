import pandas as pd

def enum_contract_type(
    excel_file: str,
    sheet_name: str,
    column_name: str = "Enum_Contract_Type"
) -> list:
    # Load data
    df = pd.read_excel(
        excel_file,
        sheet_name=sheet_name,
        engine="openpyxl",
        dtype=str
    )

    # Normalize column names
    df.columns = df.columns.str.strip()

    # Filter Record_Type to to 'CONTRACT COMMERCIAL DOCUMENT'
    master_df = df[df["Record_Type"] == "CONTRACT COMMERCIAL DOCUMENT"]
    
    # Populate Enum_Contract_Type with 'Master Agreement' where Article_Number ends with .001
    master_df[column_name] = master_df["Article_Number"].apply(
        lambda x: "Master Agreement" if isinstance(x, str) and x.strip().endswith(".001") else None
    )
    
    # Populate Enum_Contract_Type with 'Master Amendment' for remaining rows 
    master_df[column_name] = master_df[column_name].fillna("Master Amendment")
    
    # Replace Enum_Contract_Type with 'Independent Agreement' for rows where Contract_Type does not starts with 'MASTER'
    master_df.loc[
        ~master_df["Contract_Type"].str.startswith("MASTER", na=False),
        column_name
    ] = "Independent Agreement"
    
    # Filter Record_Type back to "CONTRACT PA DOCUMENT"
    product_df = df[df["Record_Type"] == "CONTRACT PA DOCUMENT"]
    
    # Populate Enum_Contract_Type with 'Product Agreement' based on Title, if "Add Prod Agree" is in Title
    product_df[column_name] = product_df["Title"].apply(
        lambda x: "Product Agreement" if isinstance(x, str) and "Add Prod Agree" in x else None
    )

    # Populate Enum_Contract_Type with 'Product Amendment' for remaining rows
    product_df[column_name] = product_df[column_name].fillna("Product Amendment")

    # Combine dataframes in same order as original
    result_df = pd.concat([master_df, product_df]).sort_index()
    
    # Save updated DataFrame back to Excel
    with pd.ExcelWriter(excel_file, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        result_df.to_excel(writer, sheet_name="sheet_name", index=False)
    
    print(f"✅ Enum_Contract_Type updated and saved to {excel_file} → sheet '{sheet_name}'")

enum_contract_type(
    excel_file="metadata/processed_metadata_iter_3.xlsx",
    sheet_name="Metadata"
)