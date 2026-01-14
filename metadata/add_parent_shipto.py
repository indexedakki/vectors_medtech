import pandas as pd

def add_parent_shipto(
    excel_file_shipto_parent,
    excel_file_metadata,
    COL_NAME_PARENT,
    COL_NAME_SHIPTO,
    COL_NAME_METADATA_SHIPTO,
    COL_NAME_METADATA_PARENT,
):
    # Load data
    df_shipto_parent = pd.read_excel(excel_file_shipto_parent, sheet_name="Exploded", engine="openpyxl", dtype=str)
    df_metadata = pd.read_excel(excel_file_metadata, engine="openpyxl", dtype=str)

    # Normalize column names (remove case sensitivity and spaces)
    df_shipto_parent.columns = df_shipto_parent.columns.str.strip()
    df_metadata.columns = df_metadata.columns.str.strip()

    # Create a mapping from SHIPTO UCN to Parent UCN
    shipto_to_parent = df_shipto_parent[[COL_NAME_PARENT, COL_NAME_SHIPTO]].dropna().drop_duplicates()
    shipto_to_parent_dict = shipto_to_parent.set_index(COL_NAME_SHIPTO)[COL_NAME_PARENT].to_dict()

    # Add 0s padding to COL_NAME_METADATA_SHIPTO so it's 8 characters long
    df_metadata[COL_NAME_METADATA_SHIPTO] = df_metadata[COL_NAME_METADATA_SHIPTO].str.zfill(8)
    
    # Map Parent UCN to metadata DataFrame based on SHIPTO UCN
    df_metadata[COL_NAME_METADATA_PARENT] = df_metadata[COL_NAME_METADATA_SHIPTO].map(shipto_to_parent_dict)
    print("✅ Parent UCNs added to metadata DataFrame")
    
    with pd.ExcelWriter(excel_file_metadata, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_metadata.to_excel(writer, index=False, sheet_name="Metadata_With_Parent_UCN")

# Usage example
EXCEL_FILE_SHIPTO_PARENT = "metadata/UCN Distinct Output.xlsx"
EXCEL_FILE_METADATA = "metadata/processed_metadata_iter_4.xlsx"
COL_NAME_PARENT = "Parent UCN"
COL_NAME_SHIPTO = "SHIPTO UCN"
COL_NAME_METADATA_PARENT = "Parent_UCN"
COL_NAME_METADATA_SHIPTO = "UCN"
add_parent_shipto(
    excel_file_shipto_parent=EXCEL_FILE_SHIPTO_PARENT,
    excel_file_metadata=EXCEL_FILE_METADATA,
    COL_NAME_PARENT=COL_NAME_PARENT,
    COL_NAME_SHIPTO=COL_NAME_SHIPTO,
    COL_NAME_METADATA_SHIPTO=COL_NAME_METADATA_SHIPTO,
    COL_NAME_METADATA_PARENT=COL_NAME_METADATA_PARENT,
)