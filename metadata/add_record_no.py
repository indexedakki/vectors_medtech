import pandas as pd

def add_parent_shipto(
    excel_file_1,
    excel_file_2,
    COL_NAME_1,
    COL_NAME_2,
    ARTICLE_NUMBER
):
    # Load data
    df_1 = pd.read_excel(
        excel_file_1,
        sheet_name="in",
        engine="openpyxl",
        dtype=str
    )

    df_2 = pd.read_excel(
        excel_file_2,
        engine="openpyxl",
        dtype=str
    )

    # Normalize column names
    df_1.columns = df_1.columns.str.strip()
    df_2.columns = df_2.columns.str.strip()
    
    # Make .PDF to .pdf for Filename column
    if COL_NAME_2=="FileName":
        df_2[COL_NAME_2] = df_2[COL_NAME_2].str.replace(".PDF", ".pdf", regex=False)

    # Build mapping (take FIRST Article Number per ICS)
    mapping = (
        df_2
        .dropna(subset=[COL_NAME_2, ARTICLE_NUMBER])
        .drop_duplicates(subset=[COL_NAME_2])
        .set_index(COL_NAME_2)[ARTICLE_NUMBER]
    )

    # Map → SAFE one-to-one assignment
    df_1[ARTICLE_NUMBER] = df_1[COL_NAME_1].map(mapping)
    
    with pd.ExcelWriter(excel_file_1, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_1.to_excel(writer, index=False, sheet_name="in")
    print("✅ Article Numbers added to metadata DataFrame")

# Usage example
EXCEL_FILE_1 = "metadata/lexora_metadata.xlsx"
EXCEL_FILE_2 = "metadata/processed_metadata_iter_4.xlsx"
COL_NAME_1 = "cntrc_id"
COL_NAME_2 = "ICS"
COL_NAME_1 = "Trim Number"
COL_NAME_2 = "FileName"
ARTICLE_NUMBER = "Article_Number"
add_parent_shipto(
    excel_file_1=EXCEL_FILE_1,
    excel_file_2=EXCEL_FILE_2,
    COL_NAME_1=COL_NAME_1,
    COL_NAME_2=COL_NAME_2,
    ARTICLE_NUMBER=ARTICLE_NUMBER,
)