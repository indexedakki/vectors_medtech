import pandas as pd
import re

def add_agreement_amendment_record_no_column(
    excel_file_metadata,
    col_name_related_rec
):
    # Load data
    df_metadata = pd.read_excel(
        excel_file_metadata,
        engine="openpyxl",
        dtype=str
    )

    # Normalize column names
    df_metadata.columns = df_metadata.columns.str.strip()

    # Column names
    PRODUCT_COL = "Product_Agreement_Rec"
    AMENDMENT_COL = "Amendments_Rec"

    # 🔹 Drop columns if they already exist
    df_metadata = df_metadata.drop(
        columns=[col for col in [PRODUCT_COL, AMENDMENT_COL] if col in df_metadata.columns],
        errors="ignore"
    )
    
    # Regex for record numbers
    record_no_pattern = re.compile(r"\d{9}~\d{8}(?:\.\d{3})?")

    product_agreement_col = []
    amendment_col = []

    for _, row in df_metadata.iterrows():
        related_to = row.get(col_name_related_rec, "")

        if pd.isna(related_to):
            product_agreement_col.append("")
            amendment_col.append("")
            continue

        product_agreements = []
        amendments = []

        # Split by Excel line breaks
        lines = re.split(r"_x000D_|\n", related_to)

        for line in lines:
            record_nos = record_no_pattern.findall(line)

            for record_no in record_nos:
                if re.search(r"\bAdd Prod Agree\b", line, re.IGNORECASE):
                    product_agreements.append(record_no)
                else:
                    amendments.append(record_no)

        product_agreement_col.append(", ".join(product_agreements))
        amendment_col.append(", ".join(amendments))

    # Find index of related_records column
    insert_idx = df_metadata.columns.get_loc(col_name_related_rec) + 1

    # Insert columns right next to related_records
    df_metadata.insert(insert_idx, PRODUCT_COL, product_agreement_col)
    df_metadata.insert(insert_idx + 1, AMENDMENT_COL, amendment_col)

    # Drop all columns which are completely empty
    df_metadata = df_metadata.dropna(axis=1, how="all")

    # Save if output path provided
    with pd.ExcelWriter(excel_file_metadata, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_metadata.to_excel(writer, index=False, sheet_name="Metadata")

    print(f"✅ Added 'product_agreement' and 'amendments' columns to metadata: {excel_file_metadata}")
    
# Usage example
EXCEL_FILE_METADATA = "metadata/processed_metadata_iter_4.xlsx"
COL_NAME_RELATED_REC = "Related_Records"
add_agreement_amendment_record_no_column(
    excel_file_metadata=EXCEL_FILE_METADATA,
    col_name_related_rec=COL_NAME_RELATED_REC
)