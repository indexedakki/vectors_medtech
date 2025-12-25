import json 
import pandas as pd

def extract_IDN_by_UCN(excel_path: str, UCN: list):
    """
    Filter rows where M_SUPER_PARNT_UNI_CUST_NO is in the given UCN list.
    """
    # Force string dtype to preserve leading zeros
    df = pd.read_excel(excel_path, dtype=str)

    # Normalize
    df["M_SUPER_PARNT_UNI_CUST_NO"] = (
        df["M_SUPER_PARNT_UNI_CUST_NO"]
        .astype(str)
        .str.strip()
    )

    # Normalize input UCN list
    ucn_set = {str(u).strip() for u in UCN}

    # Filter rows
    filtered_df = df[df["M_SUPER_PARNT_UNI_CUST_NO"].isin(ucn_set)]

    # Output to Excel
    with pd.ExcelWriter("IDN.xlsx", engine="openpyxl") as writer:
        filtered_df.to_excel(writer, index=False, sheet_name="Filtered_IDN")

    print(f"Filtered data written to 'IDN.xlsx' with {len(filtered_df)} records.")

UCN = ["01018471", "01018845", "01030242"]
excel_path = "IDN Full Explosion Report 11.24.2025.xlsx"
ucn_to_idn = extract_IDN_by_UCN(excel_path, UCN)