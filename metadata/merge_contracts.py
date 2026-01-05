import pandas as pd

def merge_contracts(excel_file_1, excel_file_2, sheet_name_1, sheet_name_2, COL1, COL2):
    df_extracted = pd.read_excel(excel_file_1, sheet_name=sheet_name_1, engine="openpyxl")
    df_regents_joe = pd.read_excel(excel_file_2, sheet_name=sheet_name_2, engine="openpyxl")
    
    # Normalize columns
    df_extracted.columns = df_extracted.columns.str.strip()
    df_regents_joe.columns = df_regents_joe.columns.str.strip()
    
    # Find common records
    merged = df_extracted[df_extracted[COL1].isin(df_regents_joe[COL2])].copy()

    # Find missing records after merge with active records
    missing = df_regents_joe[~df_regents_joe[COL2].isin(df_extracted[COL1])].copy()

    # Only consider missing if record number ends with .xxx
    missing = missing[missing[COL2].astype(str).str.contains(r'\.\d{3}$')]
    
    print(f"\n🟢 Total matching (found): {len(merged)}")
    print(f"🔴 Total missing in TRIM: {len(missing)}")
    
    if len(missing) > 0:
        print("\nMissing values:")
        for val in missing[COL2].tolist():
            print(" ❗", val)

    # Save merged and missing data into workbook
    with pd.ExcelWriter(excel_file_1, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        merged.to_excel(writer, sheet_name="Metadata", index=False)
        missing.to_excel(writer, sheet_name="Missing Records", index=False)

    print(f"\n✔ Merged saved to Metadata")
    print(f"✔ Missing records saved to 'Missing Records' sheet")

# # Usage example
# EXCEL_FILE = "processed_metadata_iter_1.xlsx"
# SHEET_NAME_1 = "TRIM"
# SHEET_NAME_2 = "Active Records Joe"
# COL1 = "Article_Number"
# COL2 = "Record Number"

# merge_contracts(EXCEL_FILE, SHEET_NAME_1, SHEET_NAME_2, COL1, COL2)
