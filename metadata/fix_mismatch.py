import pandas as pd
from fuzzywuzzy import fuzz

def fuzzy_match(str1: str, str2: str, threshold: int = 70) -> bool:
    if not str1 or not str2:
        return False

    s1 = str(str1).strip().lower()
    s2 = str(str2).strip().lower()

    if s1 == s2:
        return True

    scores = [
        fuzz.ratio(s1, s2),
        fuzz.partial_ratio(s1, s2),
        fuzz.token_sort_ratio(s1, s2),
        fuzz.token_set_ratio(s1, s2),
    ]

    return max(scores) >= threshold

def sync_processed_with_golden(
    golden_path,
    processed_path,
    golden_sheet,
    processed_sheet,
    golden_key,
    processed_key,
    column_map,
    output_path
):
    # Load
    df_golden = pd.read_excel(golden_path, sheet_name=golden_sheet, dtype=str)
    df_proc = pd.read_excel(processed_path, sheet_name=processed_sheet, dtype=str)

    print(f"Dropped duplicates: Golden from {len(df_golden)} to {len(df_golden.drop_duplicates(subset=[golden_key]))}, Processed from {len(df_proc)} to {len(df_proc.drop_duplicates(subset=[processed_key]))}")
    # Drop duplicates
    df_golden = df_golden.drop_duplicates(subset=[golden_key])
    df_proc = df_proc.drop_duplicates(subset=[processed_key])
    
    # Normalize
    for df in (df_golden, df_proc):
        df.columns = df.columns.str.strip()
        df[:] = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Index golden for fast lookup
    golden_indexed = df_golden.set_index(golden_key)

    fixes_applied = 0

    for i, row in df_proc.iterrows():
        key = row.get(processed_key)

        if pd.isna(key) or key not in golden_indexed.index:
            continue

        golden_row = golden_indexed.loc[key]

        for golden_col, proc_col in column_map.items():
            golden_val = golden_row.get(golden_col)
            proc_val = row.get(proc_col)

            # Skip if both missing
            if pd.isna(golden_val) and pd.isna(proc_val):
                continue

            # Replace if missing or mismatched
            if pd.isna(proc_val):
                df_proc.at[i, proc_col] = golden_val
                fixes_applied += 1

            elif str(golden_val) == str(proc_val):
                continue

            elif fuzzy_match(str(golden_val), str(proc_val)):
                continue

            else:
                # MISMATCH → replace with golden
                if golden_val!="Not Listed":
                    df_proc.at[i, proc_col] = golden_val
                    fixes_applied += 1

    # Drop all columns which are completely empty
    df_proc = df_proc.dropna(axis=1, how="all")
    
    # Save corrected processed file
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        df_proc.to_excel(writer, index=False, sheet_name=processed_sheet)

    print(f"Total fixes applied: {fixes_applied}")
    print(f"Corrected file written to: {output_path}")

column_mapping = {
    "ContractID": "ICS",
    "Effective Date": "Effective_Date",
    "Title": "Title",
    "Contract End Date": "End_Date",
    "Business Unit": "Business_Unit",
    "Product Line": "Product_Lines",
    "Contract Type": "Contract_Type",
    "Eligible Participants": "Eligible_Participants",
    "Type of pricing": "Type_of_Pricing",
    "Pricing Terms – Does the agreements have pricing terms. (TBC) ": "Pricing_Terms",
    "SFDC No.": "SFDC_no",
    "Version": "Version"
    }

sync_processed_with_golden(
    golden_path="Golden Record Manual.xlsx",
    processed_path="processed_metadata_iter_3.xlsx",
    golden_sheet="Sheet1",
    processed_sheet="Metadata",
    golden_key="Trim Number",
    processed_key="Article_Number",
    column_map=column_mapping,
    output_path="Golden Record Lexora.xlsx"
)
