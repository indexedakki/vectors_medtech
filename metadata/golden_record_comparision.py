import pandas as pd
from fuzzywuzzy import fuzz

def fuzzy_match(str1: str, str2: str, threshold: int = 70) -> bool:
    """
    Check if two strings are a fuzzy match based on a similarity threshold.
    Uses multiple fuzzy metrics for robustness.
    """

    if not str1 or not str2:
        return False

    # Normalize
    s1 = str(str1).strip().lower()
    s2 = str(str2).strip().lower()

    # Exact match shortcut
    if s1 == s2:
        return True

    # Compute multiple fuzzy scores
    scores = [
        fuzz.ratio(s1, s2),
        fuzz.partial_ratio(s1, s2),
        fuzz.token_sort_ratio(s1, s2),
        fuzz.token_set_ratio(s1, s2),
    ]

    # Take best score
    best_score = max(scores)

    return best_score >= threshold

def compare_golden_records(
    df1_path,
    df2_path,
    sheet_name1,
    sheet_name2,
    common_col1,
    common_col2,
    column_map,
    output_path=None
):
    """
    Compare mapped columns between two Excel sheets based on a common key.
    """

    # Load data
    df1 = pd.read_excel(df1_path, sheet_name=sheet_name1, dtype=str)
    df2 = pd.read_excel(df2_path, sheet_name=sheet_name2, dtype=str)

    # Normalize column names & values
    df1.columns = df1.columns.str.strip()
    df2.columns = df2.columns.str.strip()
    df1 = df1.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df2 = df2.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Drop duplicates
    print(f"Dropped duplicates: DF1 from {len(df1)} to {len(df1.drop_duplicates(subset=[common_col1]))}, DF2 from {len(df2)} to {len(df2.drop_duplicates(subset=[common_col2]))}")
    df1 = df1.drop_duplicates(subset=[common_col1])
    df2 = df2.drop_duplicates(subset=[common_col2])
    
    # Merge on common column
    merged = df1.merge(
        df2,
        how="outer",
        left_on=common_col1,
        right_on=common_col2,
        indicator=True,
        suffixes=("_df1", "_df2")
    )

    results = []

    for _, row in merged.iterrows():
        record_key = row.get(common_col1) or row.get(common_col2)

        for df1_col, df2_col in column_map.items():
            val1 = row.get(df1_col)
            val2 = row.get(df2_col)

            if pd.isna(val1) and pd.isna(val2):
                status = "Both Missing"
            elif pd.isna(val1):
                status = "Missing in DF1"
            elif pd.isna(val2):
                status = "Missing in DF2"
            elif str(val1) == str(val2):
                status = "Match"
            elif fuzzy_match(str(val1), str(val2)):
                status = "Fuzzy Match"
            else:
                status = "Mismatch"

            results.append({
                "Record Key": record_key,
                "Manual Column": df1_col,
                "Lexora Column": df2_col,
                "Manual Value": val1,
                "Lexora Value": val2,
                "Status": status
            })

    comparison_df = pd.DataFrame(results)

    # Optional export
    if output_path:
        comparison_df.to_excel(output_path, index=False)

    print(f"Total comparisons made: {len(comparison_df)}")

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

comparison_df = compare_golden_records(
    df1_path="Golden Record Manual.xlsx",
    df2_path="processed_metadata_iter_4.xlsx",
    sheet_name1="Sheet1",
    sheet_name2="Metadata",
    common_col1="Trim Number",
    common_col2="Article_Number",
    column_map=column_mapping,
    output_path="golden_vs_metadata_comparison.xlsx"
)
