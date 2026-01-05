import pandas as pd

def compare_contracts(excel_file, sheet_name_1, sheet_name_2, COLOUMN_NAME_1, COLOUMN_NAME_2, CHECK_FOR_COLUMN_NAME_1, CHECK_FOR_COLUMN_NAME_2):
    df_regents_joe = pd.read_excel(excel_file, sheet_name=sheet_name_1, engine="openpyxl")
    df_extracted = pd.read_excel(excel_file, sheet_name=sheet_name_2, engine="openpyxl")
    
    # Normalize column names (remove case sensitivity and spaces)
    df_regents_joe.columns = df_regents_joe.columns.str.strip()
    df_extracted.columns = df_extracted.columns.str.strip()

    # Find records that exist in both by Record Number
    common_records = df_regents_joe[
        df_regents_joe[COLOUMN_NAME_1].isin(df_extracted[COLOUMN_NAME_2])
    ].copy()

    # Add related columns from both DataFrames for comparison
    merged = pd.merge(
        common_records,
        df_extracted[[COLOUMN_NAME_2, CHECK_FOR_COLUMN_NAME_2]],
        left_on=COLOUMN_NAME_1,
        right_on=COLOUMN_NAME_2,
        how="left",
        suffixes=("_joe", "_extracted")
    )

    # Check which ones have related values present (non-empty on both sides)
    merged[f"{CHECK_FOR_COLUMN_NAME_1}_Present_Both"] = merged[CHECK_FOR_COLUMN_NAME_1].notna() & merged[CHECK_FOR_COLUMN_NAME_2].notna()

    # Just to see which have different related values
    merged[f"{CHECK_FOR_COLUMN_NAME_1}_Same_Value"] = merged[CHECK_FOR_COLUMN_NAME_1].astype(str) == merged[CHECK_FOR_COLUMN_NAME_2].astype(str)

    # Summary counts
    total_common = len(merged)
    present_both = merged[f"{CHECK_FOR_COLUMN_NAME_1}_Present_Both"].sum()
    same_value = merged[f"{CHECK_FOR_COLUMN_NAME_1}_Same_Value"].sum()
    
    # Identify where values are present in only one dataset
    only_joe = merged[merged[CHECK_FOR_COLUMN_NAME_1].notna() & merged[CHECK_FOR_COLUMN_NAME_2].isna()]
    only_extracted = merged[merged[CHECK_FOR_COLUMN_NAME_1].isna() & merged[CHECK_FOR_COLUMN_NAME_2].notna()]

    present_only_joe = len(only_joe)
    present_only_extracted = len(only_extracted)

    # Record numbers for each
    record_numbers_present_only_joe = only_joe[COLOUMN_NAME_1].tolist()
    record_numbers_present_only_extracted = only_extracted[COLOUMN_NAME_1].tolist()

    print(f"✅ Total common record numbers: {total_common}")
    print(f"✅ Records where both have {CHECK_FOR_COLUMN_NAME_1} values (present in both): {present_both}")
    print(f"✅ Records where {CHECK_FOR_COLUMN_NAME_1} values are identical: {same_value}")
    print(f"❌ Records where {CHECK_FOR_COLUMN_NAME_1} values differ: {present_both - same_value}")
    print(f"ℹ️ Records with {CHECK_FOR_COLUMN_NAME_1} present only in Joe's data: {present_only_joe - present_both}")
    print(f"ℹ️ Records with {CHECK_FOR_COLUMN_NAME_1} present only in Extracted data: {present_only_extracted - present_both}")
    print(f"ℹ️ Record numbers with {CHECK_FOR_COLUMN_NAME_1} present only in Joe's data: {record_numbers_present_only_joe}")
    print(f"ℹ️ Record numbers with {CHECK_FOR_COLUMN_NAME_1} present only in Extracted data: {record_numbers_present_only_extracted}")
    
    # Optional — view mismatched ones
    mismatched = merged[~merged[f"{CHECK_FOR_COLUMN_NAME_1}_Same_Value"] & merged[f"{CHECK_FOR_COLUMN_NAME_1}_Present_Both"]]
    print(f"\n🔍 Mismatched '{CHECK_FOR_COLUMN_NAME_1}' values between Joe's and Extracted:")
    print(mismatched[[COLOUMN_NAME_1, CHECK_FOR_COLUMN_NAME_1, CHECK_FOR_COLUMN_NAME_2]])

sheet_mappings = {
    "Baptist South Florida": "Baptist Extracted",
    "Banner Health": "Banner Extracted",
    "Regents of Univ of CA": "Regents Extracted"
}
column_mappings = {
                    "Record Number": "RecordNumber", 
                    "End Date": "EndDate", 
                    "Start Date": "StartDate",
                    "Title": "RecordTitle",
                    "ICS #": "ICS",
                    "Related records": "RelatedRecords",
                    "Contract Type": "ContractType",
                    "Customer Name": "CustomerName",
                    "JJ Contract #": "ContractNo",
                    "Product Line(s)": "ProductLines",
                    "All thesaurus terms": "RecordKeywords_BusinessUnit"
                   }

column_mappings = {"Title": "RecordTitle",
                   "Product Line(s)": "ProductLine",
                   "All thesaurus terms": "RecordKeywords_BusinessUnit"
                   }
for customer_name, extracted_sheet in sheet_mappings.items():
    print(f"\n=============================== Comparing contracts for {customer_name} ===============================")
    print("-"*80)
    EXCEL_FILE = "Banner Health - Baptist South Florida - Regents of Univ of CA.xlsx"
    SHEET_NAME_1 = customer_name
    SHEET_NAME_2 = extracted_sheet
    COLOUMN_NAME_1 = "Record Number"
    COLOUMN_NAME_2 = "RecordNumber"
    for col1, col2 in column_mappings.items():
        CHECK_FOR_COLUMN_NAME_1 = col1
        CHECK_FOR_COLUMN_NAME_2 = col2
        print(f"\n----- Comparing column: {CHECK_FOR_COLUMN_NAME_1} with {CHECK_FOR_COLUMN_NAME_2} -----")
        compare_contracts(EXCEL_FILE, SHEET_NAME_1, SHEET_NAME_2, COLOUMN_NAME_1, COLOUMN_NAME_2, CHECK_FOR_COLUMN_NAME_1, CHECK_FOR_COLUMN_NAME_2)
    
# # Simple usage example
# EXCEL_FILE = "Banner Health - Baptist South Florida - Regents of Univ of CA.xlsx"
# SHEET_NAME_1 = "Regents of Univ of CA"
# SHEET_NAME_2 = "Regents Extracted"
# COLOUMN_NAME_1 = "Record Number"
# COLOUMN_NAME_2 = "RecordNumber"
# CHECK_FOR_COLUMN_NAME_1 = "End Date"
# CHECK_FOR_COLUMN_NAME_2 = "EndDate"
# compare_contracts(EXCEL_FILE, SHEET_NAME_1, SHEET_NAME_2, COLOUMN_NAME_1, COLOUMN_NAME_2, CHECK_FOR_COLUMN_NAME_1, CHECK_FOR_COLUMN_NAME_2)
