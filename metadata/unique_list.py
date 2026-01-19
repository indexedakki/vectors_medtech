import pandas as pd

def unique_values_in_excel(
    excel_file,
    sheet_name,
    column_names
):
    # Load data
    df = pd.read_excel(
        excel_file,
        sheet_name=sheet_name,
        engine="openpyxl",
        dtype=str
    )

    # Normalize column names
    df.columns = df.columns.str.strip()

    # Collect unique values
    unique_dict = {}
    max_len = 0

    for column in column_names:
        values = df[column].dropna().unique().tolist()
        unique_dict[column] = values
        max_len = max(max_len, len(values))

    # Pad lists so all columns have equal length
    for column in unique_dict:
        unique_dict[column] += [None] * (max_len - len(unique_dict[column]))

    # Convert to DataFrame
    unique_df = pd.DataFrame(unique_dict)

    # Save to Excel (single sheet)
    with pd.ExcelWriter(excel_file, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        unique_df.to_excel(writer, sheet_name="Unique Values", index=False)

    print(f"✅ Unique values saved to {excel_file} → sheet 'Unique Values'")

# Usage example
unique_list = unique_values_in_excel(
    excel_file="metadata/processed_metadata_iter_3.xlsx",
    sheet_name="Metadata",
    column_names=["Record_Type", "Product_Lines", "Type_of_Pricing"]
)

unique_list = unique_values_in_excel(
    excel_file="metadata/processed_metadata_iter_2.xlsx",
    sheet_name="Metadata",
    column_names=["Record_Type", "Product_Lines", "Type_of_Pricing"]
)

unique_list = unique_values_in_excel(
    excel_file="metadata/Golden Record Lexora.xlsx",
    sheet_name="Sheet1",
    column_names=["Record_Type", "Business_Unit", "Product_Lines", "Type_of_Pricing"]
)

unique_list = unique_values_in_excel(
    excel_file="metadata/lexora_metadata.xlsx",
    sheet_name="in",
    column_names=["Contract Type", "Business Unit", "Product Line", "Type of pricing"]
)