import pandas as pd

def fix_refer_parent_document(input_excel: str, output_excel: str) -> None:
    df = pd.read_excel(input_excel, engine="openpyxl", dtype=str)

    # Normalize Article_Number
    df["Article_Number"] = df["Article_Number"].str.strip()

    # Extract base document id and suffix (.001, .002, etc.)
    df["base_id"] = df["Article_Number"].str.rsplit(".", n=1).str[0]
    df["suffix"] = df["Article_Number"].str.rsplit(".", n=1).str[1]

    # Identify parent rows (.001)
    parents = df[df["suffix"] == "001"].set_index("base_id")

    # Columns that may contain "refer parent document"
    columns_to_fix = [
        col for col in df.columns
        if col not in ["Article_Number", "base_id", "suffix"]
    ]

    count_references_fixed = 0
    # Iterate over child rows
    for idx, row in df.iterrows():
        if row["suffix"] != "001":  # child document
            base_id = row["base_id"]

            if base_id in parents.index:
                parent_row = parents.loc[base_id]

                for col in columns_to_fix:
                    if (isinstance(row[col], str) and row[col].strip().lower() == "refer parent document"):
                        df.at[idx, col] = parent_row[col]
                        count_references_fixed += 1

    # Cleanup helper columns
    df.drop(columns=["base_id", "suffix"], inplace=True)

    # Save output
    with pd.ExcelWriter(output_excel, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Metadata")

    print(f"Fixed {count_references_fixed} 'refer parent document' entries and saved to {output_excel}")

input_excel = "processed_metadata_iter_3.xlsx"
output_excel = "processed_metadata_iter_3.xlsx"
fix_refer_parent_document(input_excel, output_excel)