import pandas as pd
from fuzzywuzzy import fuzz
from collections import Counter

def normalize_similar_values(
    golden_path,
    golden_sheet,
    columns,
    output_path,
    threshold=85
):
    # Load data
    df = pd.read_excel(golden_path, sheet_name=golden_sheet, dtype=str)

    # Normalize whitespace
    df.columns = df.columns.str.strip()
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    normalization_report = {}

    for col in columns:
        print(f"\nProcessing column: {col}")

        # Get non-null values
        values = df[col].dropna().astype(str)

        # Count frequency
        value_counts = Counter(values)

        unique_values = list(value_counts.keys())
        used = set()
        replacement_map = {}

        for i, val in enumerate(unique_values):
            if val in used:
                continue

            # Start a similarity group
            group = [val]
            used.add(val)

            for other in unique_values[i + 1:]:
                if other in used:
                    continue

                score = [
                    fuzz.ratio(val, other),
                    fuzz.token_sort_ratio(val, other),
                    fuzz.token_set_ratio(val, other),
                ]
                max_score = max(score)

                if max_score >= threshold:
                    group.append(other)
                    used.add(other)

            # Choose most frequent value as canonical
            canonical = max(group, key=lambda x: value_counts[x])

            for g in group:
                replacement_map[g] = canonical

        # Apply normalization
        df[col] = df[col].map(lambda x: replacement_map.get(x, x))

        normalization_report[col] = replacement_map

        print(f"Normalized {len(replacement_map)} values in column '{col}'")

    
    # Convert normalization report to DataFrame for better readability
    report_rows = []
    for col, mapping in normalization_report.items():
        for old_val, new_val in mapping.items():
            report_rows.append({"Column": col, "Old Value": old_val, "New Value": new_val})

    report_df = pd.DataFrame(report_rows)
    normalization_report = report_df
    
    # Save output
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Metadata")
        normalization_report.to_excel(writer, index=False, sheet_name="Normalization_Report")
        
    print(f"\nNormalization complete. Output written to: {output_path}")

normalize_similar_values(
    golden_path="Golden Record Lexora.xlsx",
    golden_sheet="Metadata",
    columns=["Contract_Type", "Type_of_Pricing"],
    output_path="Golden Record Lexora.xlsx"
)
