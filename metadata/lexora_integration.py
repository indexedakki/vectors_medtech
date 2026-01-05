from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd


class LexoraIntegration:
    def add_lexora_metadata(self,
                                excel_file_iter_2: str,
                                excel_file_lexora: str,
                                sheet_name_iter_2: str,
                                sheet_name_lexora: str,
                                common_column_name_iter_2: str,
                                common_column_name_lexora: str,
                                field_map: Dict[str, str],
                                output_trim_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Replace values in TRIM dataframe with matching RADAR values using field_map mapping.
        Returns a report dict with counts and path to saved files.
        """
        df_iter_2 = pd.read_excel(excel_file_iter_2, sheet_name=sheet_name_iter_2, engine="openpyxl")
        df_iter_2.columns = df_iter_2.columns.str.strip()
        df_lexora = pd.read_excel(excel_file_lexora, sheet_name=sheet_name_lexora, engine="openpyxl")
        df_lexora.columns = df_lexora.columns.str.strip()
 
        df_iter_2[common_column_name_iter_2] = df_iter_2[common_column_name_iter_2].astype(str).str.strip()
        df_lexora[common_column_name_lexora] = df_lexora[common_column_name_lexora].astype(str).str.strip()
 
        df_iter_2 = df_iter_2.drop_duplicates(subset=[common_column_name_iter_2])
        df_lexora = df_lexora.drop_duplicates(subset=[common_column_name_lexora])
 
        lexora_lookup = df_lexora.set_index(common_column_name_lexora).to_dict(orient="index")
        # Uppercase keys for consistent matching
        lexora_lookup = {k.upper(): v for k, v in lexora_lookup.items()}
        changes = []
        for idx, row in df_iter_2.iterrows():
            rec_id = row.get(common_column_name_iter_2, "")
            if rec_id in lexora_lookup:
                lexora_row = lexora_lookup[rec_id]
                updated = False
                change_record = {"RecordNumber": rec_id, "row_index": int(idx)}
                for trim_field, lexora_field in field_map.items():
                    if lexora_field not in lexora_row:
                        continue
                    lexora_val = lexora_row[lexora_field]
                    trim_val = row.get(trim_field, None)
                    if pd.isna(lexora_val) or pd.notna(trim_val) and trim_field!="Contract_Type":
                        continue
                    old_val = df_iter_2.at[idx, trim_field] if trim_field in df_iter_2.columns else None
                    new_val = lexora_val
                    if str(old_val) != str(new_val):
                        if trim_field not in df_iter_2.columns:
                            df_iter_2[trim_field] = pd.NA
                        df_iter_2.at[idx, trim_field] = new_val
                        updated = True
                        # change_record[f"{trim_field}_old"] = old_val
                        change_record[f"{trim_field}_new"] = new_val
                if updated:
                    changes.append(change_record)
 
        if output_trim_file is None:
            output_trim_file = str(Path(excel_file_iter_2).with_name(Path(excel_file_iter_2).stem + "_updated.xlsx"))
 
        df_iter_2 = df_iter_2.fillna("N/A")
        out_path = Path(output_trim_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(out_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as writer:
            df_iter_2.to_excel(writer, sheet_name=sheet_name_iter_2, index=False)
 
        changes_df = pd.DataFrame(changes)
        changes_report_path = str(out_path).replace(".xlsx", "_changes_report.xlsx")
        if not changes_df.empty:
            with pd.ExcelWriter(changes_report_path, engine="xlsxwriter") as writer:
                changes_df.to_excel(writer, index=False, sheet_name=excel_file_lexora)
            print("Saved changes report to: %s", changes_report_path)
        else:
            print("No changes were made during radar->trim replace.")

        print("Updated TRIM saved to: %s", out_path)

lex = LexoraIntegration()
lex.add_lexora_metadata(
    excel_file_iter_2="processed_metadata_iter_2.xlsx",
    excel_file_lexora="lexora_metadata.xlsx",
    sheet_name_iter_2="Metadata",
    sheet_name_lexora="in",
    common_column_name_iter_2="FileName",
    common_column_name_lexora="Trim Number",
    field_map={
        "SFDC_no": "SFDC No.",
        "ICS": "ContractID",
        "Version": "Version",
        "Effective_Date": "Effective Date",
        "End_Date": "Contract End Date",
        "Business_Unit": "Business Unit",
        "Product_Lines": "Product Line",
        "Contract_Type": "Contract Type",
        "Eligible_Participants": "Eligible Participants",
        "Type_of_Pricing": "Type of pricing",
        "Product_details": "Product Details",
        "Pricing_Terms": "Pricing Terms"
    },
    output_trim_file="processed_metadata_iter_3.xlsx"
)