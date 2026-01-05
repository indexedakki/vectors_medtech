import pandas as pd

def identify_distinct_Ind_SHIPto_UCN(df_IDN, UCN, COL1, COL2, COL3):    
    # normalize UCN formats
    # df_IDN[COL1] = df_IDN[COL1].astype(str).str.strip()
    # df_IDN[COL2] = df_IDN[COL2].astype(str).str.strip()
    # df_IDN[COL3] = df_IDN[COL3].astype(str).str.strip()

    # filter
    df_filtered = df_IDN[df_IDN[COL1] == UCN]
    print(f"="*60)
    print(f"Rows found for Parent UCN {UCN}: {len(df_filtered)}")

    if df_filtered.empty:
        return {
            "Parent UCN": UCN,
            "Distinct IND Count": 0,
            "Distinct SHIPTO Count": 0,
            "IND UCN List": [],
            "SHIPTO UCN List": []
        }

    # get unique INDIV and SHIPTO values
    distinct_IND = sorted(df_filtered[COL2].dropna().unique().tolist())
    distinct_SHIPTO = sorted(df_filtered[COL3].dropna().unique().tolist())
    
    print(f"\n=== Results for Parent UCN: {UCN} ===")
    print(f"Distinct INDIV_UCN ({COL2}): {len(distinct_IND)}")
    print(f"Distinct SHIPTO_UCN ({COL3}): {len(distinct_SHIPTO)}")
    
    return {
        "Parent UCN": UCN,
        "Distinct IND Count": len(distinct_IND),
        "Distinct SHIPTO Count": len(distinct_SHIPTO),
        "IND UCN List": distinct_IND,
        "SHIPTO UCN List": distinct_SHIPTO
    }



# Usage example
EXCEL_FILE = "IDN Full Explosion Report 11.24.2025.xlsx"
SHEET_NAME = "Sheet1"

df_IDN = pd.read_excel(
    EXCEL_FILE,
    sheet_name=SHEET_NAME,
    engine="openpyxl",
    dtype={
        "M_SUPER_PARNT_UNI_CUST_NO": str,
        "INDIV_UCN": str,
        "MEMBER_SHIPTO_UCN": str
    }
)


COL_NAME_PARENT = "M_SUPER_PARNT_UNI_CUST_NO"
UCNS = ["01018471", "01018845", "01030242"]
COL_NAME_IND = "INDIV_UCN"
COL_NAME_SHIPTO = "MEMBER_SHIPTO_UCN"

# Collect output results
results = []
exploded_rows = []
for UCN in UCNS:
    res = identify_distinct_Ind_SHIPto_UCN(df_IDN, UCN, COL_NAME_PARENT, COL_NAME_IND, COL_NAME_SHIPTO)
    results.append(res)

# Explode into individual rows
    ind_list = res["IND UCN List"]
    ship_list = res["SHIPTO UCN List"]

    # max length among IND and SHIPTO lists
    max_len = max(len(ind_list), len(ship_list))

    for i in range(max_len):
        exploded_rows.append({
            "Parent UCN": res["Parent UCN"],
            "IND UCN": ind_list[i] if i < len(ind_list) else "",
            "SHIPTO UCN": ship_list[i] if i < len(ship_list) else ""
        })

df_out = pd.DataFrame(results)
# ❗ REMOVE list columns
df_out = df_out.drop(columns=["IND UCN List", "SHIPTO UCN List"])
df_exploded = pd.DataFrame(exploded_rows)

# Save both to Excel
OUTPUT_FILE = "UCN Distinct Output.xlsx"

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df_out.to_excel(writer, sheet_name="Summary", index=False)
    df_exploded.to_excel(writer, sheet_name="Exploded", index=False)

print("Written to:", OUTPUT_FILE)