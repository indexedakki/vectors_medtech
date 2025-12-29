table_text = """
|   JJHCS Account Number | ParticipantName                              | Address          | City       | State   |   ZIP Code |
|------------------------|----------------------------------------------|------------------|------------|---------|------------|
|               00019161 | University of California Davis MedicalCenter | 2315StocktonBlvd | Sacramento | CA      |      95817 |
"""

# table_text = """|                    |                                                 | OUT OF WARRANTY                        | ESSENTIAL SUPPORT                            | PREMIER SUPPORT                            |
# |--------------------|------------------------------------------------------|----------------------------------------|----------------------------------------------|--------------------------------------------|
# | CLINICAL ASSURANCE | Technical phone support                              | 8 a.m.-5 p.m. ET M-F                   | 8 a.m.-5 p.m. ET M-F                         | 8 a.m.-5 p.m. ET M-F                       |
# | CLINICAL ASSURANCE | 24x7 omergency phone oddns                           | Included                               | Included                                     | Included                                   |
# | CLINICAL ASSURANCE | Fleld service onginoor on-sitoguarantoo              | Basod on availabifity                  | Within 6 business days of ramoto diagnostics | Within 2 busin9ss daysofromote diagnostics |
# | CLINICAL ASSURANCE | Correotive maintenanca/ repair: labor                | List prico 8a.m.-5 p.m. M-F looal time | Included 0a.m.-5p.m. M-F local timo          | Included 8 a.m.-9 p.m. M-F local tirno     |
# | CLINICAL ASSURANCE | Correctivomaintenance/ ropair: parts*                | List price Ground shipping             | Inaluded Buddys ep                           | lncluded Overmight shipping                |
# | CLINICAL ASSURANCE | Provontative maintenanco visit                       | Upon roquest List prico                | Inoluded Annually                            | lncluded Annually                          |
# | JNVESTMENT VALUE   | Loanor equipment                                     | $2,500/month                           | $1,500/ovont                                 | Included                                   |
# | JNVESTMENT VALUE   | Ropair visits                                        | List prioe per visit                   | Unlimited                                    | Unlimnod                                   |
# | JNVESTMENT VALUE   | Ablation confirnation: reconfigurations maintenance& | Remote support On-sito at list prica   | Romotooron-site toddns                       | Remoteoron-site loddns                     |
# | PARTNERSHIP        | Unecvered replacemont parts covorage                 | List price                             | Up to $7,600                                 | Up to $16,000                              |
# | PARTNERSHIP        | Comprohonsivo systom oparational rovlow              | Upon request                           | Included                                     | lncluded                                   |
# """

table_text = """| Participant                                             | Description                           | Start Date   | End Date   |
|---------------------------------------------------------|---------------------------------------|--------------|------------|
| Banner Ironwood Medical Center                          | Trauma Products                       | 2/22/2016    | 3/1/2019   |
| Banner Baywood Medical Center                           | Trauma Products                       | 9/13/2018    | 5/31/2020  |
| Banner Del E Webb Medical Center                        | Joint Reconstruction Products         | 9/13/2018    | 5/31/2020  |
| Banner Desert                                           | CNV, Codman Neurovascular, Cerenovus  | 5/31/2018    | 5/31/2020  |
| Banner Gateway Medical Center                           | Trauma Products                       | 9/13/2018    | 5/31/2020  |
| Banner Gateway Medical Center                           | CMF, Craniomaxillofacial Products     | 9/13/2018    | 5/31/2020  |
| Banner Good Samaritan University Medical Center Phoenix | CNV, Codman Neurovascular             | 8/1/2015     | 5/31/2020  |
| Banner Ironwood Medical Center                          | Trauma Products                       | 9/13/2018    | 5/31/2020  |
| Banner UMC Tucson                                       | CNV, Codman Neurovascular Products    | 1/13/2016    | 5/31/2020  |
| Banner UMC Tucson                                       | CNV, Codman Neurovascular Products    | 5/12/2016    | 5/31/2020  |
| Banner University Medical Center Phoenix                | Codman Neuro Products                 | 1/18/2018    | 5/31/2020  |
| Banner University Medical Center Tucson                 | CNV, Cerenovus Neurovascular          | 8/24/2018    | 5/31/2020  |
| Casa Grande Regional Medical Center                     | Trauma Products                       | 9/13/2018    | 5/31/2020  |
| Banner University Medical Center Phoenix                | CNV, Cerenovus Neurovascular          | 4/24/2019    | 5/31/2020  |
| Banner Desert Medical Center                            | CNV, Cerenovus Neurovascular Products | 5/1/2019     | 5/31/2020  |
| Banner Boswell Medical Center                           | Trauma Products                       | 9/13/2017    | 9/30/2020  |
| Banner Del E Webb Medical Center                        | Trauma Products                       | 9/13/2017    | 9/30/2020  |
| Banner Good Samaritan Medical Center                    | CMF, Craniomaxillofacial Products     | 4/24/2018    | 4/30/2021  |
| Fairbanks Memorial Hospital                             | Joint Reconstruction Products         | 5/4/2018     | 4/30/2021  |
| Ogallala Community Hospital                             | Joint Reconstruction Products         | 5/8/2018     | 5/31/2021  |
| Melissa Memorial Hospital                               | Mitek Sports Medicine Products        | 9/18/2019    | 9/17/2022  |"""
import pandas as pd

def markdown_table_to_df(md_text: str) -> pd.DataFrame:
    rows = []
    lines = [l for l in md_text.splitlines() if l.strip()]

    for line in lines:
        # Skip separator rows
        if set(line.replace("|", "").strip()) <= {"-"}:
            continue

        # Split safely and strip
        cells = [c.strip() for c in line.strip("|").split("|")]
        rows.append(cells)

    # Find max column count
    max_cols = max(len(r) for r in rows)

    # Normalize rows (pad or trim)
    normalized = [r + [""] * (max_cols - len(r)) for r in rows]

    # First row = header
    df = pd.DataFrame(normalized[1:], columns=normalized[0])

    return df

df = markdown_table_to_df(table_text)
print(df)


with pd.ExcelWriter("table.xlsx", engine="openpyxl") as writer:
    df.to_excel(writer, index=False, sheet_name="Table2")
