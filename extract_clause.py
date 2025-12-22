from pathlib import Path
import re

# def extract_clauses(filename: str):
#     def remove_signature_block(text: str) -> str:
#         """
#         Removes execution/signature sections commonly found at the end of clauses.
#         """
#         pattern = re.compile(
#             r"""
#             \n\s*
#             (The\s+Company:|
#             The\s+Customer:|
#             By:|
#             Name:|
#             Title:)
#             .*$
#             """,
#             re.IGNORECASE | re.DOTALL | re.VERBOSE
#         )
#         return re.sub(pattern, "", text).strip()
    
#     # Read markdown file
#     md_text = Path(f"{filename}").read_text(encoding="utf-8")
#     # Isolate main clauses section
#     md_text = re.split(r"\n##\s", md_text, maxsplit=2)[1]
#     # Remove signature blocks
#     md_text = remove_signature_block(md_text)

#     pattern = re.compile(
#         r"""
#         (?P<number>\d+)\.\s+                # Clause number (e.g. 1.)
#         (?P<heading>[^.]+)\.\s+              # Clause heading
#         (?P<body>.*?)                        # Clause body (lazy)
#         (?=\n\d+\.|\Z)                       # Stop at next clause or EOF
#         """,
#         re.DOTALL | re.VERBOSE
#     )

#     clauses = []

#     for match in pattern.finditer(md_text):
#         clauses.append({
#             "clause_number": match.group("number"),
#             "heading": match.group("heading").strip(),
#             "text": match.group("body").strip()
#         })

#     for c in clauses:
#         print(f"\nClause {c['clause_number']}: {c['heading']}")
#         print("-" * 40)
#         print(c["text"], "...")

#     with open(f"{filename[:-3]}_clause.md", "w", encoding="utf-8") as f:
#         for c in clauses:
#             f.write(f"## {c['clause_number']}. {c['heading']}\n\n")
#             f.write(f"{c['text']}\n\n")
# filename = "1030295.md"
# extract_clauses(filename)

def extract_single_clause(text: str, clause_title: str, all_titles: list[str]) -> str:
    """
    Extracts a single clause based on its title.
    """

    # Escape titles for regex
    escaped_titles = [re.escape(t) for t in all_titles]

    # Build stop pattern (any other clause title)
    stop_titles = [t for t in escaped_titles if t != re.escape(clause_title)]

    stop_pattern = "|".join(stop_titles)

    pattern = re.compile(
        rf"""
        (?:^|\n)                         # Start of doc or new line
        \s*\d*\.*\s*                     # Optional numbering
        {re.escape(clause_title)}        # Clause title
        \s*\.?\s*                        # Optional trailing dot
        (?P<content>.*?)                 # Clause body
        (?=
            \n\s*\d*\.*\s*(?:{stop_pattern})   # Next clause
            | \Z                                # End of document
        )
        """,
        re.IGNORECASE | re.DOTALL | re.VERBOSE
    )

    match = pattern.search(text)
    if not match:
        return ""

    return f"{clause_title}\n{match.group('content').strip()}"

def remove_signature_block(filename: str) -> str:
    text = Path(f"{filename}").read_text(encoding="utf-8")
    pattern = re.compile(
        r"""
        \n\s*
        (IN\s+WITNESS\s+WHEREOF|
         The\s+Company:|
         The\s+Customer:|
         By:|
         Name:|
         Title:)
        [\s\S]*$
        """,
        re.IGNORECASE | re.VERBOSE
    )
    return pattern.sub("", text).strip()

def run_extract(filename, CLAUSE_TITLES):
    document_text = remove_signature_block(filename)
    def extract_all_clauses(text: str, clause_titles: list[str]) -> dict:
        clauses = {}

        for title in clause_titles:
            clauses[title] = extract_single_clause(text, title, clause_titles)

        return clauses
    clauses = extract_all_clauses(document_text, CLAUSE_TITLES)

    with open(f"{filename[:-3]}_clauses.md", "w", encoding="utf-8") as f:
        for idx, (heading, text) in enumerate(clauses.items(), start=1):
            if not text.strip():
                continue  # skip empty clauses
            f.write(f"## {idx}. {heading}\n\n")
            f.write(f"{text.strip()}\n\n")
            
filename = "1008315_pypdf.md"
CLAUSE_TITLES = [
    "Eligible Participants",
    "Equipment Location",
    "Responsibilities of Participant",
    "Data Collection",
    "Supplement Term",
    "Pricing",
    "Price Adjustments",
    "Definitions",
    "Pricing Disclosure",
    "Confidentiality",
    "Offer Expiration",
    "Incorporation By Reference",
    "Warranty of Authority",
    "Limited Warranties and Remedies",
    "General Exclusions",
    "Initial Stock Level",
    "Inventory Management",
    "Inventory Replenishment",
    "Term and Termination",
    "Audit",
    "Compliance",
    "Governing Law",
    "Dispute Resolution",
    "Insurance",
    "Indemnity",
    "Force Maieure",
    "Severability",
    "Publicity and Trademarks",
    
    "Amendments to the Agreement",
    "No Further Amendment",
    "Counterparts"
    "Amendment",
    "Assignment",
    "Notices",
    "Waiver",
    
]
run_extract(filename, CLAUSE_TITLES)

