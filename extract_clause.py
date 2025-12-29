from pathlib import Path
import re
import os

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

import re
from pathlib import Path

def extract_numbered_clauses(text: str) -> list[dict]:
    """
    Extract clauses that start with numbered headings like:
    1. Title.
    1.1 Title.
    2 Title.
    """

    pattern = re.compile(
        r"""
        (?:^|\n)                              # start or new line
        (?P<number>\d+(?:\.\d+)*)             # clause number (1, 1.1, 2.3)
        \s*\.?\s+                             # dot optional + space
        (?P<title>[A-Z][A-Za-z0-9\s,&()\-]+?) # clause title
        \.\s*                                 # title ends with period
        (?P<content>.*?)                      # clause body
        (?=                                   # stop condition
            \n\s*\d+(?:\.\d+)*\s*\.?\s+[A-Z]  # next clause
            | \Z                              # end of document
        )
        """,
        re.DOTALL | re.VERBOSE
    )

    clauses = []
    for match in pattern.finditer(text):
        clauses.append({
            "clause_number": match.group("number"),
            "heading": match.group("title").strip(),
            "text": match.group("content").strip()
        })

    return clauses


def remove_signature_block_text(text: str) -> str:
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


def run_extract(filename: str):
    raw_text = Path(filename).read_text(encoding="utf-8")
    document_text = remove_signature_block_text(raw_text)

    clauses = extract_numbered_clauses(document_text)

    output_dir = Path("pdfs_clauses")
    output_dir.mkdir(exist_ok=True)
    output_file = Path(filename).stem + ".md"
    with open(output_dir / output_file, "w", encoding="utf-8") as f:
        for clause in clauses:
            f.write(f"## {clause['clause_number']}. {clause['heading']}\n\n")
            f.write(f"{clause['text']}\n\n")
    print(f"✅ Extracted {len(clauses)} clauses from {filename} to {output_dir / output_file}")
            
# filename = "896307.md"
# run_extract(filename)

files = os.listdir("pdfs_md")
for file in files:
    if file.endswith(".md"):
        md_file = os.path.join("pdfs_md", file)
        run_extract(md_file)