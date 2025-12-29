import ssl
import os
import re
import sys
from docling.document_converter import DocumentConverter
# # SSL bypass
# ssl._create_default_https_context = ssl._create_unverified_context

# # Huggingface settings
# os.environ["HF_HUB_OFFLINE"] = "1"
# os.environ["HF_HUB_DISABLE_SYMLINKS"] = "1"
# os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"

# from huggingface_hub import snapshot_download
# local = snapshot_download("docling-project/docling-layout-heron")
# os.environ["DOCLING_LAYOUT_MODEL_PATH"] = local
# print("Using local model:", local)
 
def clean_markdown(md: str) -> str:
    """Remove excessive whitespace and garbage characters."""
    md = md.replace("\u200b", "")  # zero-width spaces
    md = re.sub(r'[ \t]+$', '', md, flags=re.MULTILINE)  # trailing spaces
    md = re.sub(r'\n\s*\n\s*\n+', '\n\n', md)  # 2+ blank lines → 1
    return md.strip()
 
def convert_pdf_to_markdown(pdf_path: str):
    """Convert PDF and return clean structured markdown including tables."""
   
    converter = DocumentConverter()
    result = converter.convert(pdf_path)
 
    md = result.document.export_to_markdown()
 
    # Extract & Append tables explicitly
    try:
        tables = result.document.tables
        if tables:
            md += "\n\n---\n# Extracted Tables\n"
            for i, t in enumerate(tables, start=1):
                md += f"\n\n## Table {i}\n"
                md += t.export_to_markdown()
    except:
        pass
 
    return clean_markdown(md)
 
def process(pdf_path: str):
    print(f"Processing: {pdf_path}")
 
    md = convert_pdf_to_markdown(pdf_path)
    out_file = os.path.splitext(pdf_path)[0] + ".md"
 
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(md)
 
    print(f"✔ Output saved: {out_file}")
 
if __name__ == "__main__":
    pdf_file = r"1008315.pdf"
    process(pdf_file)