from pypdf import PdfReader
from pathlib import Path
import os

def extract_text_pypdf(pdf_path: str) -> str:
    """
    Fast text extraction for text-based PDFs using pypdf.
    Returns combined text from all pages.
    """
    reader = PdfReader(pdf_path)
    pages_text = []

    for i, page in enumerate(reader.pages):
        try:
            text = page.extract_text()
            if text:
                pages_text.append(text)
        except Exception as e:
            print(f"⚠️ Page {i+1} failed: {e}")

    # Save as md file to
    outptut_dir = Path("pdfs_md")
    outptut_dir.mkdir(exist_ok=True)
    filename = Path(pdf_path).stem + ".md"
    with open(outptut_dir / filename, 'w', encoding='utf-8') as f:
        f.write("\n\n".join(pages_text))
        
    print(f"✅ Extracted {len(pages_text)} pages from {pdf_path} to {outptut_dir / filename}")

# Get all pdfs from pdfs folder using pathlib
pdfs = os.listdir("pdfs")
for pdf in pdfs:
    if pdf.endswith(".pdf"):
        pdf_file = os.path.join("pdfs", pdf)
        extract_text_pypdf(pdf_file)

# pdf_file = r"1008315.pdf"
# extract_text_pypdf(pdf_file)