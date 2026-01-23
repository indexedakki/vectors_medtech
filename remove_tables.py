import os
import re
import io

import fitz  # PyMuPDF
import pdfplumber
import pytesseract
from PIL import Image

pytesseract.pytesseract.tesseract_cmd = r"C:\Users\AkashKumar\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"
# ================= CONFIG =================

MIN_PAGES_BEFORE_TABLE = 5
FULL_PAGE_COVERAGE_RATIO = 0.75
TEXT_THRESHOLD = 500  # chars to detect text PDF

# =========================================


def is_image_pdf(pdf_path):
    """Detect if PDF is image-based"""
    doc = fitz.open(pdf_path)
    for page in doc:
        page_text_len = len(page.get_text().strip())
        if page_text_len > TEXT_THRESHOLD:
            doc.close()
            return False
    doc.close()
    return True


def ocr_page_text(page):
    """OCR a single page"""
    pix = page.get_pixmap(dpi=300)
    img = Image.open(io.BytesIO(pix.tobytes("png")))
    return pytesseract.image_to_string(img)


def looks_like_table(text):
    """Heuristic table detection for OCR text"""
    lines = [l for l in text.split("\n") if l.strip()]
    if len(lines) < 5:
        return False

    aligned = sum(1 for l in lines if re.search(r"\s{3,}", l))
    numeric = sum(1 for l in lines if re.search(r"\d", l))

    return aligned >= 3 and numeric >= 3


def is_full_page_table(bbox, page_width, page_height):
    """Check if table covers most of page"""
    x0, top, x1, bottom = bbox
    table_area = (x1 - x0) * (bottom - top)
    page_area = page_width * page_height
    return (table_area / page_area) >= FULL_PAGE_COVERAGE_RATIO


def find_truncation_page(pdf_path):
    """
    Returns page index (0-based) to truncate from
    """
    # ---------- TEXT PDF ----------
    if not is_image_pdf(pdf_path):
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                tables = page.find_tables()
                if not tables:
                    continue

                # Condition: table after N pages
                if i >= MIN_PAGES_BEFORE_TABLE:
                    return i

                # Condition: full-page table
                for table in tables:
                    if is_full_page_table(
                        table.bbox, page.width, page.height
                    ):
                        print(f"🛑 Table found on page {i + 1} meeting truncation conditions.")
                        return i
        return None

    # ---------- IMAGE PDF (OCR) ----------
    doc = fitz.open(pdf_path)
    for i, page in enumerate(doc):
        text = ocr_page_text(page)
        if looks_like_table(text) and i >= MIN_PAGES_BEFORE_TABLE:
            print(f"🛑 Table-like content found on page {i + 1} meeting truncation conditions.")
            doc.close()
            return i

    doc.close()
    return None


def truncate_pdf(input_pdf, output_pdf):
    truncate_from = find_truncation_page(input_pdf)

    doc = fitz.open(input_pdf)

    # No truncation → save full PDF
    if truncate_from is None:
        print("ℹ️ Saving full PDF without truncation, len pages:", len(doc))
        doc.save(output_pdf)
        doc.close()
        return

    new_doc = fitz.open()

    if truncate_from > 0:
        print(f"✂️ Truncating PDF from page {truncate_from + 1}, keeping first {truncate_from} pages.")
        new_doc.insert_pdf(doc, from_page=0, to_page=truncate_from - 1)

    new_doc.save(output_pdf)
    doc.close()
    new_doc.close()


def process_folder():
    for file in os.listdir(INPUT_FOLDER):
        if not file.lower().endswith(".pdf"):
            continue

        input_path = os.path.join(INPUT_FOLDER, file)
        output_path = os.path.join(OUTPUT_FOLDER, file)

        print(f"📄 Processing: {file}")
        truncate_pdf(input_path, output_path)
        print(f"✅ Saved: {output_path}")

INPUT_FOLDER = "pdfs_1"
OUTPUT_FOLDER = "clean_pdfs"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

if __name__ == "__main__":
    process_folder()
