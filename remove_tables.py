import os
import re
import io

import fitz  # PyMuPDF
import pdfplumber
import pytesseract
from PIL import Image

pytesseract.pytesseract.tesseract_cmd = r"C:\Users\AkashKumar\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"
# ================= CONFIG =================

DOLLAR_THRESHOLD = 50
TEXT_THRESHOLD = 500

# =========================================


def is_image_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    for page in doc:
        # Look for maximum 5 pages to decide
        if page.number == 5:
            break
        page_len = len(page.get_text().strip())
        if page_len > TEXT_THRESHOLD:
            doc.close()
            return False
    doc.close()
    return True


def ocr_page_text(page):
    pix = page.get_pixmap(dpi=300)
    img = Image.open(io.BytesIO(pix.tobytes("png")))
    return pytesseract.image_to_string(img)


def find_truncation_page(pdf_path):
    # -------- TEXT PDF --------
    if not is_image_pdf(pdf_path):
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                dollar_count = len(re.findall(r"\$", text))
                if dollar_count > DOLLAR_THRESHOLD:
                    return i
        return None
    print("ℹ️ Detected as image-based PDF")
    # -------- IMAGE PDF (OCR) --------
    doc = fitz.open(pdf_path)
    for i, page in enumerate(doc):
        text = ocr_page_text(page)
        dollar_count = len(re.findall(r"\$", text))
        if dollar_count > DOLLAR_THRESHOLD:
            doc.close()
            return i

    doc.close()
    return None


def truncate_pdf(input_pdf, output_pdf):
    truncate_from = find_truncation_page(input_pdf)
    doc = fitz.open(input_pdf)

    # No truncation → save full PDF
    if truncate_from is None:
        print("ℹ️ No truncation needed, len of doc:", len(doc))
        doc.save(output_pdf)
        doc.close()
        return

    new_doc = fitz.open()
    if truncate_from > 0:
        print("🛑 Truncating from page:", truncate_from)
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
