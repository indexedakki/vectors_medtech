import os
import re
import io
import fitz
import boto3, fitz
import configparser
 
config = configparser.ConfigParser()
config.read('configs/config.ini')
 
awsAccessKey = config['DEFAULT']['aws_access_key_id']
awsSecretKey = config['DEFAULT']['aws_secret_access_key']
aws_region = config['DEFAULT']['region_name']
 
DOLLAR_THRESHOLD = 30
 
textract = boto3.client(service_name = 'textract', region_name = aws_region, aws_access_key_id = awsAccessKey, aws_secret_access_key = awsSecretKey)
 
def find_truncation_page(pdf_path):
    doc = fitz.open(pdf_path)
 
    for i, page in enumerate(doc):
        pix = page.get_pixmap(dpi=300)
        response = textract.analyze_document(
            Document={"Bytes": pix.tobytes("png")},
            FeatureTypes=["TABLES"]
        )
 
        dollar_count = sum(
            block["Text"].count("$")
            for block in response["Blocks"]
            if block["BlockType"] == "WORD"
        )
 
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
 
INPUT_FOLDER = "pdfs"
OUTPUT_FOLDER = "clean_pdfs"
 
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
 
if __name__ == "__main__":
    process_folder()