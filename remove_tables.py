import os
import re
import io
import fitz
import boto3, fitz
import configparser
import shutil

config = configparser.ConfigParser()
config.read('configs/config.ini')
 
awsAccessKey = config['DEFAULT']['aws_access_key_id']
awsSecretKey = config['DEFAULT']['aws_secret_access_key']
aws_region = config['DEFAULT']['region_name']
 
DOLLAR_THRESHOLD = 30
 
textract = boto3.client(service_name = 'textract', region_name = aws_region, aws_access_key_id = awsAccessKey, aws_secret_access_key = awsSecretKey)
 
def find_truncation_page(pdf_path):
    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
 
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
    try:
        truncate_from = find_truncation_page(input_pdf)
        with open(input_pdf, "rb") as f:
            pdf_bytes = f.read()
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    
        # No truncation → save full PDF
        if truncate_from is None:
            print("ℹ️ No truncation needed, len of doc:", len(doc))
            doc.save(output_pdf)
            doc.close()
            return True
    
        new_doc = fitz.open()
        if truncate_from > 0:
            print("🛑 Truncating from page:", truncate_from, "Total pages:", len(doc))
            new_doc.insert_pdf(doc, from_page=0, to_page=truncate_from - 1)
    
        new_doc.save(output_pdf)
        doc.close()
        new_doc.close()
        return True
    except Exception as e:
        print("❌ Error during truncation:", str(e))
        return False
 
 
def process_folder():
    start_time = os.times()
    for file in os.listdir(INPUT_FOLDER):
        if not file.lower().endswith(".pdf"):
            continue
        
        print(f"🔍 Processing file: {file}")
        input_path = os.path.join(INPUT_FOLDER, file)
        output_path = os.path.join(OUTPUT_FOLDER, file)
 
        res = truncate_pdf(input_path, output_path)
        if res:
            shutil.move(input_path, os.path.join(PROCESSED_FOLDER, file))

    end_time = os.times()
    print(f"⏱️ Total processing time: {end_time.elapsed - start_time.elapsed} seconds")

INPUT_FOLDER = "pdfs"
PROCESSED_FOLDER = "pdfs/processed_pdfs"
OUTPUT_FOLDER = "clean_pdfs"

os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
 
if __name__ == "__main__":
    process_folder()