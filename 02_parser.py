import os
import re
import base64
import binascii
import io
import argparse
from bs4 import BeautifulSoup, ProcessingInstruction
from markdownify import markdownify as md
from tqdm import tqdm

def uudecode_line(line):
    """
    Decodes a single line of uuencoded data.
    Tries strict binascii first, falls back to manual decoding for malformed/padded lines.
    """
    if not line: return b""
    
    # Try efficient binascii first
    try:
        return binascii.a2b_uu(line)
    except (binascii.Error, ValueError):
        pass
        
    # Manual Fallback
    # Length
    n = ord(line[0]) - 32
    if n <= 0: return b""
    
    decoded = bytearray()
    
    try:
        # Body starts at index 1
        # Process 4 chars -> 3 bytes
        for i in range(1, len(line), 4):
            chunk = line[i:i+4]
            if len(chunk) < 4: break 
            
            vals = []
            for c in chunk:
                v = ord(c) - 32
                vals.append(v & 0x3F)
                
            c1, c2, c3, c4 = vals
            
            b1 = (c1 << 2) | (c2 >> 4)
            b2 = ((c2 & 0xF) << 4) | (c3 >> 2)
            b3 = ((c3 & 0x3) << 6) | c4
            
            decoded.append(b1)
            decoded.append(b2)
            decoded.append(b3)
            
        return bytes(decoded[:n])
    except Exception:
        return b""

def uudecode_content(encoded_text):
    """
    Decodes UUEncoded text content.
    """
    try:
        lines = encoded_text.strip().splitlines()
        
        start_idx = -1
        for i, line in enumerate(lines):
            if line.startswith("begin "):
                start_idx = i
                break
        
        if start_idx == -1:
            return None 

        # Decode line by line until "end"
        decoded = bytearray()
        for line in lines[start_idx+1:]:
            if line == "end":
                break
            if not line: continue
            
            chunk = uudecode_line(line)
            decoded.extend(chunk)
                
        return bytes(decoded)
            
    except Exception:
        return None

def clean_html(html_content):
    """
    Cleans HTML content using BeautifulSoup before conversion.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove Processing Instructions (<?xml ... ?>)
    for element in soup.find_all(string=lambda text: isinstance(text, ProcessingInstruction)):
        element.extract()

    # Remove scripts and styles
    for script in soup(["script", "style"]):
        script.decompose()
        
    # Handle Inline XBRL (iXBRL)
    # 1. Un-nest the main content if it's wrapped in <XBRL> or <XML>
    #    Common in modern filings: <DOCUMENT><TEXT><XBRL> ...html... </XBRL></TEXT></DOCUMENT>
    #    We want the HTML inside, so we UNWRAP the parent container.
    for tag_name in ["xbrl", "xml"]:
        for tag in soup.find_all(tag_name):
            tag.unwrap()
            
    # 2. Remove metadata/hidden sections that contain raw data (the "garbage" text)
    #    ix:header -> usually metadata
    #    ix:hidden -> hidden facts
    #    FILENAME/DESCRIPTION/TYPE/SEQUENCE -> SGML metadata often leaked into TEXT
    for tag_name in ["ix:header", "ix:hidden", "filename", "description", "type", "sequence", "title"]:
        for tag in soup.find_all(tag_name):
            tag.decompose()
        
    return str(soup)

def parse_sgml_filing(filepath, output_dir):
    """
    Parses a single SEC SGML filing (full-submission.txt).
    Extracts documents (10-K, 10-Q, EX-*, GRAPHIC) and saves them to output_dir.
    """
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

    # Regex to find <DOCUMENT> blocks
    # Flags: Dotall to match newlines
    doc_pattern = re.compile(r'<DOCUMENT>(.*?)</DOCUMENT>', re.DOTALL)
    
    matches = doc_pattern.findall(content)
    
    if not matches:
        return # No documents found

    # Metadata extraction helper
    def extract_tag(block, tag):
        m = re.search(f"<{tag}>(.*)", block, re.IGNORECASE)
        return m.group(1).strip() if m else None

    # Extract Header Metadata (CIK, Accession)
    cik = None
    accession = None
    
    # Matches usually for header tags
    # CENTRAL INDEX KEY: \s* (\d+)
    # ACCESSION NUMBER: \s* ([\d-]+)
    
    cik_match = re.search(r'CENTRAL INDEX KEY:\s*(\d+)', content)
    if cik_match:
        cik = str(int(cik_match.group(1))) # Strip leading zeros
        
    acc_match = re.search(r'ACCESSION NUMBER:\s*([\d-]+)', content)
    if acc_match:
        accession = acc_match.group(1).replace('-', '')
        
    for block in matches:
        doc_type = extract_tag(block, "TYPE")
        filename = extract_tag(block, "FILENAME")
        
        # Extract Content between <TEXT> ... </TEXT>
        text_match = re.search(r'<TEXT>(.*?)</TEXT>', block, re.DOTALL)
        if not text_match:
            continue
            
        doc_content = text_match.group(1)
        
        if not filename:
            # Fallback filename if missing
            filename = f"doc_{doc_type}.txt"

        # Sanitize filename
        safe_filename = os.path.basename(filename)
        
        # Determine strict output path
        # If passed output_dir is "data/markdown/...", then use that.
        out_path = os.path.join(output_dir, safe_filename)
        
        # User Request: Extract ONLY full-submission.md (Main 10-K/10-Q), Images, and Spreadsheets.
        # Filter based on type and extension.
        
        is_main_doc = doc_type in ["10-K", "10-Q"]
        
        # Extensions interested in
        # Images: .jpg, .gif, .png
        # Spreadsheets: .xlsx, .xls, .csv
        ext = os.path.splitext(filename)[1].lower() if filename else ""
        
        is_image = doc_type == "GRAPHIC" or ext in ['.jpg', '.gif', '.png', '.jpeg']
        is_spreadsheet = ext in ['.xlsx', '.xls', '.csv']
        
        if not (is_main_doc or is_image or is_spreadsheet):
            continue
            
        # Handle Binary Types
        # GRAPHIC = Images (uuencoded)
        # EXCEL = Excel files (uuencoded)
        # ZIP = Zip files (uuencoded)
        # PDF = PDF files (uuencoded)
        binary_types = ["GRAPHIC", "EXCEL", "ZIP", "PDF"]
        
        if doc_type in binary_types or filename.lower().endswith(('.zip', '.xlsx', '.xls', '.pdf', '.jpg', '.gif')):
            # Attempt to decode UUEncoded content
            decoded_bytes = uudecode_content(doc_content)
            
            if decoded_bytes:
                # Save binary content
                with open(out_path, 'wb') as f_out:
                    f_out.write(decoded_bytes)
            else:
                pass
                
        # Handle Text/HTML Documents (10-K, 10-Q, Exhibits)
        else:
            # Check if content looks like HTML
            is_html = "<html" in doc_content.lower() or "<body" in doc_content.lower()
            
            if is_html:
                # Convert to Markdown
                cleaned_html = clean_html(doc_content)
                
                # Markdown conversion
                markdown_text = md(cleaned_html, heading_style="ATX")
                
                # Rename .htm/.html to .md
                base, _ = os.path.splitext(safe_filename)
                md_filename = f"{base}.md"
                md_out_path = os.path.join(output_dir, md_filename)
                
                # Special case for primary document logic:
                if doc_type in ["10-K", "10-Q"]:
                     md_out_path = os.path.join(output_dir, "full-submission.md")
                     
                     # Prepend Link if available
                     if cik and accession and filename:
                         # https://www.sec.gov/ix?doc=/Archives/edgar/data/320193/000032019320000096/aapl-20200926.htm
                         link = f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{accession}/{filename}"
                         markdown_text = f"[SEC Filing]({link})\n\n" + markdown_text
                
                with open(md_out_path, 'w', encoding='utf-8') as f_out:
                    f_out.write(markdown_text)
            else:
                # Plain text, save as is (or .txt)
                with open(out_path, 'w', encoding='utf-8') as f_out:
                    f_out.write(doc_content)

import concurrent.futures
import multiprocessing

def process_filing(args):
    """
    Wrapper for parse_sgml_filing to be used with ProcessPoolExecutor.
    args: tuple(filing_path, input_base, output_base)
    """
    filing_path, input_base, output_base = args
    rel_path = os.path.relpath(os.path.dirname(filing_path), input_base)
    target_dir = os.path.join(output_base, rel_path)
    
    # Check if already processed
    if os.path.exists(os.path.join(target_dir, "full-submission.md")):
        return filing_path # treat as done
        
    os.makedirs(target_dir, exist_ok=True)
    parse_sgml_filing(filing_path, target_dir)
    return filing_path

def main():
    parser = argparse.ArgumentParser(description="Parse SEC SGML filings to Markdown/Images")
    parser.add_argument("--input_base", default="data/sgml", help="Input directory containing SGML filings")
    parser.add_argument("--output_base", default="data/markdown", help="Output directory for parsed files")
    parser.add_argument("--workers", type=int, default=multiprocessing.cpu_count(), help="Number of worker processes")
    parser.add_argument("--ticker", help="Specific ticker to process")
    
    args = parser.parse_args()
    
    input_base = os.path.abspath(args.input_base)
    output_base = os.path.abspath(args.output_base)
    
    filings = []
    
    # 1. Scan for files
    print(f"Scanning {input_base} for full-submission.txt...")
    for root, dirs, files in os.walk(input_base):
        for file in files:
            if file == "full-submission.txt":
                if args.ticker:
                    parts = root.split(os.sep)
                    if args.ticker.upper() not in [p.upper() for p in parts]:
                        continue
                filings.append(os.path.join(root, file))
                
    print(f"Found {len(filings)} filings to parse.")
    
    # 2. Process in parallel
    # Prepare arguments for each task
    tasks = [(f, input_base, output_base) for f in filings]
    
    print(f"Starting parsing with {args.workers} workers...")

    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = [executor.submit(process_filing, task) for task in tasks]
            for _ in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Parsing"):
                pass
    except PermissionError as e:
        # Some restricted environments disallow semaphores used by ProcessPool.
        print(f"Process pool unavailable ({e}). Falling back to single-process parsing.")
        for task in tqdm(tasks, total=len(tasks), desc="Parsing (fallback)"):
            process_filing(task)

    print("Parsing complete.")

if __name__ == "__main__":
    main()
