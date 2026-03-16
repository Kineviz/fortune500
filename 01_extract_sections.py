import os
import re
import json
import argparse
from bs4 import BeautifulSoup, ProcessingInstruction
import concurrent.futures
from tqdm import tqdm

# Reuse clean_html logic (simplified/adapted from parser.py)
def clean_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove Processing Instructions
    for element in soup.find_all(string=lambda text: isinstance(text, ProcessingInstruction)):
        element.extract()

    # Remove scripts and styles
    for script in soup(["script", "style"]):
        script.decompose()
        
    # Unwrap XBRL/XML
    for tag_name in ["xbrl", "xml"]:
        for tag in soup.find_all(tag_name):
            tag.unwrap()
            
    # Remove metadata garbage
    for tag_name in ["ix:header", "ix:hidden", "filename", "description", "type", "sequence", "title"]:
        for tag in soup.find_all(tag_name):
            tag.decompose()
        
    return str(soup)

def extract_sections_from_text(text):
    """
    Splits 10-K text into sections based on "Item" headers.
    Returns a dict {section_name: content}
    """
    # Regex for 10-K Item headers
    # Examples: "Item 1.", "Item 1A.", "ITEM 7."
    # We look for "Item" followed by a number, optional letter, and a period, 
    # at the start of a line or preceded by newlines.
    
    # Simple regex to find start indices of items
    # Note: parsing 10-Ks is notoriously hard due to formatting variations.
    # This is a heuristic approach.
    
    # Standard items in 10-K
    # Part I: 1, 1A, 1B, 2, 3, 4
    # Part II: 5, 6, 7, 7A, 8, 9, 9A, 9B
    # Part III: 10, 11, 12, 13, 14
    # Part IV: 15
    
    pattern = re.compile(r'(?i)\n\s*(ITEM\s+(?:1[0-5]|[1-9])[AB]?\.?)(.*?)(?=\n\s*ITEM\s+(?:1[0-5]|[1-9])[AB]?\.?|$)', re.DOTALL)
    
    sections = {}
    
    # Another strategy: Split by the headers
    # We want to capture the header to know which item it is
    
    # Let's try iterating through matches
    matches = list(pattern.finditer(text))
    
    for i, match in enumerate(matches):
        header = match.group(1).strip().replace('\n', ' ')
        content = match.group(2).strip()
        
        # Normalize header: ITEM 1. -> Item 1
        header_norm = re.sub(r'\s+', ' ', header).title()
        if not header_norm.endswith('.'):
            header_norm += '.'
            
        if header_norm in sections:
            if len(content) <= len(sections[header_norm]):
                continue
             
        sections[header_norm] = content
        
    return sections

def process_filing(args):
    filepath, output_base, ticker, year = args
    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            
        # Find 10-K Document
        # <TYPE>10-K
        doc_start = re.search(r'<TYPE>10-K\s*', content, re.IGNORECASE)
        if not doc_start:
            return None # Not a 10-K or missing
            
        # Find start of text for this doc
        # We need to find the <TEXT> tag *after* this TYPE tag, but before the next DOCUMENT
        
        # Slicing from the TYPE found
        sub_content = content[doc_start.start():]
        text_match = re.search(r'<TEXT>(.*?)</TEXT>', sub_content, re.DOTALL)
        
        if not text_match:
            return None
            
        raw_html = text_match.group(1)
        
        # Clean HTML to Text
        # Note: We're doing HTML -> Text conversion here to make it clean for LLM
        # using BeautifulSoup get_text() after cleaning
        soup = BeautifulSoup(clean_html(raw_html), 'html.parser')
        clean_text = soup.get_text(separator='\n\n')
        
        # Find Company Name
        # COMPANY CONFORMED NAME:                 CINTAS CORP
        company_name_match = re.search(r'COMPANY CONFORMED NAME:\s+(.+)', sub_content[:1000] if 'sub_content' in locals() else content[:3000])
        # Fallback if sub_content not defined yet or name is earlier
        if not company_name_match:
             company_name_match = re.search(r'COMPANY CONFORMED NAME:\s+(.+)', content[:5000])
             
        company_name = company_name_match.group(1).strip() if company_name_match else ticker

        # Extract Extended Metadata
        metadata = {
            "cik": None,
            "sic": None,
            "irs_number": None,
            "state_of_inc": None,
            "org_name": None,
            "sec_file_number": None,
            "film_number": None,
            "business_address": None,
            "mail_address": None
        }

        # Metadata Parsing
        # The SGML header is at the very beginning of the file, before the <DOCUMENT> tags.
        # We must search 'content', not 'sub_content' (which starts at <TYPE>10-K).
        header_content = content[:10000]

        # Helper to extract address block
        def extract_address(block_name, text):
            # Look for BLOCK NAME: ... until next block with indentation logic or empty line
            match = re.search(rf'{block_name}:\s*(.*?)(?=\n\s*[A-Z ]+:|$)', text, re.DOTALL)
            if match:
                addr_text = match.group(1)
                # Parse lines like "STREET 1: ..."
                addr_parts = {}
                for line in addr_text.split('\n'):
                    if ':' in line:
                        key, val = line.split(':', 1)
                        # Normalize key: STREET 1 -> street_1
                        norm_key = key.strip().lower().replace(' ', '_')
                        addr_parts[norm_key] = val.strip()
                return addr_parts
            return {}

        metadata["business_address"] = extract_address("BUSINESS ADDRESS", header_content)
        metadata["mail_address"] = extract_address("MAIL ADDRESS", header_content)
        
        # Regex Patterns for other fields
        field_patterns = {
            "cik": r'CENTRAL INDEX KEY:\s+(.+)',
            "sic": r'STANDARD INDUSTRIAL CLASSIFICATION:\s+(.+)',
            "irs_number": r'IRS NUMBER:\s+(.+)',
            "state_of_inc": r'STATE OF INCORPORATION:\s+(.+)',
            "org_name": r'ORGANIZATION NAME:\s+(.+)',
            "sec_file_number": r'SEC FILE NUMBER:\s+(.+)',
            "film_number": r'FILM NUMBER:\s+(.+)'
        }

        for key, pattern in field_patterns.items():
             m = re.search(pattern, header_content)
             if m:
                 metadata[key] = m.group(1).strip()


        # Extract Accession Number from path
        # filepath: .../10-K/0000320193-20-000096/full-submission.txt
        accession_number = os.path.basename(os.path.dirname(filepath))
        accession_nodash = accession_number.replace('-', '')

        # EXTRACT FILING YEAR FROM HEADER
        # CONFORMED PERIOD OF REPORT:	20211231
        period_match = re.search(r'CONFORMED PERIOD OF REPORT:\s+(\d{4})', header_content)
        if period_match:
            extracted_year = period_match.group(1)
        else:
            extracted_year = str(year) # Fallback to path year if extraction fails

        # Extract Primary Document Filename
        # Look for <FILENAME> in the same 10-K document block (sub_content)
        # It usually appears after <TYPE> and before <TEXT>
        filename_match = re.search(r'<FILENAME>(.+?)(?=\n|<)', sub_content, re.IGNORECASE)
        primary_doc_filename = filename_match.group(1).strip() if filename_match else "main.htm" # Fallback

        # Construct Filing URL
        # Format: https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{accession_nodash}/{filename}
        cik_str = metadata.get("cik", "0")
        if cik_str:
            cik_int = str(int(cik_str)) # Remove leading zeros
        else:
            cik_int = "0"
            
        filing_url = f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik_int}/{accession_nodash}/{primary_doc_filename}"


        # Extract Sections
        sections = extract_sections_from_text(clean_text)
        
        if not sections:
            return None
            
        # Prepare Output
        output_dir = os.path.join(output_base, ticker, extracted_year)
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "sections.jsonl")
        
        # Extract filename from path for ID
        original_filename = os.path.basename(filepath)
        
        records = []
        for sec_id, sec_content in sections.items():
            record = {
                "filing_id": original_filename,
                "company": ticker,
                "company_name": company_name,
                "cik": metadata["cik"],
                "sic": metadata["sic"],
                "irs_number": metadata["irs_number"],
                "state_of_inc": metadata["state_of_inc"],
                "org_name": metadata["org_name"],
                "sec_file_number": metadata["sec_file_number"],
                "film_number": metadata["film_number"],
                
                # Flattened Business Address
                "business_street_1": metadata["business_address"].get("street_1"),
                "business_street_2": metadata["business_address"].get("street_2"),
                "business_city": metadata["business_address"].get("city"),
                "business_state": metadata["business_address"].get("state"),
                "business_zip": metadata["business_address"].get("zip"),
                "business_phone": metadata["business_address"].get("business_phone"),
                
                # Flattened Mail Address
                "mail_street_1": metadata["mail_address"].get("street_1"),
                "mail_street_2": metadata["mail_address"].get("street_2"),
                "mail_city": metadata["mail_address"].get("city"),
                "mail_state": metadata["mail_address"].get("state"),
                "mail_state": metadata["mail_address"].get("state"),
                "mail_zip": metadata["mail_address"].get("zip"),

                # Constructed Filing URL
                "filing_url": filing_url,

                "year": int(extracted_year),
                "section_id": sec_id,
                "content": sec_content
            }
            records.append(record)
            
        # Write JSONL
        with open(output_file, 'w', encoding='utf-8') as f_out:
            for r in records:
                f_out.write(json.dumps(r) + '\n')
                
        return f"{ticker}-{extracted_year}"
        
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Extract 10-K sections to JSONL")
    parser.add_argument("--input_base", default="data/sgml", help="Input directory")
    parser.add_argument("--output_base", default="data/json", help="Output directory")
    parser.add_argument("--ticker", help="Specific ticker to process")
    parser.add_argument("--year", help="Specific year to process")
    parser.add_argument("--workers", type=int, default=4)
    
    args = parser.parse_args()
    
    tasks = []
    
    # Walk directory
    # Structure: data/sgml/[Year]/[Ticker]/10-K/[Accession]/full-submission.txt
    # WAIT: Based on list_dir earlier: data/sgml/2020/[Ticker]/10-K/...
    # So ROOT is data/sgml/2020 (Year is parent of Ticker? Or Ticker parent of Year?)
    # Let's check listing again.
    # Step 10: data/sgml contains "2020" directory.
    # Step 14: data/sgml/2020 contains "AAPL", etc.
    # So structure represents: data/sgml/[Year]/[Ticker]/...
    
    # However, standard scraper usually does Ticker/Year or similar. 
    # Let's adjust walker to be generic or specific to observed structure.
    
    input_base = os.path.abspath(args.input_base)
    output_base = os.path.abspath(args.output_base)
    
    print(f"Scanning {input_base}...")
    
    for root, dirs, files in os.walk(input_base):
        if "full-submission.txt" in files:
            # Infer metadata from path
            # path: .../sgml/2020/AAPL/10-K/0000.../full-submission.txt
            parts = root.split(os.sep)
            # print(f"Checking {root}")
            
            # Try to find Year and Ticker in path
            # We know "10-K" is likely in path
            if "10-K" in parts:
                idx = parts.index("10-K")
                # Ticker should be the parent of 10-K
                curr_ticker = parts[idx-1]
                
                # Try to find Year in path as well (either parent of Ticker or child of Accession etc)
                # But we'll rely on content extraction for the final output path.
                # We just need a candidate year for filtering.
                curr_year = "unknown"
                if idx >= 2 and len(parts[idx-2]) == 4 and parts[idx-2].isdigit():
                    curr_year = parts[idx-2]
                
                # Apply filters
                if args.ticker and args.ticker.upper() != curr_ticker.upper():
                    continue
                if args.year and str(args.year) != str(curr_year):
                    continue
                    
                filepath = os.path.join(root, "full-submission.txt")
                tasks.append((filepath, output_base, curr_ticker, curr_year))

    print(f"Found {len(tasks)} filings to process.")
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_filing, t) for t in tasks]
        for _ in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            pass
            
    print("Done.")

if __name__ == "__main__":
    main()
