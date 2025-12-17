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
        
        # Extract Sections
        sections = extract_sections_from_text(clean_text)
        
        if not sections:
            return None
            
        # Prepare Output
        output_dir = os.path.join(output_base, ticker, str(year))
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "sections.jsonl")
        
        # Extract filename from path for ID
        original_filename = os.path.basename(filepath)
        
        records = []
        for sec_id, sec_content in sections.items():
            record = {
                "filing_id": original_filename,
                "company": ticker,
                "year": int(year) if year.isdigit() else year,
                "section_id": sec_id,
                "content": sec_content
            }
            records.append(record)
            
        # Write JSONL
        with open(output_file, 'w', encoding='utf-8') as f_out:
            for r in records:
                f_out.write(json.dumps(r) + '\n')
                
        return f"{ticker}-{year}"
        
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
    
    print(f"Scanning {input_base}...")
    
    for root, dirs, files in os.walk(input_base):
        if "full-submission.txt" in files:
            # Infer metadata from path
            # path: .../sgml/2020/AAPL/10-K/0000.../full-submission.txt
            parts = root.split(os.sep)
            
            # Try to find Year and Ticker in path
            # We know "10-K" is likely in path
            if "10-K" in parts:
                idx = parts.index("10-K")
                # Ticker should be idx-1
                # Year should be idx-2
                
                curr_ticker = parts[idx-1]
                curr_year = parts[idx-2]
                
                # Apply filters
                if args.ticker and args.ticker.upper() != curr_ticker.upper():
                    continue
                if args.year and str(args.year) != str(curr_year):
                    continue
                    
                filepath = os.path.join(root, "full-submission.txt")
                tasks.append((filepath, args.output_base, curr_ticker, curr_year))

    print(f"Found {len(tasks)} filings to process.")
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_filing, t) for t in tasks]
        for _ in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            pass
            
    print("Done.")

if __name__ == "__main__":
    main()
