import asyncio
import os
import io
import argparse
import pandas as pd
import requests
import json
from datetime import datetime
from thefuzz import process
from tqdm.asyncio import tqdm
from bs4 import BeautifulSoup
import time
import threading
import concurrent.futures

class RateLimiter:
    def __init__(self, max_calls, period=1.0):
        self.max_calls = max_calls
        self.period = period
        self.tokens = max_calls
        self.lock = threading.Lock()
        self.last_update = time.monotonic()

    def wait(self):
        with self.lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last_update
                # Refill tokens based on elapsed time
                new_tokens = elapsed * (self.max_calls / self.period)
                if new_tokens > 0:
                    self.tokens = min(self.max_calls, self.tokens + new_tokens)
                    self.last_update = now
                
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                else:
                    # Calculate time to wait for 1 token
                    needed = 1.0 - self.tokens
                    wait_time = needed * (self.period / self.max_calls)
                    time.sleep(wait_time)

class SECScraper:
    def __init__(self, limit=10, year=None, last_n_years=None, workers=5, output_dir="sec-edgar-filings", headless=True, dry_run=False, cik=None, ticker=None):
        self.cik = cik
        self.ticker = ticker
        self.limit = limit
        self.year = year
        self.last_n_years = last_n_years
        self.workers = workers
        self.output_dir = output_dir
        self.headless = headless
        self.dry_run = dry_run
        
        # SEC Limit is 10 req/s. We set to 9 to be safe.
        self.rate_limiter = RateLimiter(max_calls=9, period=1.0)
        
        # Date Logic
        self.current_year = datetime.now().year
        if self.year:
            self.start_year = int(self.year)
            self.end_year = int(self.year)
        elif self.last_n_years:
            self.start_year = self.current_year - self.last_n_years + 1
            self.end_year = self.current_year
        else:
            # Default to current year if nothing specified (or maybe just latest?)
            # For this custom scraper, let's default to last 1 year to be safe
            self.start_year = self.current_year
            self.end_year = self.current_year

        self.tickers_df = None

    def fetch_tickers(self):
        """Fetches official SEC tickers synchronously."""
        print("Fetching SEC tickers...")
        headers = {"User-Agent": "Kineviz scraper dienert@kineviz.com"}
        try:
            resp = requests.get(
                "https://www.sec.gov/files/company_tickers.json",
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            self.tickers_df = pd.DataFrame.from_dict(data, orient='index')
            print(f"Loaded {len(self.tickers_df)} tickers.")
        except Exception as e:
            # Allow explicit --ticker runs to proceed even when SEC ticker
            # metadata endpoint is temporarily unavailable/network-restricted.
            print(f"Warning: unable to fetch SEC ticker metadata ({e}).")
            print("Proceeding with ticker-as-CIK fallback for explicit ticker mode.")
            self.tickers_df = pd.DataFrame(columns=["ticker", "cik_str", "title"])

    def resolve_ticker(self, company_name):
        """Resolves company name to Ticker/CIK."""
        choices = self.tickers_df['title'].tolist()
        best_match, score = process.extractOne(company_name, choices)
        if score < 85:
            return None, None
        row = self.tickers_df[self.tickers_df['title'] == best_match].iloc[0]
        return row['ticker'], str(row['cik_str']).zfill(10) # CIK is usually 10 digits

    async def run(self):
        self.fetch_tickers()
        
        if self.cik or self.ticker:
            # Handle single CIK/Ticker mode
            if self.cik:
                cik_padded = str(self.cik).zfill(10)
                match = self.tickers_df[self.tickers_df['cik_str'].astype(str).str.zfill(10) == cik_padded]
            else:
                ticker_upper = self.ticker.upper()
                match = self.tickers_df[self.tickers_df['ticker'] == ticker_upper]

            if not match.empty:
                row = {
                    'Company': match.iloc[0]['title'],
                    'Ticker': match.iloc[0]['ticker'],
                    'CIK': str(match.iloc[0]['cik_str']).zfill(10)
                }
            else:
                if self.cik:
                    val = str(self.cik).zfill(10)
                    row = {'Company': f"CIK {val}", 'Ticker': val, 'CIK': val}
                else:
                    # SEC browse endpoint accepts ticker in CIK parameter.
                    # This keeps explicit ticker runs working without metadata.
                    ticker_upper = self.ticker.upper()
                    print(
                        f"Warning: ticker {ticker_upper} not found in SEC metadata; "
                        "using ticker-as-CIK fallback."
                    )
                    row = {'Company': ticker_upper, 'Ticker': ticker_upper, 'CIK': ticker_upper}

            target_companies = [row]
        else:
            # Load Fortune 500
            try:
                f500_df = pd.read_csv("list.csv")
                target_companies = [row for _, row in f500_df.head(self.limit).iterrows()]
            except FileNotFoundError:
                print("Error: list.csv not found.")
                return

        print(f"Starting crawl for {len(target_companies)} unit(s) with {self.workers} workers...")
        
        # Use ThreadPoolExecutor for blocking requests
        loop = asyncio.get_running_loop()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as pool:
            tasks = []
            for row in target_companies:
                tasks.append(loop.run_in_executor(pool, self.process_company, row))
            
            for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping"):
                try:
                    res = await f
                    # Optionally log errors
                    if res and "Error" in res:
                        tqdm.write(res)
                except Exception as e:
                    tqdm.write(f"Task failed: {e}")
            
        print("Done.")

    def _get_request(self, url, headers, stream=False, timeout=30):
        """Rate-limited wrapper for requests.get"""
        self.rate_limiter.wait()
        return requests.get(url, headers=headers, stream=stream, timeout=timeout)

    def process_company(self, row):
        company_name = row.get('Company')
        cik = row.get('CIK')
        ticker = row.get('Ticker')
        
        if not cik:
            ticker, cik = self.resolve_ticker(company_name)
        
        if not ticker:
            return f"Skipped {company_name}: No match"

        # Headers for requests
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 AntigravityBot/1.0 (generic@antigravity.com)",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.sec.gov/"
        }
        
        try:
            date_b = f"{self.end_year}1231"
            
            for filing_type in ["10-K"]:

                # https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=AAPL&type=10-K&dateb=20241231&owner=exclude&count=100
                url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={filing_type}&dateb={date_b}&owner=exclude&count=100"
                
                # tqdm.write(f"[{ticker}] Fetching {url}")
                resp = self._get_request(url, headers=headers, timeout=30)
                if resp.status_code != 200:
                    tqdm.write(f"[{ticker}] Failed to fetch results for {filing_type}: {resp.status_code}")
                    continue
                
                soup = BeautifulSoup(resp.content, "html.parser")
                
                # Table selector: class="tableFile2"
                table = soup.find("table", class_="tableFile2")
                if not table:
                    tqdm.write(f"[{ticker}] No results table found for {filing_type}")
                    continue
                
                rows = table.find_all("tr")
                
                for r in rows:
                    cols = r.find_all("td")
                    if len(cols) < 4: continue # Skip header or invalid rows
                    
                    # Col 3 is Date (YYYY-MM-DD)
                    filing_date_str = cols[3].text.strip()
                    try:
                        filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d")
                    except ValueError:
                        continue
                    
                    if filing_date.year < self.start_year:
                        break # Too old
                        
                    if filing_date.year > self.end_year:
                        continue # Too new? (Shouldn't happen with dateb logic usually)
                        
                    # Found valid filing. Get Document Link
                    # Col 1 is "Format" with link to Documents
                    doc_link_el = cols[1].find("a")
                    if not doc_link_el: continue
                    
                    doc_href = doc_link_el['href'] # /Archives/edgar/data/CIK/Accession/...
                    
                    try:
                        # Extract Accession from URL: /data/CIK/Accession/index.htm
                        parts = doc_href.split("/")
                        if len(parts) >= 6:
                            accession = parts[5]
                        else:
                            # Fallback regex or skip
                            continue
                            
                        full_doc_url = "https://www.sec.gov" + doc_href
                        
                        # Fetch Document Page to find full submission txt
                        doc_resp = self._get_request(full_doc_url, headers=headers, timeout=30)
                        if doc_resp.status_code != 200:
                            continue
                            
                        doc_soup = BeautifulSoup(doc_resp.content, "html.parser")
                        
                        # Look for "Complete Submission Text File" link
                        txt_link = None
                        for a in doc_soup.find_all("a"):
                            if a.text and "Complete Submission Text File" in a.text:
                                txt_link = a['href']
                                break
                            if a.get('href', '').endswith(".txt"):
                                # Check if it's likely the main file (same accession)
                                if accession in a.get('href', ''):
                                    txt_link = a['href']
                                    
                        if txt_link:
                            download_url = "https://www.sec.gov" + txt_link
                            
                            save_path = os.path.join(self.output_dir, ticker, filing_type, accession, "full-submission.txt")
                            os.makedirs(os.path.dirname(save_path), exist_ok=True)
                            
                            if not os.path.exists(save_path):
                                if self.dry_run:
                                    tqdm.write(f"[Dry Run] Would download {filing_type} from {filing_date_str} to {save_path}")
                                    continue
                                    
                                # Download
                                file_resp = self._get_request(download_url, headers=headers, stream=True, timeout=60)
                                if file_resp.status_code == 200:
                                    with open(save_path, "wb") as f:
                                        for chunk in file_resp.iter_content(chunk_size=8192):
                                            f.write(chunk)
                    except Exception as e:
                        tqdm.write(f"[{ticker}] Error processing row: {e}")
                        continue
        except Exception as e:
            return f"Error {ticker}: {e}"
        
        return f"Processed {ticker}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--year", type=str)
    parser.add_argument("--last-n-years", type=int)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--output-dir", type=str, default="test")
    parser.add_argument("--cik", type=str, help="Specify a single CIK to process (skips list.csv)")
    parser.add_argument("--ticker", type=str, help="Specify a single Ticker to process (skips list.csv)")
    parser.add_argument("--headless", action="store_true", default=True) # Default headless
    parser.add_argument("--no-headless", action="store_false", dest="headless")
    parser.add_argument("--dry-run", action="store_true")
    
    args = parser.parse_args()
    
    scraper = SECScraper(
        limit=args.limit,
        year=args.year,
        last_n_years=args.last_n_years,
        workers=args.workers,
        output_dir=args.output_dir,
        headless=args.headless,
        dry_run=args.dry_run,
        cik=args.cik,
        ticker=args.ticker
    )
    
    asyncio.run(scraper.run())
