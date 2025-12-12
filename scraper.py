import argparse
import pandas as pd
import requests
from sec_edgar_downloader import Downloader
from thefuzz import process
from tqdm import tqdm

def get_sec_tickers():
    """
    Fetches the official SEC company tickers JSON and returns it as a DataFrame.
    """
    headers = {
        "User-Agent": "AntigravityBot generic@antigravity.com"
    }
    url = "https://www.sec.gov/files/company_tickers.json"
    print(f"Fetching SEC tickers from {url}...")
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    # The JSON is indexed by a number, so we need to transform it
    # Format: { "0": { "cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc." }, ... }
    df = pd.DataFrame.from_dict(data, orient='index')
    print(f"Loaded {len(df)} tickers from SEC.")
    return df

def find_ticker(company_name, tickers_df):
    """
    Finds the best matching ticker/CIK for a given company name using fuzzy matching.
    """
    # Create a map of title -> ticker/cik
    choices = tickers_df['title'].tolist()
    
    # Extract the best match
    best_match, score = process.extractOne(company_name, choices)
    
    if score < 85: # Threshold for confidence
        print(f"Warning: Low match score ({score}) for '{company_name}' -> '{best_match}'. Skipping.")
        return None, None
        
    row = tickers_df[tickers_df['title'] == best_match].iloc[0]
    return row['ticker'], row['cik_str']

def main():
    parser = argparse.ArgumentParser(description="SEC Scraper for Fortune 500")
    parser.add_argument("--limit", type=int, default=10, help="Number of companies to process")
    parser.add_argument("--year", type=str, help="Specific year of filings to download (e.g., 2024)")
    parser.add_argument("--last-n-years", type=int, help="Download filings for the last N years (including current)")
    parser.add_argument("--dry-run", action="store_true", help="Resolve tickers but do not download filings")
    args = parser.parse_args()

    # Determine date range and internal download limit
    after_date = None
    before_date = None
    download_limit_10k = 1
    download_limit_10q = 1
    
    import datetime
    current_year = datetime.datetime.now().year

    if args.year:
        after_date = f"{args.year}-01-01"
        before_date = f"{args.year}-12-31"
        download_limit_10k = 4 
        download_limit_10q = 4
        print(f"Downloading filings for year: {args.year}")
    elif args.last_n_years:
        start_year = current_year - args.last_n_years + 1
        after_date = f"{start_year}-01-01"
        before_date = f"{current_year}-12-31"
        # 10-K: 1/year + buffer
        download_limit_10k = args.last_n_years + 2
        # 10-Q: 3/year + buffer
        download_limit_10q = (args.last_n_years * 3) + 4
        print(f"Downloading filings from {start_year} to {current_year}")

    # 1. Load Fortune 500 list
    csv_path = "list.csv"
    print(f"Loading companies from {csv_path}...")
    try:
        f500_df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"Error: {csv_path} not found.")
        return

    # 2. Load SEC Tickers
    sec_df = get_sec_tickers()

    # 3. Process top N companies
    target_companies = f500_df.head(args.limit)
    
    # Initialize Downloader
    # Note: The library handles rate limiting (10 requests/sec) automatically.
    print("Initializing Downloader...")
    import time
    time.sleep(2)
    dl = Downloader("AntigravityBot", "generic@antigravity.com")

    # Wrap iteration with tqdm
    for index, row in tqdm(target_companies.iterrows(), total=target_companies.shape[0], desc="Scraping Companies", unit="company"):
        company_name = row['Company']
        rank = row['Rank']
        
        # tqdm.write allows printing without breaking the progress bar
        # tqdm.write(f"Processing #{rank}: {company_name}")
        
        ticker, cik = find_ticker(company_name, sec_df)
        
        if not ticker:
            tqdm.write(f"Could not resolve ticker for {company_name}")
            continue
            
        # tqdm.write(f"  -> Resolved to: {ticker} (CIK: {cik})")
        
        if args.dry_run:
            tqdm.write(f"  Dry run: Skipping download for {ticker}.")
            continue
            
        try:
            # Download 10-K
            # tqdm.write(f"  Downloading 10-K for {ticker}...")
            dl.get("10-K", ticker, limit=download_limit_10k, after=after_date, before=before_date, download_details=False)
            
            # Download 10-Q
            # tqdm.write(f"  Downloading 10-Q for {ticker}...")
            dl.get("10-Q", ticker, limit=download_limit_10q, after=after_date, before=before_date, download_details=False)
            
        except Exception as e:
            tqdm.write(f"  Error downloading filings for {company_name}: {e}")

if __name__ == "__main__":
    main()
