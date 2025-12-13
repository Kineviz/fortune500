# Fortune 500 SEC Filing Scraper (Custom Edition)

A high-performance, custom-built Python scraper to download 10-K and 10-Q filings for Fortune 500 companies from the SEC EDGAR database.

This tool was built from scratch to bypass common anti-bot restrictions and improve performance over standard libraries. It uses `requests` and `BeautifulSoup` with `asyncio` for concurrent downloading.

## Features

- **Custom Implementation**: No dependency on `sec-edgar-downloader` or `datamule`. Scrapes SEC "Classic Browse" directly.
- **High Performance**: Downloads multiple filings concurrently.
- **Robustness**: Handles ticker resolution and SEC rate limiting (10 req/s compliant).
- **Flexible Filtering**:
    - Filter by specific year (`--year 2024`)
    - Filter by last N years (`--last-n-years 3`)
- **Dry Run**: Preview what would be downloaded without saving files (`--dry-run`).

## Dependencies

- `pandas`
- `requests`
- `beautifulsoup4`
- `thefuzz`
- `tqdm`

Install them via pip:

```bash
pip install pandas requests beautifulsoup4 thefuzz tqdm
```

## Usage

Run the `scraper.py` script from the command line.

### Basic Usage

Download filings for the top 10 companies for the current year:

```bash
python scraper.py --limit 10
```

### Advanced Usage

**Filter by Year:**
Download filings for the top 20 companies for the year 2023:
```bash
python scraper.py --limit 20 --year 2023
```

**Filter by Last N Years:**
Download filings for the top 50 companies for the last 5 years:
```bash
python scraper.py --limit 50 --last-n-years 5
```

**Dry Run (Simulation):**
See what would be downloaded without actually downloading/saving:
```bash
python scraper.py --limit 1 --year 2024 --dry-run
```

**Custom Output Directory:**
Save filings to a specific folder:
```bash
python scraper.py --limit 10 --output-dir my_custom_folder
```
(Default is `sec-edgar-filings`)

**Concurrency:**
Adjust the number of worker threads (default is 5):
```bash
python scraper.py --workers 10
```

### All Parameters Example

Run with all options combined:
```bash
python scraper.py --limit 50 --year 2024 --workers 20 --output-dir /tmp/sec_data --dry-run
```

## Output Structure

Filings are saved in the following directory structure:

```
sec-edgar-filings/
├── [Ticker]
│   ├── 10-K
│   │   └── [Accession Number]
│   │       └── full-submission.txt
│   └── 10-Q
│       └── [Accession Number]
│           └── full-submission.txt
```

Example:
```
sec-edgar-filings/
├── WMT
│   ├── 10-K
│   │   └── 0000104169-24-000056
│   │       └── full-submission.txt
...
```

## License
[MIT](https://choosealicense.com/licenses/mit/)
