# Fortune 500 SEC Scraper

This project contains a Python script `scraper.py` effectively designed to scrape **10-K** (Annual) and **10-Q** (Quarterly) filings from the SEC EDGAR database for Fortune 500 companies.

## Features

- **Automated Ticker Resolution**: Automatically matches company names from `list.csv` to official SEC tickers (e.g., "Walmart" -> "WMT") using fuzzy string matching.
- **Robust Downloading**: Uses the `sec-edgar-downloader` library to handle SEC rate limiting and directory organization.
- **Flexible Date Filtering**: Support for downloading filings for a specific year or the last N years.
- **Progress Tracking**: Includes a progress bar to monitor extraction status.

## Prerequisites

Ensure you have Python 3.9+ installed.

### Installation

1.  Clone the repository or navigate to the project folder.
2.  Install the required dependencies:

    ```bash
    pip install pandas requests sec-edgar-downloader thefuzz tqdm
    ```

## Usage

The script is run via the command line.

### Basic Usage

Download filings for the top 10 companies in `list.csv` (defaults to the latest filing):

```bash
python scraper.py --limit 10
```

### Dry Run (Verify Ticker Resolution)

Check which companies will be processed and their resolved tickers without downloading anything:

```bash
python scraper.py --limit 10 --dry-run
```

### Download by Specific Year

Download all 10-K and 10-Q filings for the year 2024:

```bash
python scraper.py --limit 50 --year 2024
```

### Download for the Last N Years

Download filings for the last 5 years (including the current year) for all 500 companies:

```bash
python scraper.py --limit 500 --last-n-years 5
```

## Output Structure

Filings are saved in the `sec-edgar-filings` directory, organized by Ticker, Filing Type, and Accession Number:

```
sec-edgar-filings/
├── WMT/
│   ├── 10-K/
│   │   └── 0000104169-24-000056/
│   │       └── full-submission.txt
│   └── 10-Q/
│       └── ...
├── AAPL/
│   └── ...
└── ...
```

## Configuration

- **User Agent**: The script uses a default User-Agent (`AntigravityBot generic@antigravity.com`) to comply with SEC requirements. You can modify this in `scraper.py` if needed.
- **Rate Limiting**: The download library automatically limits requests to 10 per second to stay within SEC guidelines.
