# Fortune 500 SEC Filings Pipeline (Custom Edition)

A high-performance, custom-built Python scraper to download 10-K and 10-Q filings for Fortune 500 companies from the SEC EDGAR database, combined with a robust workflow that transforms this massive unstructured text directly into a queried Property Graph in Google BigQuery utilizing Vertex AI (Gemini 3.1Pro) for dynamic entity extraction. 


## Features

- **Custom Python Backing**: Scrapes SEC "Classic Browse" directly with `asyncio`.
- **Top Tier Performance**: Concurrent downloading, handling resolution and strictly compliant with SEC limiting protocols. 
- **Flexible Extractor Configurations**: Parse exact years, CIK, Tickers, and automatically skip files via checkpointing.
- **AI Powered Synthesis**: Extracts exact insights (Markets, Risks, Competiments) organically into JSON structures using `ML.GENERATE_TEXT`.
- **Intelligent Graph Creation**: Seamlessly takes extraction tables into Node/Edge graphs to visualize the data immediately in BigQuery.

## Usage

### Recommended Method: Colab Notebook

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/Kineviz/fortune500/blob/main/pipeline.ipynb)

The absolute easiest way to execute the **entire** end-to-end pipeline is by launching the **`pipeline.ipynb`** notebook! We strongly suggest running this inside Google Colab using the button above.

### 💻 Advanced Method: Specific Command Line Scripts

If you want to run the pipeline sequentially (e.g., executing the SEC scraper script, extraction algorithms, or BQ property SQL setups individually), we detailed these advanced individual configurations in the accompanying guide below. 

👉 **[Read the Manual Scripts Setup Guide](SCRIPTS.md)**


## Visualizing with GraphXR

Once your property graph is configured natively inside BigQuery, connect to the dataset with GraphXR using the following sequence:

1. **Create Project**

   ![Create Project](images/01_create_project.png)

2. **Select Name & Database Type (BigQuery)**
   
   ![Select Name and Type](images/02_selct%20_name_type_bigquey.png)

3. **Upload Account Key**
   
   ![Upload Key](images/03_upload_account_key.png)

4. **Select Database**
   
   ![Select DB](images/04_select_db.png)

5. **Select Region**
   
   ![Select Region](images/05_select_region.png)

6. **Select Graph**
   
   ![Select Graph](images/06_select_graph.png)

## License
[MIT](https://choosealicense.com/licenses/mit/)