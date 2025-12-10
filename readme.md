# World's Largest Banks ETL Project

This project implements a complete ETL (Extract, Transform, Load) pipeline in Python to process data about the world's largest banks.

## Overview

The script `banks_project.py` performs the following operations:

1.  **Extract**: Scrapes the "List of largest banks" from a specific Wikipedia archive URL. It identifies the table "By market capitalization" and extracts Bank Name and Market Capitalization (USD Billion).
2.  **Transform**: Converts the Market Capitalization from USD to GBP (British Pound), EUR (Euro), and INR (Indian Rupee) using live/static exchange rate data.
3.  **Load**:
    *   Saves the processed data to a CSV file (`Largest_banks_data.csv`).
    *   Loads the data into a SQLite database (`Banks.db`) table named `Largest_banks`.
4.  **Logging**: detailed progress logging to `code_log.txt` with timestamps.
5.  **Querying**: Executes sample SQL queries on the database to verify the load.

## Project Structure

*   `banks_project.py`: Main Python script containing the ETL logic.
*   `exchange_rate.csv`: Source file for currency exchange rates (downloaded automatically if missing).
*   `Largest_banks_data.csv`: Output CSV file containing the transformed data.
*   `Banks.db`: Output SQLite database file.
*   `code_log.txt`: Log file tracking the execution progress.

## Dependencies

*   Python 3.x
*   `requests`
*   `beautifulsoup4`
*   `pandas`
*   `numpy`
*   `sqlite3` (Built-in with Python)

## Setup & Installation

1.  **Clone the repository** (if applicable) or download the files.

2.  **Set up a Virtual Environment** (Recommended):
    ```bash
    # Windows
    python -m venv .venv
    .venv\Scripts\activate

    # Linux/Mac
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install Requirements**:
    ```bash
    pip install requests pandas beautifulsoup4 numpy
    ```

## Usage

Run the script from your terminal:

```bash
python banks_project.py
```

## Logging

Check `code_log.txt` for execution details. Example output:
```text
2023-11-15-10-30-01: Preliminaries complete. Initiating ETL process
2023-11-15-10-30-05: Data extraction complete. Initiating Transformation process
...
```

## Data Source

*   **Banks Data**: [Wikipedia - List of largest banks (Archived)](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks)
*   **Exchange Rates**: [IBM Skills Network](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv)