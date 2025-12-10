import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
import os

# Known Entities
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
csv_path = "./exchange_rate.csv"
output_csv_path = "./Largest_banks_data.csv"
database_name = "Banks.db"
table_name = "Largest_banks"
log_file = "code_log.txt"
table_attribs = ["Name", "MC_USD_Billion"]

def log_progress(message):
    ''' Logs the progress of the code to code_log.txt '''
    timestamp_format = '%Y-%m-%d-%H-%M-%S' # Year-Month-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp}: {message}\n")

def extract(url, table_attribs):
    ''' Scrape the website to create a DataFrame '''
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    
    df = pd.DataFrame(columns=table_attribs)
    
    # Locate the table under the heading "By market capitalization"
    # Note: Wikipedia tables often share class names, looking for specific context or index is robust.
    # The requirement says "Locate the table under the heading...". 
    # Usually this is the first table or can be found by looking for the heading span.
    
    tables = soup.find_all('tbody')
    # Inspecting typical wiki structure: The required table is usually the first one matching the structure 
    # or finding the heading 'By market capitalization' previous sibling.
    # Let's try to find tables and check headers.
    
    # A more robust way given the URL is known:
    # The table is the first wikitable sortable under the specific section.
    # However, simply iterating rows of all tables to find match is safer if index changes.
    
    rows = []
    
    # Using a strategy to find the specific table index or content. 
    # Based on the URL, there are multiple tables. 
    # The "By market capitalization" section usually contains the table we want.
    # Let's find all tables and pick the first one that looks right (Rank, Bank name, Market cap).
    
    found_table = None
    all_tables = soup.find_all('table', attrs={"class": "wikitable"})
    
    for table in all_tables:
        # Check if "Market cap" is in the header
        if "Market cap" in str(table) and "US$" in str(table):
            found_table = table
            # Based on the specific wiki page archive, it is likely the first table under that section.
            # But let's verify if there are multiple.
            # The prompt implies a specific one. I will assume the first matching one.
            break
            
    if not found_table:
        # Fallback or error, but let's assume it works or try simpler index 0 if valid.
        # Often the first wikitable is the one.
        found_table = all_tables[0]

    for row in found_table.find_all('tr'):
        col = row.find_all('td')
        if len(col) != 0:
            # The structure usually is: Rank | Bank Name | Market Cap
            # Sometimes Bank Name is in the second column (index 1) and Market Cap in third (index 2).
            # Let's check the provided attributes. attribs = ["Name", "MC_USD_Billion"]
            
            # The Bank Name might be a link <a> text.
            # Market Cap might have '\n'
            
            try:
                # Name is usually the second column (index 1)
                # Market Cap is usually the third column (index 2)
                # Note: The prompt says "Market Cap" column contents.
                
                # Check column indices for specific wiki page structure:
                # Rank | Name | Market Cap (US$ billion)
                
                bank_name = col[1].find_all('a')[1]['title'] if len(col[1].find_all('a')) > 1 else col[1].text.strip()
                # Simplified extraction: usually text is enough, but sometimes links have the full name.
                # Let's go with text first, cleaning it.
                 
                bank_name = col[1].text.strip()
                market_cap = col[2].text.strip()
                
                # Critical Cleaning: Remove the last character (specifically \n) - wait, strip() handles \n.
                # But instruction says "Remove the last character (specifically \n)".
                # If using text property, it might not have \n at end if we don't strip.
                # Let's access the text raw or just strip. 
                # "Remove the last character (specifically \n)" implies it might be there.
                # I will treat the specific instruction carefully:
                # If I use `col[2].contents[0]`, it might have \n. 
                # Let's use string manipulation as requested.
                
                # Re-reading: "Remove the last character (specifically \n) from the 'Market Cap' column contents"
                # This implies I should treat it as a string that definitely has it or check.
                # Wikipedia cells often end with \n in the raw text.
                
                # Let's follow strictly:
                # val = val[:-1] if val.endswith('\n') else val
                
                # Actually, reading standard text from `td` usually doesn't include the trailing newline of the html unless using `.get_text()`.
                # I will use .strip() as it is safer and covers \n.
                # But to be pedantic to the requirement: 
                # "Remove the last character (specifically \n) ... and typecast ... to float"
                
                mc_val = str(col[2].text)
                if mc_val.endswith('\n'):
                   mc_val = mc_val[:-1]
                mc_val = mc_val.strip() # Remove other spaces just in case
                
                data_dict = {"Name": bank_name, "MC_USD_Billion": float(mc_val)}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
            except Exception as e:
                # Skip rows that don't match or errors
                continue

    return df

def transform(df, csv_path):
    ''' Add converted currency columns based on exchange rates '''
    exchange_rate = pd.read_csv(csv_path)
    # Convert to dictionary: 1st column key, 2nd value
    exchange_rate_dict = exchange_rate.set_index(exchange_rate.columns[0]).to_dict()[exchange_rate.columns[1]]
    
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate_dict['INR'], 2) for x in df['MC_USD_Billion']]
    
    return df

def load_to_csv(df, output_path):
    ''' Save the final DataFrame to a CSV file '''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' Save the final DataFrame to the SQLite database '''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' execute a SQL query and print the output '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

if __name__ == "__main__":
    # Preliminaries
    # Check if exchange_rate.csv exists, if not download it
    if not os.path.exists(csv_path):
        resp = requests.get(exchange_rate_url)
        with open(csv_path, 'wb') as f:
            f.write(resp.content)
            
    log_progress("Preliminaries complete. Initiating ETL process")
    
    # Extract
    df = extract(url, table_attribs)
    log_progress("Data extraction complete. Initiating Transformation process")
    
    # Transform
    df = transform(df, csv_path)
    log_progress("Data transformation complete. Initiating Loading process")
    
    # Load to CSV
    load_to_csv(df, output_csv_path)
    log_progress("Data saved to CSV file")
    
    # Database Operations
    sql_connection = sqlite3.connect(database_name)
    log_progress("SQL Connection initiated")
    
    load_to_db(df, sql_connection, table_name)
    log_progress("Data loaded to Database as a table, Executing queries")
    
    # Run Queries
    run_query(f"SELECT * FROM {table_name}", sql_connection)
    run_query(f"SELECT AVG(MC_GBP_Billion) FROM {table_name}", sql_connection)
    run_query(f"SELECT Name from {table_name} LIMIT 5", sql_connection)
    
    log_progress("Process Complete")
    
    # Cleanup
    sql_connection.close()
    log_progress("Server Connection closed")
