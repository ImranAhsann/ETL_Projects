# Import required libraries
from io import StringIO
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime
import os

# Function to log progress messages with timestamps
def log_progress(message):
    """This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing"""
    
    # Check if the 'logs' directory exists, create it if not
    log_dir = './logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Open the log file in append mode and write the message with timestamp
    with open(os.path.join(log_dir, 'code_log.txt'), 'a') as f:
        f.write(f'{datetime.now()}: {message}\n')

# Function to extract data from the webpage
def extract(url, table_attribs):
    """ This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. """
    
    try:
        # Send HTTP request to the URL and check for successful response
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad responses
        
        # Parse the page content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Locate the table based on the given attribute ('By market capitalization') and extract it
        table = soup.find('span', string=table_attribs).find_next('table')
        
        # Convert the HTML table to a pandas DataFrame
        df = pd.read_html(StringIO(str(table)))[0]
        
        # Log progress of data extraction
        log_progress('Data extraction complete. Initiating Transformation process')
        
        # Return the DataFrame for further processing
        return df
    except requests.exceptions.RequestException as e:
        # If there's an issue with the request, log the error
        log_progress(f'Error during web scraping: {e}')
        raise  # Re-raise the exception after logging it
    except Exception as e:
        # If any other error occurs, log it
        log_progress(f'Error in extract function: {e}')
        raise

# Function to transform the extracted data using exchange rates
def transform(df, csv_path):
    """ This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of the Market Cap column to
    respective currencies """
    
    try:
        # Read the exchange rates CSV and convert it into a dictionary
        exchange_rate = pd.read_csv(csv_path, index_col=0).to_dict()['Rate']
        
        # Convert 'Market cap (US$ billion)' into GBP, EUR, and INR using the exchange rates
        df['MC_GBP_Billion'] = round(df['Market cap (US$ billion)'] * exchange_rate['GBP'], 2)
        df['MC_EUR_Billion'] = round(df['Market cap (US$ billion)'] * exchange_rate['EUR'], 2)
        df['MC_INR_Billion'] = round(df['Market cap (US$ billion)'] * exchange_rate['INR'], 2)

        # Print a specific value for debugging purposes (5th row, index 4)
        print(df['MC_EUR_Billion'][4])

        # Log the completion of the transformation process
        log_progress('Data transformation complete. Initiating Loading process')

        # Return the transformed DataFrame
        return df
    except Exception as e:
        # If any error occurs during transformation, log it
        log_progress(f'Error in transform function: {e}')
        raise

# Function to save the transformed data to a CSV file
def load_to_csv(df, output_path):
    """ This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing."""
    
    try:
        # Save the DataFrame to CSV without including the index column
        df.to_csv(output_path, index=False)
        
        # Log that the data has been saved successfully
        log_progress('Data saved to CSV file')
    except Exception as e:
        # If there's an error saving to CSV, log it
        log_progress(f'Error saving data to CSV: {e}')
        raise

# Function to load the transformed data into an SQLite database
def load_to_db(df, sql_connection, table_name):
    """ This function saves the final data frame to a database
    table with the provided name. Function returns nothing."""
    
    try:
        # Save the DataFrame to the specified database table, replacing it if it exists
        df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
        
        # Log that the data has been successfully loaded into the database
        log_progress('Data loaded to Database as a table, Executing queries')
    except Exception as e:
        # If there's an error loading to the database, log it
        log_progress(f'Error loading data to database: {e}')
        raise

# Function to run SQL queries on the database
def run_query(query_statement, sql_connection):
    """ This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. """
    
    try:
        # Create a cursor object to interact with the database
        cursor = sql_connection.cursor()
        
        # Execute the provided SQL query
        cursor.execute(query_statement)
        
        # Fetch all results of the query
        result = cursor.fetchall()
        
        # Log that the query execution is complete
        log_progress('Process Complete')
        
        # Return the result of the query
        return result
    except Exception as e:
        # If there's an error running the query, log it
        log_progress(f'Error running query: {e}')
        raise

# Main execution block
if __name__ == '__main__':
    # Define URL, output paths, and database connection details
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    output_csv_path = './output/Largest_banks_data.csv'
    database_name = './output/Banks.db'
    table_name = 'Banks'
    
    # Log the start of the ETL process
    log_progress('Preliminaries complete. Initiating ETL process')
    
    try:
        # Extract data from the web
        df = extract(url, 'By market capitalization')
        
        # Print the first few rows of the extracted data
        print(df.head())
        
        # Transform the data using exchange rates
        df = transform(df, './input/exchange_rate.csv')
        
        # Load the transformed data to a CSV file
        load_to_csv(df, output_csv_path)
        
        # Connect to the SQLite database and load data into it
        with sqlite3.connect(database_name) as conn:
            # Load data into the database
            load_to_db(df, conn, table_name)
            
            # Run SQL queries to fetch results from the database and print them
            print(run_query('SELECT * FROM Banks', conn))
            print(run_query('SELECT AVG(MC_GBP_Billion) FROM Banks', conn))
            print(run_query('SELECT "Bank name" FROM Banks LIMIT 5', conn))

    except Exception as e:
        # If there's an error in the ETL process, log it and print the message
        log_progress(f'Error in ETL process: {e}')
        print(f"Error during the process: {e}")
