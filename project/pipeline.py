import requests
import pandas as pd
from sqlalchemy import create_engine
import zipfile
import os

#Download a ZIP file and extract its contents to the data folder.
def download_and_extract(url, extract_to):
 
    try:
        response = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        })
        response.raise_for_status()  # Check if the request was successful


        # Determine file type from the headers or URL
        if 'application/zip' in response.headers.get('Content-Type', '') or url.endswith('.zip'):
            zip_path = os.path.join(extract_to, os.path.basename(url))
            with open(zip_path, 'wb') as outFile:
                outFile.write(response.content)

            # Extract the ZIP file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
                print(f"Extracted to {extract_to}")

            # Remove the ZIP file after extraction
            os.remove(zip_path)
        else:
            print("The file downloaded is not a zip file.")
            
    except requests.RequestException as e:
        print(f"Request failed: {e}")
    except zipfile.BadZipFile:
        print("The downloaded file is not a zip file or it is corrupted.")


#Load and clean CSV data
def process_csv_data(file_path):
   
    try:
        df = pd.read_csv(file_path, parse_dates=True) 
        # Clean data by dropping rows with missing values or fill them as needed
        df.dropna(inplace=True)  

        # Normalize or standardize numerical data; skip date columns
        for column in df.select_dtypes(include=['float64', 'int64']).columns:
            df[column] = (df[column] - df[column].mean()) / df[column].std()

        print(f"Processed data from {file_path}")
        return df
    except Exception as e:
        print(f"Failed to process data from {file_path}: {e}")
        return None


#Save DataFrame to an SQLite database table
def save_to_sqlite(df, db_name, table_name):
    if df is not None:
        try:
            database_dir = '../data'
            os.makedirs(database_dir, exist_ok=True)
            db_path = os.path.join(database_dir, f"{db_name}.db")
            engine = create_engine(f'sqlite:///{db_path}')
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"Saved data to SQLite table {table_name} in {db_path}")
        except Exception as e:
            print(f"Failed to save data to SQLite: {e}")
    else:
        print("No data to save to database.")

def main():
    data_directory = '../data'
    os.makedirs(data_directory, exist_ok=True)

    # List of URLs for ZIP data
    zip_urls = [
        'https://archive.ics.uci.edu/static/public/882/large-scale+wave+energy+farm.zip',
        'https://archive.ics.uci.edu/static/public/501/beijing+multi+site+air+quality+data.zip'
    ]

    for url in zip_urls:
        download_and_extract(url, data_directory)
    
    # Process all CSV files in the directory
    for file_name in os.listdir(data_directory):
        if file_name.endswith('.csv'):
            file_path = os.path.join(data_directory, file_name)
            df = process_csv_data(file_path)
            table_name = os.path.splitext(file_name)[0]
            save_to_sqlite(df, "climate_data", table_name)

    print("Data processing complete. All datasets are stored in the SQLite database.")

if __name__ == '__main__':
    main()
