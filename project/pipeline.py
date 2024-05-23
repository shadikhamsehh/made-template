import requests
import pandas as pd
from sqlalchemy import create_engine
import zipfile
import os

# Download a ZIP file
def download_and_extract(url, extract_to):
    try:
        response = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        })
        response.raise_for_status()

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

# Download a CSV file
def download_csv(url, save_to):
    try:
        response = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        })
        response.raise_for_status()  

        csv_path = os.path.join(save_to, os.path.basename(url))
        with open(csv_path, 'wb') as outFile:
            outFile.write(response.content)
        print(f"Downloaded CSV to {csv_path}")
        return csv_path

    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

# Combine separate date and time columns into a single datetime column
def combine_datetime(df):
    df['datetime'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']])
    return df.drop(['year', 'month', 'day', 'hour'], axis=1)

# Normalize specified columns
def normalize_data(df, columns):
    for column in columns:
        df[column] = (df[column] - df[column].mean()) / df[column].std()
    return df

def process_csv_data(file_path, dataset_type):
    try:
        df = pd.read_csv(file_path)
        
        # Handle missing data for Air Quality dataset
        if dataset_type == "AirQuality":
            df.replace("NA", pd.NA, inplace=True)
            df.dropna(inplace=True)
            
            # Combine date and time columns into a single datetime column
            if 'year' in df.columns:
                df = combine_datetime(df)
        
        # Handle missing data for Inorganic Gases dataset
        elif dataset_type == "InorganicGases":
            df.dropna(inplace=True)
        
        # General normalization for both datasets
        df = normalize_data(df, df.select_dtypes(include=['float64', 'int64']).columns)
        
        print(f"Processed data from {file_path}")
        return df
    except Exception as e:
        print(f"Failed to process data from {file_path}: {e}")
        return None

# Save DataFrame to an SQLite database table
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

  
    zip_urls = [
        'https://archive.ics.uci.edu/static/public/501/beijing+multi+site+air+quality+data.zip'
    ]

    csv_url = 'https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/InorganicGases/Ver2017-01-01/InorganicGases_2017.csv'

    for url in zip_urls:
        download_and_extract(url, data_directory)
    
    # Download the direct CSV file
    csv_file_path = download_csv(csv_url, data_directory)

    # Process all CSV files in the directory and subdirectories
    for root, dirs, files in os.walk(data_directory):
        for file_name in files:
            if file_name.endswith('.csv'):
                file_path = os.path.join(root, file_name)
                if "InorganicGases" in file_name:
                    df = process_csv_data(file_path, "InorganicGases")
                    save_to_sqlite(df, "InorganicGases", os.path.splitext(file_name)[0])
                else:
                    df = process_csv_data(file_path, "AirQuality")
                    save_to_sqlite(df, "AirQuality", os.path.splitext(file_name)[0])

    print("Data processing complete. All datasets are stored in the respective SQLite databases.")

if __name__ == '__main__':
    main()
