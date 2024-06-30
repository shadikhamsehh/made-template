import pandas as pd
from sqlalchemy import create_engine
import os
import requests

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

def normalize_data(df):
    columns = df.select_dtypes(include=['float64', 'int64']).columns
    df[columns] = (df[columns] - df[columns].mean()) / df[columns].std()
    return df

def process_inorganic_gases(file_path):
    try:
        df = pd.read_csv(file_path)
        df.dropna(inplace=True)  # Drop missing values
        df = normalize_data(df)  # Normalize data
        print(f"Processed data from {file_path}")
        return df
    except Exception as e:
        print(f"Failed to process data from {file_path}: {e}")
        return None

def combine_and_process_csv_files(files):
    combined_df = pd.DataFrame()
    for file_path in files:
        df = pd.read_csv(file_path)
        df.replace("NA", pd.NA, inplace=True)
        df.dropna(inplace=True)
        if 'year' in df.columns:
            df['datetime'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']])
            df = df.drop(['year', 'month', 'day', 'hour'], axis=1)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    return combined_df

def save_to_sqlite(df, db_name, table_name, directory):
    if df is not None:
        try:
            os.makedirs(directory, exist_ok=True)
            db_path = os.path.join(directory, f"{db_name}.db")
            engine = create_engine(f'sqlite:///{db_path}')
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"Saved data to SQLite database '{db_name}' in table '{table_name}'.")
        except Exception as e:
            print(f"Failed to save data to SQLite: {e}")
    else:
        print("No data to save to database.")

def main():
    data_directory = '../data'
    os.makedirs(data_directory, exist_ok=True)

    # Process Beijing Air Quality Data
    beijing_data_directory = os.path.join(data_directory, 'PRSA_Data_20130301-20170228')
    all_beijing_csv_files = [os.path.join(root, file) for root, dirs, files in os.walk(beijing_data_directory) for file in files if file.endswith('.csv')]
    beijing_df = combine_and_process_csv_files(all_beijing_csv_files)
    if not beijing_df.empty:
        normalized_beijing_df = normalize_data(beijing_df)
        save_to_sqlite(normalized_beijing_df, 'BeijingAirQuality', 'combined_data', data_directory)

    # Process Inorganic Gases Data
    inorganic_gases_csv_url = 'https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/InorganicGases/Ver2017-01-01/InorganicGases_2017.csv'
    inorganic_gases_file_path = download_csv(inorganic_gases_csv_url, data_directory)
    if inorganic_gases_file_path:
        inorganic_gases_df = process_inorganic_gases(inorganic_gases_file_path)
        if inorganic_gases_df is not None:
            save_to_sqlite(inorganic_gases_df, 'InorganicGases', 'data_2017', data_directory)

if __name__ == '__main__':
    main()

