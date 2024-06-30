import os
import unittest
import logging
import sqlite3
import requests
from zipfile import ZipFile
from pipeline import main

logging.basicConfig(level=logging.DEBUG, filename='test_pipeline.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')

class DataPipelineTests(unittest.TestCase):
    def setUp(self):
        self.test_directory = './test_environment'
        os.makedirs(self.test_directory, exist_ok=True)
        base_dir = os.getenv('GITHUB_WORKSPACE', os.path.abspath(os.path.join(__file__, "../..")))
        self.data_directory = os.path.join(base_dir, 'data')
        os.makedirs(self.data_directory, exist_ok=True)
        self.db_air_quality = os.path.join(self.data_directory, 'BeijingAirQuality.db')
        self.db_inorganic_gases = os.path.join(self.data_directory, 'InorganicGases.db')
        print(f"Expected path for BeijingAirQuality.db: {self.db_air_quality}")
        print(f"Expected path for InorganicGases.db: {self.db_inorganic_gases}")

        # Ensure the PRSA_Data_20130301-20170228.zip file is downloaded and unzipped
        self.download_and_unzip_prsa_data()

    def tearDown(self):
        import shutil
        if os.path.exists(self.test_directory):
            shutil.rmtree(self.test_directory)

    def download_and_unzip_prsa_data(self):
        prsa_zip_url = 'https://archive.ics.uci.edu/static/public/501/beijing+multi+site+air+quality+data.zip'
        prsa_zip_path = os.path.join(self.data_directory, 'PRSA_Data_20130301-20170228.zip')
        prsa_data_directory = os.path.join(self.data_directory, 'PRSA_Data_20130301-20170228')

        if not os.path.exists(prsa_zip_path):
            response = requests.get(prsa_zip_url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            })
            with open(prsa_zip_path, 'wb') as outFile:
                outFile.write(response.content)
            print(f"Downloaded PRSA zip to {prsa_zip_path}")

        if not os.path.exists(prsa_data_directory):
            with ZipFile(prsa_zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.data_directory)
            print(f"Unzipped {prsa_zip_path} to {self.data_directory}")

            # Check if the expected directory exists, or if it is nested
            if not os.path.exists(prsa_data_directory):
                # Sometimes the extracted files might be nested inside another directory
                nested_dirs = os.listdir(self.data_directory)
                print(f"Nested directories after unzipping: {nested_dirs}")
                for dir_name in nested_dirs:
                    if os.path.isdir(os.path.join(self.data_directory, dir_name)):
                        prsa_data_directory = os.path.join(self.data_directory, dir_name)
                        break
            print(f"PRSA data directory set to: {prsa_data_directory}")

            # List contents of the extracted directory
            extracted_files = os.listdir(prsa_data_directory)
            print(f"Contents of {prsa_data_directory}: {extracted_files}")

        else:
            print(f"PRSA data directory already exists at {prsa_data_directory}")

        # Verify the expected files exist
        expected_files = [
            'PRSA_Data_Aotizhongxin_20130301-20170228.csv',
            'PRSA_Data_Changping_20130301-20170228.csv',
            'PRSA_Data_Dingling_20130301-20170228.csv',
            'PRSA_Data_Dongsi_20130301-20170228.csv',
            'PRSA_Data_Gucheng_20130301-20170228.csv',
            'PRSA_Data_Guanyuan_20130301-20170228.csv',
            'PRSA_Data_Huairou_20130301-20170228.csv',
            'PRSA_Data_Nongzhanguan_20130301-20170228.csv',
            'PRSA_Data_Shunyi_20130301-20170228.csv',
            'PRSA_Data_Tiantan_20130301-20170228.csv',
            'PRSA_Data_Wanliu_20130301-20170228.csv',
            'PRSA_Data_Wanshouxigong_20130301-20170228.csv'
        ]
        for file_name in expected_files:
            file_path = os.path.join(prsa_data_directory, file_name)
            self.assertTrue(os.path.exists(file_path), f"Expected file {file_path} does not exist.")

    def test_full_pipeline_execution(self):
        main()

        # Check if the BeijingAirQuality database is created
        self.check_database_exists(self.db_air_quality, 'BeijingAirQuality database')

        # Check if the InorganicGases database is created
        self.check_database_exists(self.db_inorganic_gases, 'InorganicGases database')

        # Verify the structure of tables in the BeijingAirQuality database
        self.verify_table_structure(self.db_air_quality, 'combined_data', 
                                    ['PM2.5', 'PM10', 'SO2', 'NO2', 'CO', 'O3', 'TEMP', 'PRES', 'DEWP', 'RAIN', 'wd', 'WSPM', 'station', 'datetime'])

        # Verify the structure of tables in the InorganicGases database
        self.verify_table_structure(self.db_inorganic_gases, 'data_2017', 
                                    ['DateTime', 'NO', 'NO2', 'NOx', 'SO2', 'O3', 'CO_ppm'])

    def check_database_exists(self, db_path, db_name):
        logging.info(f"Checking existence of {db_path} for {db_name}")
        if not os.path.exists(db_path):
            logging.error(f"Database file not found: {db_path}")
        self.assertTrue(os.path.exists(db_path), f"{db_name} not found at {db_path}")

    def verify_table_structure(self, db_path, table_name, expected_columns):
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            table_exists = cursor.fetchone()
            if not table_exists:
                logging.error(f"Table {table_name} does not exist in {db_path}")
            self.assertIsNotNone(table_exists, f"Table {table_name} should exist in the database.")

            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = [row[1] for row in cursor.fetchall()]
            logging.info(f"Columns in {table_name}: {columns}")
            for column in expected_columns:
                self.assertIn(column, columns, f"{column} is expected in {table_name} but not found.")
            logging.info(f"All expected columns are present in {table_name}.")

if __name__ == '__main__':
    unittest.main()
