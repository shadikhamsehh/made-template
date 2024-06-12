import os
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sqlite3
from pipeline import download_and_extract, download_csv, process_csv_data, save_to_sqlite, main

class TestDataPipelineSystem(unittest.TestCase):

    def setUp(self):
        self.test_dir = './test_data'
        os.makedirs(self.test_dir, exist_ok=True)
        self.air_quality_db_path = os.path.join('../data', 'AirQuality.db')
        self.inorganic_gases_db_path = os.path.join('../data', 'InorganicGases.db')

    def tearDown(self):
        # Clean up the test directory but do not delete the databases
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    @patch('requests.get')
    @patch('zipfile.ZipFile')
    def test_download_and_extract(self, mock_zipfile, mock_get):
        # Mock the requests.get response for a ZIP file
        mock_response = MagicMock()
        mock_response.headers = {'Content-Type': 'application/zip'}
        mock_response.content = b'mock_zip_content'
        mock_get.return_value = mock_response

        # Mock the ZipFile to simulate extraction
        mock_zip = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip

        # Call the download_and_extract function
        download_and_extract('https://example.com/mock.zip', self.test_dir)

        # Check if the ZIP file was downloaded and extracted correctly
        mock_zip.extractall.assert_called_once_with(self.test_dir)

    @patch('requests.get')
    def test_download_csv(self, mock_get):
        # Mock the requests.get response for a CSV file
        mock_response = MagicMock()
        mock_response.content = b'mock_csv_content'
        mock_get.return_value = mock_response

        # Call the download_csv function
        csv_path = download_csv('https://example.com/mock.csv', self.test_dir)

        # Check if the CSV file was downloaded correctly
        self.assertTrue(os.path.exists(csv_path))

    def test_process_csv_data_air_quality(self):
        # Create a mock AirQuality CSV file
        mock_csv_path = os.path.join(self.test_dir, 'mock_air_quality.csv')
        mock_data = {'year': [2022, 2022], 'month': [1, 2], 'day': [1, 2], 'hour': [1, 2], 'value': [1.0, 2.0]}
        pd.DataFrame(mock_data).to_csv(mock_csv_path, index=False)

        # Process the mock CSV data
        df = process_csv_data(mock_csv_path, 'AirQuality')

        # Validate the processed data
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn('datetime', df.columns)
        self.assertNotIn('year', df.columns)
        self.assertNotIn('month', df.columns)
        self.assertNotIn('day', df.columns)
        self.assertNotIn('hour', df.columns)

    def test_process_csv_data_inorganic_gases(self):
        # Create a mock InorganicGases CSV file
        mock_csv_path = os.path.join(self.test_dir, 'mock_inorganic_gases.csv')
        mock_data = {'NO': [1.0, 2.0], 'NO2': [2.0, 3.0], 'SO2': [3.0, 4.0], 'O3': [4.0, 5.0]}
        pd.DataFrame(mock_data).to_csv(mock_csv_path, index=False)

        # Process the mock CSV data
        df = process_csv_data(mock_csv_path, 'InorganicGases')

        # Validate the processed data
        self.assertIsInstance(df, pd.DataFrame)

        # Check that the columns are normalized 
        for col in ['NO', 'NO2', 'SO2', 'O3']:
            self.assertAlmostEqual(df[col].mean(), 0, places=1)
            self.assertAlmostEqual(df[col].std(), 1, places=1)

    @patch('pandas.DataFrame.to_sql')
    def test_save_to_sqlite(self, mock_to_sql):
        # Create a mock DataFrame
        mock_data = {'value': [1.0, 2.0]}
        df = pd.DataFrame(mock_data)

        # Save the DataFrame to the SQLite database
        save_to_sqlite(df, 'mock_db', 'mock_table')

        # Check if the to_sql method was called with the correct parameters
        mock_to_sql.assert_called_once_with('mock_table', con=unittest.mock.ANY, if_exists='replace', index=False)

    def test_system_pipeline(self):
        main()

        # Check if the AirQuality SQLite database is created
        self.assertTrue(os.path.exists(self.air_quality_db_path), 
                        f"The SQLite database AirQuality.db should be created.")

        # Check if the InorganicGases SQLite database is created
        self.assertTrue(os.path.exists(self.inorganic_gases_db_path), 
                        f"The SQLite database InorganicGases.db should be created.")

        # Check if the expected tables are in the AirQuality database
        with sqlite3.connect(self.air_quality_db_path) as conn:
            tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
            expected_tables = ['test', 'data'] 
            for table in expected_tables:
                self.assertIn(table, tables['name'].values, f"Table {table} should exist in the AirQuality database.")

        # Check if the expected table is in the InorganicGases database
        with sqlite3.connect(self.inorganic_gases_db_path) as conn:
            tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
            self.assertIn('InorganicGases_2017', tables['name'].values, 
                          f"Table 'InorganicGases_2017' should exist in the InorganicGases database.")

        # Verify the structure of the 'test' table in the AirQuality database
        with sqlite3.connect(self.air_quality_db_path) as conn:
            if 'test' in tables['name'].values:
                test_info = pd.read_sql_query(f"PRAGMA table_info(test);", conn)
                expected_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                self.assertEqual(sorted(test_info['name'].tolist()), sorted(expected_columns), 
                                f"Columns in 'test' should match expected columns.")
        
        # Verify the structure of the 'data' table in the AirQuality database
        with sqlite3.connect(self.air_quality_db_path) as conn:
            if 'data' in tables['name'].values:
                data_info = pd.read_sql_query(f"PRAGMA table_info(data);", conn)
                expected_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                self.assertEqual(sorted(data_info['name'].tolist()), sorted(expected_columns), 
                                f"Columns in 'data' should match expected columns.")
        
        # Verify the structure of the InorganicGases_2017 table in the InorganicGases database
        with sqlite3.connect(self.inorganic_gases_db_path) as conn:
            inorganic_gases_info = pd.read_sql_query(f"PRAGMA table_info(InorganicGases_2017);", conn)
            expected_columns = ['DateTime', 'NO', 'NO2', 'NOx', 'SO2', 'O3', 'CO_ppm']
            self.assertEqual(sorted(inorganic_gases_info['name'].tolist()), sorted(expected_columns), 
                             f"Columns in 'InorganicGases_2017' should match expected columns.")

if __name__ == '__main__':
    unittest.main()


