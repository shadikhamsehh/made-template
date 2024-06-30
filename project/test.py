import os
import unittest
import logging
import pandas as pd
import sqlite3
from pipeline import main

logging.basicConfig(level=logging.DEBUG, filename='test_pipeline.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')


class DataPipelineTests(unittest.TestCase):
    def setUp(self):
        self.test_directory = os.path.abspath('./test_environment')
        os.makedirs(self.test_directory, exist_ok=True)
        base_dir = os.path.abspath(os.path.join(__file__, "../.."))  # Adjust relative navigation based on actual project structure
        self.db_air_quality = os.path.join(base_dir, 'data', 'BeijingAirQuality.db')
        self.db_inorganic_gases = os.path.join(base_dir, 'data', 'InorganicGases.db')

    def tearDown(self):
        import shutil
        if os.path.exists(self.test_directory):
            shutil.rmtree(self.test_directory)

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




# import os
# import unittest
# import logging
# import pandas as pd
# import sqlite3
# from pipeline import download_and_extract, download_csv, process_csv_data, save_to_sqlite, main

# logging.basicConfig(level=logging.DEBUG, filename='test_pipeline.log', filemode='w',
#                     format='%(name)s - %(levelname)s - %(message)s')

# class DataPipelineTests(unittest.TestCase):
#     def setUp(self):
#         self.test_directory = './test_environment'
#         os.makedirs(self.test_directory, exist_ok=True)
#         self.db_air_quality = os.path.join('../data', 'AirQuality.db')
#         self.db_inorganic_gases = os.path.join('../data', 'InorganicGases.db')

#     def tearDown(self):
#         import shutil
#         if os.path.exists(self.test_directory):
#             shutil.rmtree(self.test_directory)

#     def test_full_pipeline_execution(self):
#         main()  

#         # Check if the AirQuality database is created
#         self.check_database_exists(self.db_air_quality, 'AirQuality database')

#         # Check if the InorganicGases database is created
#         self.check_database_exists(self.db_inorganic_gases, 'InorganicGases database')

#         # Verify the structure of tables in the AirQuality database
#         self.verify_table_structure(self.db_air_quality, 'test', 
#                                     ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
#         self.verify_table_structure(self.db_air_quality, 'data', 
#                                     ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

#         # Verify the structure of tables in the InorganicGases database
#         self.verify_table_structure(self.db_inorganic_gases, 'InorganicGases_2017', 
#                                     ['DateTime', 'NO', 'NO2', 'NOx', 'SO2', 'O3', 'CO_ppm'])

#     def check_database_exists(self, db_path, db_name):
#         logging.info(f"Checking existence of {db_path} for {db_name}")
#         if not os.path.exists(db_path):
#             logging.error(f"Database file not found: {db_path}")
#         self.assertTrue(os.path.exists(db_path), f"{db_name} not found at {db_path}")

#     def verify_table_structure(self, db_path, table_name, expected_columns):
#         with sqlite3.connect(db_path) as conn:
#             cursor = conn.cursor()
#             cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
#             table_exists = cursor.fetchone()
#             if not table_exists:
#                 logging.error(f"Table {table_name} does not exist in {db_path}")
#             self.assertIsNotNone(table_exists, f"Table {table_name} should exist in the database.")

#             cursor.execute(f"PRAGMA table_info({table_name});")
#             columns = [row[1] for row in cursor.fetchall()]
#             logging.info(f"Columns in {table_name}: {columns}")
#             for column in expected_columns:
#                 self.assertIn(column, columns, f"{column} is expected in {table_name} but not found.")
#             logging.info(f"All expected columns are present in {table_name}.")

# if __name__ == '__main__':
#     unittest.main()


