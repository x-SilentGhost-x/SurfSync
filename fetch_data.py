# *********************************************************************************************************
# Program Name: Fetch Data - NOOA API Call
# Creation Date: 2024-10-25
# Function: Fetch and retrive real-time NOOA Buoy Data for San Diego
# *********************************************************************************************************

import requests
import os
from dotenv import load_dotenv
load_dotenv()
# sys.path.append(os.path.abspath(os.getenv("script_location")))

# NOOA Station IDs - SD Weather INFO
# LJAC1 - La Jolla 

# NOOA Buoy station IDs - SD Swell INFO
# 46235 - Imperial Beach 
# 46232 - Point Loma, South
# 46258 - Mission Bay, West
# 46086 - San Clemente Basin
# 46254 - Scripps, Nearshore
# 46273 - Torrey Pines, Inner
# 46225 - Torrey Pines, Outer
# 46266 - Del Mar, Nearshore
# 46274 - Leucaida, Nearshore
# 46242 - Camp Pendleton, Nearshore
# 46224 - Oceanside, Offshore
# 46275 - Redbeach, Nearshore
# 46277 - Greenbeach, Offshore

# To do:
# execute this on a chron job? 

class FetchData:
    def __init__(self):
        # Access save folder directory
        self.save_directory = os.getenv('save_directory')
        
        # List of buoy IDs + weather station
        self.san_diego_NOAA_buoy_list = [
            '46235', '46232', '46258', '46086', '46254', '46273',
            '46225', '46266', '46274', '46242', '46224', '46275',
            '46277', 'LJAC1'
        ]
    
    #Fetches buoy data from NOAA and saves it to a specified directory
    def fetch_and_save_buoy_data(self, buoy_id, save_directory):
        noaa_url = f'https://www.ndbc.noaa.gov/data/realtime2/{buoy_id}.txt'
        file_path = os.path.join(save_directory, f'{buoy_id}.txt')
        
        try:
            response = requests.get(noaa_url)
            response.raise_for_status()  # Raises an HTTPError if the status is 4xx or 5xx
            data = response.text
            
            with open(file_path, 'w') as file:
                file.write(data)
            print(f'Data for buoy {buoy_id} saved successfully at {file_path}.')
        except requests.exceptions.HTTPError as http_err:
            print(f'HTTP error for buoy {buoy_id}: {http_err}')
        except Exception as err:
            print(f'Failed to save data for buoy {buoy_id}: {err}')
    
    # Compile functions
    def run(self, save_directory=None):
        #Fetch and save data for each buoy
        # Use the default save_directory from the instance if not provided
        if save_directory is None:
            save_directory = self.save_directory

        # Ensure save_directory is set
        if not save_directory:
            raise ValueError("The save_directory is not set. Please provide a valid directory path.")
        
        # Create the directory if it doesn't exist
        os.makedirs(save_directory, exist_ok=True)

        # Fetch and save data for each buoy
        for buoy_id in self.san_diego_NOAA_buoy_list:
            self.fetch_and_save_buoy_data(buoy_id, save_directory)

# This block ensures the script only runs when executed directly, not when imported
if __name__ == "__main__":
    # Initialize the class and run with the provided or default save directory
    surf_sync = FetchData()
    surf_sync.run()
