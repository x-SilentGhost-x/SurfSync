# *********************************************************************************************************
# Program Name: Data Processor
# Creation Date: 2024-10-25
# Function: Process buoy data in .txt from NOOA and prepare it for data lake in PostgreSQL database
# *********************************************************************************************************

# Outline: 
# Load in the most recent .txt files
# Select the necesary columns/eliminate others (DONE)
# Convert dates pieces to date-time to be handled with DF (DONE)
# create rules to determine swell angle: N, E, S, W | NE, NW, SE, SW
# Convert units from metric --> imperial
# --
# delete duplicate data? --> easiest to do in postgres?
# Group into "buckets" (hour,day,week)? --> might be better in visualization
# Generate output depending on buoy location:  Scripps Buoy is good for surf spots in La Jolla - Torrey Pines, etc
# --
# Export to a PostgreSQL DBimport pandas as pd

from dotenv import load_dotenv
import sys
import pandas as pd
import os

# Load environment variables
load_dotenv()

# Get the buoy data folder path from environment variable
buoy_data_folder = os.getenv('buoy_data_folder')

# Initialize a dictionary to store DataFrames for each file
processed_dataframes = {}

# Iterate over each file in the directory
for file_index, filename in enumerate(os.listdir(buoy_data_folder)):
    file_path = os.path.join(buoy_data_folder, filename)
    
    # Make sure to only process files (skip directories)
    if os.path.isfile(file_path):
        try:
            # Read the file into a DataFrame
            df = pd.read_csv(file_path, delim_whitespace=True)
            df.drop(index=df.index[0], axis=0, inplace=True)  # Drop the first row (header repetition)

            # Define the columns for combining date and time
            cols = ['#YY', 'MM', 'DD', 'hh', 'mm']

            # Lists to store extracted data for each file
            formatted_date_list = []
            wind_direction_list = []
            wind_speed_list = []
            dominant_wave_height_list = []
            dominant_wave_period_list = []
            dominant_wave_angle_list = []
            air_temp_list = []
            sea_temp_list = []
            visibility_list = []
            tide_list = []

            # Loop through each row in the DataFrame
            for index, row in df.iterrows():
                # Combine the date and time columns into a datetime object
                combined_date = pd.to_datetime('-'.join(row[cols].astype(str)), format='%Y-%m-%d-%H-%M')
                formatted_date_list.append(combined_date)

                # Append data to respective lists, converting to numeric where applicable
                wind_direction_list.append(row['WDIR'])
                wind_speed_list.append(row['WSPD'])
                dominant_wave_height_list.append(row['WVHT'])
                dominant_wave_period_list.append(row['DPD'])
                dominant_wave_angle_list.append(row['MWD'])
                air_temp_list.append(row['ATMP'])
                sea_temp_list.append(row['WTMP'])
                visibility_list.append(row['VIS'])
                tide_list.append(row['TIDE'])

            # Create a new DataFrame with the processed data for the current file
            df_processed = pd.DataFrame(
                list(zip(formatted_date_list, wind_direction_list, wind_speed_list, 
                        dominant_wave_height_list, dominant_wave_period_list, 
                        dominant_wave_angle_list, air_temp_list, sea_temp_list, 
                        visibility_list, tide_list)),
                columns=['Date-Time', 'Wind Direction', 'Wind Speed', 'Dominant Wave Height', 
                        'Dominant Wave Period', 'Dominant Wave Angle', 'Air Temp.', 
                        'Sea Temp.', 'Vis.', 'Tide']
            )

            # Store the processed DataFrame in the dictionary
            processed_dataframes[f'df{file_index}'] = df_processed
            print(f"Processed {filename} into DataFrame 'df{file_index}'")

        except Exception as e:
            print(f"Failed to process {filename}: {e}")

# Optional: Print or access individual processed DataFrames
for key, df in processed_dataframes.items():
    print(f"{key}:")
    print(df.head())  # Display the first few rows of each processed DataFrame