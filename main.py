# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 09:47:52 2023

@author: JoseAntonioToroOspin
"""
import pandas as pd

# Define the path to the CSV file
csv_file_path = "path/to/csv/file.csv"

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file_path)

# Display the first 5 rows of the DataFrame
print(df.head())
