# POC Globant
This code is a Python script that performs the following actions:

```python
import pandas as pd
import os
import logging
from sqlalchemy import create_engine
import glob
import csv```

Defines a function to initialize a logger.
Defines a function to extract the title of a CSV file.
Defines a function to validate data in a CSV file.
Defines a function to transform and write data to a MySQL-compatible CSV file.
Reads in CSV files in the same directory as the script.
Validates the data in each CSV file.
Transforms and writes the validated data to a MySQL-compatible CSV file.
Loads the transformed data into MySQL tables.
Generates a backup of each table.
### How to Use
To use this script, follow these steps:

Make sure that you have Python installed on your machine.
Install the required Python packages: pandas, sqlalchemy, csv, and logging.
Place all the CSV files that you want to load into MySQL in the same directory as the script.
Modify the data_dict dictionary to match the structure of your CSV files.
Modify the DATABASE_URL variable to match your MySQL database.
Run the script.
### Additional Notes
The script will create a log file in a logs subdirectory.
The script uses a batch size of 1000 records for loading data into MySQL. You can modify this value by changing the BATCH_SIZE variable.
You can toggle the execute_as_buckUp variable to run the script in either normal or backup mode. In normal mode, the script will load data into MySQL and generate backups. In backup mode, the script will delete the MySQL tables and load data from backups.
### Dependencies
This script has the following dependencies:

pandas
sqlalchemy
csv
logging
