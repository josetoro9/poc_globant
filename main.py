# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 09:47:52 2023

@author: JoseAntonioToroOspin
"""
import pandas as pd
import os
import logging
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import glob
import csv


def inic_logger(path, namelog):
    global logger
    logger = logging.getLogger(namelog)
    
    directory = '/'.join([path, 'logs/'])
    print(directory) 
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Verificar si el controlador ya está en la lista
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == directory + namelog + '.log' for h in logger.handlers):
        hdlr = logging.FileHandler(directory + namelog + '.log')
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(filename)s] :%(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr) 
    
    logger.setLevel(logging.INFO)


def title(csv_file):
    title = os.path.basename(csv_file[:-4])
    return title

def validate_data(csv_file,structure):
    try:
        df = pd.read_csv(csv_file, names=structure)
        logger.info("Data for table {} is OK". format(title(csv_file)))
    except:
        logger.error("Failed to validate data for table {}". format(title(csv_file)))
        df = pd.DataFrame(columns=structure)
    return df

def mysql_data(df,name,required_fields):
    cadena = ', '.join(required_fields)
    # Transforma los datos para el formato MySQL
    df_mysql = df.applymap(lambda x: f'"{x}"' if type(x) == str and '"' in x else x)
    data = df_mysql.to_csv(index=False, header=False, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

    # Escribe los datos transformados en un archivo CSV
    with open(name +'_mysql.csv', 'w') as file:
        file.write(cadena+'\n') # nombres de las columnas
        file.write(data)

path = os.path.dirname(os.path.abspath(__file__))
os.chdir(path)

namelog = 'poc_globant'

execute_as_buckUp = False

inic_logger(path, namelog)

# Define the data dictionary rules for the tables
data_dict = {
    'jobs': ['id', 'job'],
    'hired_employees': ['id', 'name','datetime','department_id','job_id'],
    'departments': ['id', 'department']
}

DATABASE_URL = 'mysql+pymysql://root:passcode@127.0.0.1/mydatabase'

files = glob.glob(path + '/*.csv')
#In case there are processed files
filtered_files = [file for file in files if '_mysql' not in file]

#Reading and managing the data
logging.info("Reading the data")
for name in filtered_files:
    required_fields = data_dict[title(name)]
    if title(name) == 'jobs':
        jobs = validate_data(name, required_fields)
        mysql_data(jobs,title(name),required_fields)
    elif title(name) == 'hired_employees':
        hired_employees = validate_data(name, required_fields)
        mysql_data(hired_employees,title(name),required_fields)
    elif title(name) == 'departments':
        departments = validate_data(name, required_fields)
        mysql_data(departments,title(name),required_fields)
        

# Define batch size
BATCH_SIZE = 1000


#Loading data to mysql
for name in glob.glob(path + '/*_mysql.csv'):
    table_name = title(name)[:-6]
    required_fields = data_dict[table_name]
    cadena = ', '.join(required_fields)
    try:
        for chunk in pd.read_csv(name, chunksize=BATCH_SIZE):
            engine = create_engine(DATABASE_URL)
            # in this case we can change the if exists parameter eg: 'replace','append'
            chunk.to_sql(table_name, engine, if_exists='append', index=False)
            #To close sql alchemy connection
            engine.dispose()
    except:
        logger.error("Failed to load data for table {}". format(table_name))
        
# generating a back up of each table
for name in filtered_files:
    engine = create_engine(DATABASE_URL)
    query = f"select * FROM {table_name};"
    df = pd.read_sql(query, engine)
    csv_path = os.path.join(path,title(name) + '_backup.csv')
    df.to_csv(csv_path, index=False)


 


logging.shutdown()


 
