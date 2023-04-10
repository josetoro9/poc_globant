# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 09:47:52 2023

@author: JoseAntonioToroOspin
"""
import pandas as pd
import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import glob
import pip
import pkg_resources
from flask import Flask, request
import csv


#To install

required = {'pymysql'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    pip.main(['install']+list(missing))


def title(csv_file):
    title = os.path.basename(csv_file[:-4])
    return title

def validate_data(csv_file,structure):
    try:
        df = pd.read_csv(csv_file, names=structure)
        logging.info("Data for table {} is OK". format(title(csv_file)))
    except:
        logging.error("Failed to validate data for table {}". format(title(csv_file)))
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
        
#Loading data to mysql
for name in glob.glob(path + '/*_mysql.csv'):
    engine = create_engine(DATABASE_URL)
    table_name = title(name)[:-6]
    table = pd.read_csv(name)
    # in this case we can change the if exists parameter eg: 'replace','append'
    table.to_sql(table_name, engine, if_exists='replace', index=False)
    #To close sql alchemy connection
    engine.dispose()
    print(table_name)
    
print(bbb)







