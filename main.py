# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 09:47:52 2023

@author: JoseAntonioToroOspin
"""
import pandas as pd
import os
import logging
from sqlalchemy import create_engine
import glob
import csv
import fastavro
from fastavro import writer, parse_schema

def inic_logger(path, namelog):
    global logger
    logger = logging.getLogger(namelog)
    directory = '/'.join([path, 'logs/'])
    print(directory) 
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Verifying if the handler is already in the list
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

def mysql_data(df,name,required_fields,situation = None):
    if situation == None:
        text = '_mysql.csv'
    else:
        text = '_backup.csv'
    cadena = ', '.join(required_fields)
    # Transforming data for MySQL format
    df_mysql = df.applymap(lambda x: f'"{x}"' if type(x) == str and '"' in x else x)
    data = df_mysql.to_csv(index=False, header=False, quotechar='"', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8')
    # Writing the transformed data to a CSV file
    with open(name + text, 'w') as file:
        file.write(cadena+'\n') # Column names
        file.write(data)

path = os.path.dirname(os.path.abspath(__file__))
os.chdir(path)
namelog = 'poc_globant'

execute_as_buckUp = True
# in case there are files that you dont want to reload from the buckup
archivos_a_eliminar = ['hired_employees', 'jobs'] 

inic_logger(path, namelog)
# Define the data dictionary rules for the tables
data_dict = {
    'jobs': ['id', 'job'],
    'hired_employees': ['id', 'name','datetime','department_id','job_id'],
    'departments': ['id', 'department']
}
DATABASE_URL = 'mysql+pymysql://root:passcode@127.0.0.1/mydatabase'
files = glob.glob(path + '/*.csv')
# Define batch size
BATCH_SIZE = 1000


#we have to use this dict beacause avro does not support int64 dtype
type_mapping = {
    "integer":  "int",
    "floating":  "double",
    "boolean":  "boolean",
    "string":  "string",
    "mixed-integer":  "int",
    "mixed-integer-float":  "double",
    "decimal":  "double",
    "complex":  "string",
    "categorical":  "string",
    "datetime":  "string",
    "timedelta":  "string",
    "mixed":  "string",
    "empty":  "null"
}

if execute_as_buckUp == False:
    logger.info("Normal execution (no backup)")
    #In case there are processed files
    filtered_files = [file for file in files if '_mysql' not in file] 
    filtered_files = [file for file in filtered_files if '_backup' not in file]   
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
        required_fields = data_dict[title(name)]
        engine = create_engine(DATABASE_URL)
        query = f"select * FROM {title(name)};"
        df = pd.read_sql(query, engine)
        column_types = {column: pd.api.types.infer_dtype(df[column]) for column in df.columns}
        avro_schema = {
            "type": "record",
            "name": title(name),
            "fields": [
                {"name": column, "type": ["null", type_mapping[column_types[column]]]} 
                for column in df.columns
                ]
            }
        records = df.to_dict(orient='records')
        with open(title(name) +'.avro', 'wb') as out_file:
            writer(out_file, parse_schema(avro_schema), records)
        #mysql_data(df,title(name),required_fields,situation = 'backup')
        
else:
    logger.info("BACKUP execution")
    avrofiles = glob.glob(path + '/*.avro')
    # Remove elements from the original list.
    for archivo in archivos_a_eliminar:
        avrofiles.remove(os.path.join(path, archivo + '.avro'))
    for name in avrofiles:
        engine = create_engine(DATABASE_URL)
        table_name = title(name)[:-1]
        #We delete the tables before loading the backup.
        query = f"drop table if exists {table_name};"
        engine.execute(query)
        required_fields = data_dict[table_name]
        cadena = ', '.join(required_fields)
        # Open the Avro file and create the Reader object.
        with open(table_name + '.avro', 'rb') as avro_file:
            reader = fastavro.reader(avro_file)
            # The backup data is iterated
            rows = []
            for record in reader:
                rows.append(record)
        dfavro = pd.DataFrame(rows)
        mysql_data(dfavro,table_name,required_fields)
        try:
            for chunk in pd.read_csv(name[:-5]+'_mysql.csv', chunksize=BATCH_SIZE):
                chunk.to_sql(table_name, engine, if_exists='append', index=False)
        except:
            logger.error("Failed to load data for table {}". format(table_name))
        #To close sql alchemy connection
        engine.dispose()
        
for archivo in os.listdir(path):
    if archivo.endswith('_mysql.csv'):
        os.remove(os.path.join(path, archivo))

logging.shutdown()
