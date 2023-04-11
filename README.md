# Globant PoC

This code is a Python script that reads CSV files, validates the data in them, transforms it to MySQL format, and loads it into a MySQL database. It also generates backups of the tables.

## Dependencies
- pandas
- os
- logging
- sqlalchemy
- glob
- csv

## How to use
1. Install the dependencies listed above.
2. Make sure you have a MySQL database running.
3. Put your CSV files in the same directory as the script.
4. Set up the data dictionary rules for your tables in the _'data_dict'_ variable.
5. Set up your MySQL database connection URL in the _'DATABASE_URL'_ variable.
6. Set the _'execute_as_buckUp'_ variable to **True** or **False**, depending on whether you want to execute a normal load or a backup.
7. Run the script.

If you set _'execute_as_buckUp'_ to  **False**, the script will read the CSV files in the directory, validate the data, transform it to MySQL format, and load it into the MySQL database. It will also generate backups of each table.

If you set execute_as_buckUp to True, the script will load the backups of each table into the MySQL database.

## Functionality
The script has the following functionality:

- Reads all CSV files in the same directory as the script.
- Validates the data in each file against a data dictionary.
- Transforms the data to MySQL format.
- Loads the data into a MySQL database.
- Generates backups of each table.

## Example usage
Suppose you have a MySQL database running on _**'127.0.0.1'**_ with the username _'root'_ and password _'passcode'_, and you have the following CSV files in the same directory as the script:

- _'jobs.csv'_
- _'hired_employees.csv'_
- _'departments.csv'_

You could set up the data dictionary like this:

```python
data_dict = {
    'jobs': ['id', 'job'],
    'hired_employees': ['id', 'name','datetime','department_id','job_id'],
    'departments': ['id', 'department']
}
```
You could set up the MySQL database connection URL like this:

```python
DATABASE_URL = 'mysql+pymysql://root:passcode@127.0.0.1/mydatabase'
```

You could then run the script and it will read the CSV files, validate the data, transform it to MySQL format, and load it into the MySQL database. It will also generate backups of each table.
