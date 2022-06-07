
from unittest import result
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import logging
from fastavro import writer, reader, parse_schema, schemaless_writer

CHUNK_SIZE = 1000

"""
-This is a function to upload a csv file to Snowflake. It requires a connection to snowflake
-Since it requires specific libraries I used pipenv to create the pipfile.lock and pipfile files to better distribute this code.
"""


def load_csv_to_snowflake_table(
    credentials,
    destination,
    filename,
    colnames,
    datatypes
):

    conn = snowflake.connector.connect(
        user=credentials["user"],
        password=credentials["password"],
        account=credentials["account"],
    )  

    cs = conn.cursor()
    try:
        cs.execute(f"USE DATABASE GLOBANT")
        cs.execute(f"USE SCHEMA PUBLIC")       
        # Here I process the file in chunks to not overload memory
        for chunk in pd.read_csv(filename, chunksize=CHUNK_SIZE, names=colnames, on_bad_lines='skip',dtype=datatypes):
            # chunk.columns = chunk.columns.str.upper()
            write_pandas(conn, chunk, destination)
        logging.info(f"File {filename} was uploaded")

        return 1

    except Exception as e:
        print(e)

        return 0
    finally:
        cs.close()

def backup_table_to_avro(credentials, tablename, schema):
    conn = snowflake.connector.connect(
        user=credentials["user"],
        password=credentials["password"],
        account=credentials["account"]
    )  

    cs = conn.cursor()
    
    try:
        query = f"SELECT * FROM {tablename}"
        cs.execute(f"USE DATABASE GLOBANT")
        cs.execute(f"USE SCHEMA PUBLIC")  
        cs.execute(query)
        results = cs.fetchall()        
        rows = []

        # Parse to dict
        for row in results:            
            new_row = {}
            for i in range(len(schema['fields'])):                
                new_row[schema['fields'][i]['name']] = row[i]  
            rows.append(new_row) 

        parsed_schema = parse_schema(schema)

        with open(f'avro_backup/{tablename}.avro','wb') as out:            
            writer(out,parsed_schema, rows)
        
        return 1
    except Exception as e:
        print(e)

        return 0
    finally:
        cs.close()

    
def restore_table_from_avro(credentials, tablename):
    with open(f'avro_backup/{tablename}.avro','rb') as fo:
        avro_reader = reader(fo)
        for record in avro_reader:
            print(record)
