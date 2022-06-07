from flask import Flask
from flask_restful import Resource, Api, reqparse
import sys
import json
import logging
from globantpkg.data_ingestion import load_csv_to_snowflake_table
from globantpkg.data_ingestion import backup_table_to_avro
from globantpkg.data_ingestion import restore_table_from_avro

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s]{%(filename)s:%(lineno)-8d}%(levelname)-5s- %(message)s",
    datefmt="%D %H:%M:%S",
)

CONFIG_PARAMS = {}


HIRE_EMPLOYEES_SCHEMA = {
    "name": "hire_employees",
    "type": "record",
    "fields": [
        {"name": "ID", "type": ["int", "null"]},
        {"name": "NAME", "type": ["string", "null"]},
        {"name": "DATETIME", "type": ["string", "null"]},
        {"name": "DEPARTMENT_ID", "type": ["int", "null"]},
        {"name": "JOB_ID", "type": ["int", "null"]},
    ],
}

DEPARMENT_SCHEMA = {
    "name": "departments",
    "type": "record",
    "fields": [
        {"name": "ID", "type": ["int", "null"]},
        {"name": "DEPARTMENT", "type": ["string", "null"]},
    ],
}

JOB_SCHEMA = {
    "name": "jobs",
    "type": "record",
    "fields": [
        {"name": "ID", "type": ["int", "null"]},
        {"name": "JOB", "type": ["string", "null"]},
    ],
}


HIRED_EMPLOYEES_DT = {
    "ID": "Int32",
    "NAME": "string",
    "DATETIME": "string",
    "DEPARTMENT_ID": "Int32",
    "JOB_ID": "Int32",
}

DEPARTMENTS_DT = {"ID": "Int32", "DEPARTMENT": "string"}

JOBS_DT = {"ID": "Int32", "JOB": "string"}


def initialize():
    with open("config.json") as json_data_file:
        global CONFIG_PARAMS
        CONFIG_PARAMS = json.load(json_data_file)

    try:
        global FILE_NAME
        FILE_NAME = sys.argv[1]
    except:
        logging.info("")


app = Flask(__name__)
api = Api(app)


class Hired_Employees(Resource):
    def get(self, operation=None):
        if operation == "upload":
            result = load_csv_to_snowflake_table(
                CONFIG_PARAMS["credentials"],
                "HIRED_EMPLOYEES",
                "hired_employees.csv",
                colnames=["ID", "NAME", "DATETIME", "DEPARTMENT_ID", "JOB_ID"],
                datatypes=HIRED_EMPLOYEES_DT,
            )
            if result.shape[0]>=0:
                return {"message": "The file was uploaded",'errors':result.to_json(orient='records')}, 200
            else:
                return {"message": "Error encountered while processing the file"}, 200
        elif operation == "backup":
            backup_table_to_avro(
                CONFIG_PARAMS["credentials"], "HIRED_EMPLOYEES", HIRE_EMPLOYEES_SCHEMA
            )
            return {"message": "Backup file was created"}
        elif operation == "restore":
            restore_table_from_avro(CONFIG_PARAMS["credentials"], "HIRED_EMPLOYEES")
            return {"message": "The backup file was uploaded"}


class Departments(Resource):
    def get(self, operation=None):
        if operation == "upload":
            result = load_csv_to_snowflake_table(
                CONFIG_PARAMS["credentials"],
                "DEPARTMENTS",
                "departments.csv",
                colnames=["ID", "DEPARTMENT"],
                datatypes=DEPARTMENTS_DT,
            )
            if result.shape[0]>=0:
                return {"message": "The file was uploaded",'errors':result.to_json(orient='records')}, 200
            else:
                return {"message": "Error encountered while processing the file"}, 200
        elif operation == "backup":
            backup_table_to_avro(
                CONFIG_PARAMS["credentials"], "DEPARTMENTS", DEPARMENT_SCHEMA
            )
            return {"message": "Backup file was created"}
        elif operation == "restore":
            restore_table_from_avro(CONFIG_PARAMS["credentials"], "DEPARTMENTS")
            return {"message": "The backup file was uploaded"}


class Jobs(Resource):
    def get(self, operation=None):
        if operation == "upload":
            result = load_csv_to_snowflake_table(
                CONFIG_PARAMS["credentials"],
                "JOBS",
                "jobs.csv",
                colnames=["ID", "JOB"],
                datatypes=JOBS_DT,
            )
            if result.shape[0]>=0:
                return {"message": "The file was uploaded",'errors':result.to_json(orient='records')}, 200
            else:
                return {"message": "Error encountered while processing the file"}, 200
        elif operation == "backup":
            backup_table_to_avro(CONFIG_PARAMS["credentials"], "JOBS", JOB_SCHEMA)
            return {"message": "Backup file was created"}
        elif operation == "restore":
            restore_table_from_avro(CONFIG_PARAMS["credentials"], "JOBS")
            return {"message": "The backup file was uploaded"}


api.add_resource(Hired_Employees, "/hired_employees/<string:operation>")
api.add_resource(Departments, "/departments/<string:operation>")
api.add_resource(Jobs, "/jobs/<string:operation>")


if __name__ == "__main__":
    initialize()
    app.run(host="0.0.0.0",port=3000, debug=True)
