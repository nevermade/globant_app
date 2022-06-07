from flask import Flask
from flask_restful import Resource, Api, reqparse
import sys
import json
import logging
from globantpkg.data_ingestion import load_csv_to_snowflake_table

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s]{%(filename)s:%(lineno)-8d}%(levelname)-5s- %(message)s",
    datefmt="%D %H:%M:%S",
)

CONFIG_PARAMS = {}


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
        if operation=='upload':
            result = load_csv_to_snowflake_table(
                CONFIG_PARAMS["credentials"],
                "HIRED_EMPLOYEES",
                "hired_employees.csv",
                colnames=["ID", "NAME", "DATETIME", "DEPARTMENT_ID", "JOB_ID"],
                datatypes={
                    "ID": 'Int32',
                    "NAME": 'string',
                    "DATETIME": 'string',
                    "DEPARTMENT_ID": 'Int32',
                    "JOB_ID": 'Int32'
                }
            )
            if result:
                return {"message": "The file was uploaded"}, 200
            else:
                return {"message":"Error encountered while processing the file"},200
        elif operation == 'backup':
            return {"message": "backup is ready to download"}
        elif operation == 'restore':
            return {"message": "backup is ready to download"}

    
    def backup(self,operation=None):
        return "Hello Worl"


class Departments(Resource):
    def get(self,operation=None):
        result = load_csv_to_snowflake_table(
            CONFIG_PARAMS["credentials"],
            "DEPARTMENTS",
            "departments.csv",
            colnames=["ID", "DEPARTMENT"],
            datatypes={"ID": int, "DEPARTMENT": str},
        )
        if result:
            return {"message": "The file was uploaded"}, 200
        else:
            return {"message":"Error encountered while processing the file"},200

class Jobs(Resource):
    def get(self):
        result = load_csv_to_snowflake_table(
            CONFIG_PARAMS["credentials"],
            "JOBS",
            "jobs.csv",
            colnames=["ID", "JOB"],
            datatypes={"ID": int, "JOB": str},
        )
        if result:
            return {"message": "The file was uploaded"}, 200
        else:
            return {"message":"Error encountered while processing the file"},200

api.add_resource(Hired_Employees, "/hired_employees/<string:operation>")
api.add_resource(Departments, "/departments/<string:operation>")
api.add_resource(Jobs, "/jobs/<string:operation>")


if __name__ == "__main__":
    initialize()
    app.run(port=3000, debug=True)
