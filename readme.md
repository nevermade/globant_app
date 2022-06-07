# Description

This python project provides an API to run some ingestion data procceses. This project includes additional features like the following:

1.  Upload CSV for 3 tables:
    * HIRED_EMPLOYEES
    * DEPARTMENTS
    * JOBS
2. Backup the tables from snowflake to AVRO files.
3. Restore tables from ABRO backup files

## Considerations

* I considered that all the files are on the same folder like the script. We could create a UI with html, css and javascript to deliver better UI experience but this was not the scope of this task.

* I am using snowflake as my database since it provides a free tier for testing.

* I am using a config file to get my snowflake credentials. This will be provided on the email that I will send to Giovanna and Carlos.

# API Endpoints

## Upload

To execute the upload process on the server you need to call one of thefollowing endpoints:

http://127.0.0.1:3000/hired_employees/upload

http://127.0.0.1:3000/departments/upload

http://127.0.0.1:3000/jobs/upload

The server will trigger the process and send the following json:

```javascript
{
    "message": "The file was uploaded",
    "errors": [<list of records that failed becaese they had null values>]
}
```


## Backup

To execute the backup process on the server you need to call one of thefollowing endpoints. The server is going to create avro files locally on the avro_backup folder:

http://127.0.0.1:3000/hired_employees/backup

http://127.0.0.1:3000/departments/backup

http://127.0.0.1:3000/jobs/backup

The server will trigger the process and send the following json:

```javascript
{
    "message": "Backup file was created"
}
```

## Restore

To execute the restore process on the server you need to call one of thefollowing endpoints. The server is going to read the avro files and upload them into snowflake:

http://127.0.0.1:3000/hired_employees/restore

http://127.0.0.1:3000/departments/restore

http://127.0.0.1:3000/jobs/restore

The server will trigger the process and send the following json:

```javascript
{
    "message": "The backup file was uploaded"
}
```

# How to run the project

1. you need to clone this repo
2. copy the `config.json` file from the email to the root folder of the project. This file contains the snowflake credentials.
3. Run the following command to build the docker image:
```bash
docker build -t globant_app .
```
4. Run the server with the following command:
```bash
docker run -p 3000:3000 globant_app
```
5. Start using the app on your localhost with the explained endpoints from the previous section.