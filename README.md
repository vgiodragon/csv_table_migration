# README #

This a project that creates:
    - An API using serveless framework and FASTAPI to validate and upload csv file to S3.
    - A Lambda function to read a csv File from an S3 event and then insert multiple rows in a query to a Postgres DB
    - A Lambda function to truncate a Postgres table and then run a glue job that restore the table from a avro file in S3

### How do I get set up? ###

1. Create a virtual environment

        $ virtualenv ve

2. Access to the virtual env

        $ source ve/bin/activate

3. Run development requirements

        $ pip install -r requirements.txt

4. Install pre-commit

        $ pre-commit install

### How do I run? ###

1. Run API server locally

        $ uvicorn app.main:app --reload


### How do I deploy? ###

1. Install serverless framework

        $ npm install i -g serverless

2. Install npm dependecies

        $ sls plugin install -n serverless-python-requirements

3. Deploy to AWS using stage dev

        $ sls deploy --stage dev

4. Delete full stack stage dev

        $ sls remove --stage dev
