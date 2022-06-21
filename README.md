# README #

This is a generic version for a microservice in Lambda to download a specific file from S3 with Cognito as authorizer using Serverless framework.

This is a project create:
    - An API using serveless framework to validate and upload csv file to S3.

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

1. Run in local

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
