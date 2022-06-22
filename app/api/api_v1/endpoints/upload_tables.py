import logging

from app.api.api_v1.services.historic_generator import HistoricGenerator
from app.api.api_v1.services.util import Table
from app.api.api_v1.services.validate_csv import ValidateCSV
from fastapi import APIRouter, HTTPException, UploadFile

router = APIRouter()


@router.post("/departments/upload/")
async def department_upload(file: UploadFile):
    contents = await file.read()
    return valid_content_and_upload_to_s3(contents, Table.DEPARMENTS)


@router.post("/hired_employees/upload/")
async def hired_employees_upload(file: UploadFile):
    contents = await file.read()
    return valid_content_and_upload_to_s3(contents, Table.HIRED_EMPLOYEES)


@router.post("/jobs/upload/")
async def jobs_upload(file: UploadFile):
    contents = await file.read()
    return valid_content_and_upload_to_s3(contents, Table.JOBS)


def valid_content_and_upload_to_s3(contents, table_op):
    try:
        list_rows = valid_content(contents, table_op)
    except Exception as e:
        logging.info(f'/{table_op}/upload/ Invalid contents {contents}')
        return HTTPException(status_code=422, detail=str(e))

    create_csv_and_upload_to_s3(list_rows, table_op)

    return {'message': 'successful upload'}


def valid_content(contents, table_op):
    valid_csv = ValidateCSV(table_op)
    clean_content = (contents.decode("us-ascii")).replace('\r', '')
    return valid_csv.valid_content(clean_content.split('\n'))


def create_csv_and_upload_to_s3(list_rows, table_op):
    hg = HistoricGenerator(table_op)
    path_historic = hg.create_csv_file_locally(list_rows)
    hg.upload_file(path_historic)
