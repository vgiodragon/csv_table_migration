import logging

from app.api.api_v1.services.table_generator import TableGenerator
from app.api.api_v1.services.util import Table
from app.api.api_v1.services.validate_csv import ValidateCSV
from fastapi import APIRouter, HTTPException, UploadFile

router = APIRouter()


@router.post("/departments/upload/")
async def departments_upload(file: UploadFile):
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
        list_rows = valid_contents(contents, table_op)
    except Exception as e:
        logging.info(f'/{table_op}/upload/ Invalid contents {contents}')
        return HTTPException(status_code=422, detail=str(e))

    create_csv_and_upload_to_s3(list_rows, table_op)

    return {'message': 'successful upload to s3'}


def valid_contents(contents, table_op):
    valid_csv = ValidateCSV(table_op)
    clean_content = (contents.decode("us-ascii")).replace('\r', '').split('\n')
    valid_csv.valid_content(clean_content)
    return valid_csv.get_content_array_rows(clean_content)


def create_csv_and_upload_to_s3(list_rows, table_op):
    hg = TableGenerator(table_op)
    path_historic = hg.create_csv_file_locally(list_rows)
    hg.upload_file(path_historic)
