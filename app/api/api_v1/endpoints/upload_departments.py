import logging

from app.api.api_v1.services.historic_generator import HistoricGenerator
from app.api.api_v1.services.util import Table
from app.api.api_v1.services.validate_csv import ValidateCSV
from fastapi import APIRouter, HTTPException, UploadFile

router = APIRouter()


@router.post("/upload_departments/")
async def root(file: UploadFile):
    contents = await file.read()
    try:
        list_rows = content_valid(contents)
    except Exception as e:
        logging.info(f'/upload_departments/ Invalid contents {contents}')
        return HTTPException(status_code=422, detail=str(e))

    create_csv_and_upload_to_s3(list_rows)

    return {'message': 'successful upload'}


def content_valid(contents):
    valid_csv = ValidateCSV(Table.DEPARMENTS)
    clean_content = (contents.decode("us-ascii")).replace('\r', '')
    return valid_csv.valid_content(clean_content.split('\n'))


def create_csv_and_upload_to_s3(list_rows):
    hg = HistoricGenerator(Table.DEPARMENTS)
    path_historic = hg.create_csv_file_locally(list_rows)
    hg.upload_file(path_historic)
