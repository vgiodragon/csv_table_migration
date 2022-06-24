import csv
import datetime

from app.api.api_v1.services import settings
from app.api.api_v1.services.aws import S3Service
from app.api.api_v1.services.util import Table


class TableGenerator(object):

    def __init__(self, table_op):
        self.table_name = self.get_table_name(table_op)
        self.s3_service = S3Service(settings.AWS_S3_BUCKET_NAME,
                                    settings.AWS_S3_PATH)

    def get_table_name(self, table_op):
        if table_op == Table.DEPARMENTS:
            return 'departments'
        elif table_op == Table.JOBS:
            return 'jobs'
        elif table_op == Table.HIRED_EMPLOYEES:
            return 'hired_employees'
        else:
            raise Exception('Invalid Table Option')

    def create_csv_file_locally(self, list_rows):
        path_historic = '/'.join([settings.TMP_FOLDER, self.table_name + '.csv'])

        with open(path_historic, 'w', newline='') as f:
            wr = csv.writer(f)
            wr.writerows(list_rows)

        return path_historic

    def upload_file(self, path_historic):
        str_now_fname = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S.csv")

        s3_path_object = self.s3_service.build_key_name(
            self.table_name,
            str_now_fname,
        )

        self.s3_service.upload_file_from_path(path_historic, s3_path_object)
