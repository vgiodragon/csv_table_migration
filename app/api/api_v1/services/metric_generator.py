import logging

import psycopg2
from app.api.api_v1.services.aws import SecretManagerService
from app.api.api_v1.services.database import PostgresService
from app.api.api_v1.services.util import METRIC, QUERY


class MetricGenerator(object):

    def __init__(self, metric_op, db_credential, region_name):
        sm = SecretManagerService(db_credential, region_name)
        self.ps = PostgresService(
            sm.get_secret()
        )
        self.metric_op = metric_op

    def get_metric(self, year=None):
        if self.metric_op == METRIC.QUARTER:
            self.ps.connect_to_rds()
            return self.ps.execute_query(
                QUERY.QUARTER.format(
                    year=year
                )
            )
