from datetime import datetime

from app.api.api_v1.services.util import ColumnsType, Table


class ValidateCSV(object):

    def __init__(self, table_op, file_separator=','):
        self.table_op = table_op
        self.structure = self.get_structure(table_op)
        self.file_separator = file_separator

    def get_structure(self, table_op):
        if table_op == Table.DEPARMENTS:
            return [ColumnsType.INT, ColumnsType.STRING]

        elif table_op == Table.JOBS:
            return [ColumnsType.INT, ColumnsType.STRING]

        elif table_op == Table.HIRED_EMPLOYEES:
            return [
                ColumnsType.INT,
                ColumnsType.STRING,
                ColumnsType.DATE,
                ColumnsType.INT,
                ColumnsType.INT
            ]
        else:
            raise Exception("Invalid Table Option")

    def valid_content(self, content):
        for row in content:
            elements = row.split(self.file_separator)
            if len(elements) != len(self.structure):
                raise Exception('Unexpected record length')
            for i in range(len(elements)):
                try:
                    if self.structure[i] == ColumnsType.INT:
                        int(elements[i])
                    elif self.structure[i] == ColumnsType.DATE:
                        datetime.strptime(elements[i], '%Y-%m-%dT%H:%M:%SZ')
                    elif self.structure[i] == ColumnsType.STRING:
                        c_element = elements[i].replace(' ', '')
                        if not c_element.isidentifier():
                            raise Exception()
                except Exception as e:
                    raise Exception(f'Invalid element {elements[i]} {self.structure[i]}_')
        return content
