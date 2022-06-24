import psycopg2


class PostgresService(object):

    def __init__(self, secrets):
        self.secrets = secrets
        self.connection = None

    def connect_to_rds(self):
        self.connection = psycopg2.connect(user=self.secrets['username'],
                                           password=self.secrets['password'],
                                           host=self.secrets['host'],
                                           port=self.secrets['port'],
                                           database=self.secrets['dbClusterIdentifier'])
        self.connection.autocommit = True

    def execute_query(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)

        result = cursor.fetchall()

        self.connection.commit()
        self.connection.close()

        return result
