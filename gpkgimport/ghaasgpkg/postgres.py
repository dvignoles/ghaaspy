from psycopg2 import sql, connect

class PostgresDB:
    def __init__(self, database=None, user='postgres', password='admin', host='localhost', port=5432):
        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.password = password


        try:
            self.conn = connect(
                dbname=self.database,
                user=self.user,
                host=self.host,
                port=self.port,
                password=self.password
            )

        except Exception as err:
            print("psycopg2 connect() ERROR:", err)
            self.conn = None