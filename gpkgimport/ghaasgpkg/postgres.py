from psycopg2 import sql, connect
from pathlib import Path

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
    
    @classmethod
    def from_pgpass(cls, idsubstring, pgpass=Path.home().joinpath('.pgpass').resolve()):
        """Use postgres password file as source of postgres connection. See https://www.postgresql.org/docs/current/libpq-pgpass.html.
        Entries are of form hostname:port:database:username:password


        Args:
            idsubstring (str): identifying substring ie database, hostname:port:database, hostname:port:databse:username
         pgpass (Path, optional): Path of .pgpass file. Defaults to Path.home().joinpath('.pgpass').resolve().

        Raises:
            FileNotFoundError: if pgpass not valid 

        Returns:
            PostgresDB: class instance
        """
        if not pgpass.exists():
            raise FileNotFoundError(".pgpass file not found at {}".format(pgpass))
        
        with open(pgpass, 'r') as f:
            credentials = f.read().splitlines()
        
        matches = []
        for c in credentials:
            if idsubstring in c:
                matches.append(c)
        
        assert(len(matches) == 1)
        
        host, port, db, user, password = matches[0].split(':')
        return cls(database=db, user=user, password=password, host=host, port=port)
    
    @classmethod
    def from_gdal_string(cls, gdal_pg):
        pass

    
    def get_gdal_string(self):
        """Return gdal driver format postgres connection string
        https://gdal.org/drivers/vector/pg.html
        PG:"dbname='databasename' host='addr' port='5432' user='x' password='y'"

        Returns:
            [type]: [description]
        """
        return '"dbname={} host={} port={} user={} password={}"'.format(self.database, self.host, self.port, self.user, self.password)