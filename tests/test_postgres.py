import unittest
from ghaaspy.postgres import PostgresDB
from pathlib import Path

class TestPostgres(unittest.TestCase):

    def test_from_pgpass(self):
        mydb1 = PostgresDB.from_pgpass('localhost', pgpass=Path('fixtures/.pgpass'), verify=False)
        mydb2 = PostgresDB.from_pgpass('12.45.67.89:5432:mydb2', pgpass=Path('fixtures/.pgpass'), verify=False)
        mydb3 = PostgresDB.from_pgpass('12.45.67.89:5432:mydb3', pgpass=Path('fixtures/.pgpass'), verify=False)

        self.assertEqual(mydb1.database, 'mydb1')
        self.assertEqual(mydb1.host, 'localhost')
        self.assertEqual(mydb1.port, 5433)
        self.assertEqual(mydb1.user, 'itsme')
        self.assertEqual(mydb1.password, 'goodbye456')

        self.assertEqual(mydb2.database, 'mydb2')
        self.assertEqual(mydb3.database, 'mydb3')

    def test_from_gdal_string(self):
        mydb1 = PostgresDB.from_gdal_string("dbname=mydb1 host=98.76.54.123 port=5435 user=myuser password=mypassword", verify=False)
        self.assertEqual(mydb1.database, 'mydb1')
        self.assertEqual(mydb1.host, '98.76.54.123')
        self.assertEqual(mydb1.port, 5435)
        self.assertEqual(mydb1.user, 'myuser')
        self.assertEqual(mydb1.password, 'mypassword')

        self.assertRaises(AssertionError, PostgresDB.from_gdal_string,"dbname=mydb1 host=98.76.54.123 port=5435", verify=False)

    def test_get_gdal_string(self):
        mydb1 = PostgresDB(database='mydiddlydb', user='dvignoles', password='supersecure', host='800.888.8888', port='41968', verify=False)
        self.assertEqual(mydb1.get_gdal_string(), 'dbname=mydiddlydb host=800.888.8888 port=41968 user=dvignoles password=supersecure')
        
if __name__ == '__main__':
    unittest.main()