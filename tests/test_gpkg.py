import unittest
from gpkgimport.ghaasgpkg.gpkg import _group_annual_monthly, extract_tables, sanitize_path
from pathlib import Path


class TestSum(unittest.TestCase):

    def setUp(self):
        # Load test data
        fixtures = Path(__file__).resolve().parent.joinpath('fixtures')
        self.gpkg_sed = fixtures.joinpath('SE-Asia_TerraClimate+WBMstableSED+Prist_01min.gpkg')

    def test_extract_tables(self):
        tables = list(extract_tables(self.gpkg_sed))
        self.assertEqual(len(tables), 10, "Should be 10 tables (of actual data)")


if __name__ == '__main__':
    unittest.main()