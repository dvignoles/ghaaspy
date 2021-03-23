import unittest
from gpkgimport.ghaasgpkg.gpkg import *
from pathlib import Path


class TestGpkg(unittest.TestCase):

    def setUp(self):
        # Load test data
        chart_data = Path('/asrc/ecr/balazs/Projects/NASA-IDS_CHART_Coastal_Hypoxia/')
        ghaas_data = Path('/asrc/ecr/balazs/GHAAS')
        self.gpkg_sed = sanitize_path(chart_data.joinpath('GPKGresults','SE-Asia','TerraClimate+WBMstableSED+Prist','SE-Asia_TerraClimate+WBMstableSED+Prist_01min.gpkg'))
        self.gpkg_ghaas = sanitize_path(ghaas_data.joinpath('ModelRuns','GPKGresults','Brazil','TerraClimate+WBMstableDist04','Brazil_TerraClimate+WBMstableDist04_01min.gpkg'))

        self.sed_mod_tables1 = [ "main.River-Mouth_BedloadFlux_annual",
                            "main.River-Mouth_BedloadFlux_monthly",
                            "main.River-Mouth_SedimentFlux_annual",
                            "main.River-Mouth_BedloadFlux_daily",
                            "main.River-Mouth_SedimentFlux_monthly",
                            "main.River-Mouth_discharge_annual",
                            "main.River-Mouth_SedimentFlux_daily",
                            "main.River-Mouth_discharge_monthly",
                            "main.River-Mouth_discharge_daily",]

        self.sed_mod_tables2 = [ "River-Mouth_BedloadFlux_annual",
                            "River-Mouth_BedloadFlux_monthly",
                            "River-Mouth_SedimentFlux_annual",
                            "River-Mouth_BedloadFlux_daily",
                            "River-Mouth_SedimentFlux_monthly",
                            "River-Mouth_discharge_annual",
                            "River-Mouth_SedimentFlux_daily",
                            "River-Mouth_discharge_monthly",
                            "River-Mouth_discharge_daily",]

        self.ghaas_mod_tables1 = [ "brazil.\"discharge_confluence_annual_terra+wbm04_03min\"",
                            "brazil.\"discharge_confluence_monthly_terra+wbm04_03min\"",
                            "brazil.\"discharge_mouth_annual_terra+wbm04_03min\"",
                            "brazil.\"discharge_mouth_monthly_terra+wbm04_03min\"",
                            "brazil.\"discharge_reservoirdam_annual_terra+wbm04_03min\"",
                            "brazil.\"discharge_reservoirdam_monthly_terra+wbm04_03min\"",
                            "brazil.\"evapotranspiration_basin_annual_terra+wbm04_03min\"",
                            "brazil.\"evapotranspiration_basin_monthly_terra+wbm04_03min\"",
                            "brazil.\"evapotranspiration_country_annual_terra+wbm04_03min\"",
                            "brazil.\"evapotranspiration_country_monthly_terra+wbm04_03min\"",
                            "brazil.\"evapotranspiration_state_annual_terra+wbm04_03min\"",
                            "brazil.\"evapotranspiration_state_monthly_terra+wbm04_03min\"",
                            "brazil.\"evapotranspiration_subbasin_annual_terra+wbm04_03min\"",
                            "brazil.\"evapotranspiration_subbasin_monthly_terra+wbm04_03min\"",]

        self.ghaas_mod_tables2 = [ "\"brazil\".\"discharge_confluence_annual_terra+wbm04_03min\"",
                            "\"brazil\".\"discharge_confluence_monthly_terra+wbm04_03min\"",
                            "\"brazil\".\"discharge_mouth_annual_terra+wbm04_03min\"",
                            "\"brazil\".\"discharge_mouth_monthly_terra+wbm04_03min\"",
                            "\"brazil\".\"discharge_reservoirdam_annual_terra+wbm04_03min\"",
                            "\"brazil\".\"discharge_reservoirdam_monthly_terra+wbm04_03min\"",
                            "\"brazil\".\"evapotranspiration_basin_annual_terra+wbm04_03min\"",
                            "\"brazil\".\"evapotranspiration_basin_monthly_terra+wbm04_03min\"",
                            "\"brazil\".\"evapotranspiration_country_annual_terra+wbm04_03min\"",
                            "\"brazil\".\"evapotranspiration_country_monthly_terra+wbm04_03min\"",
                            "\"brazil\".\"evapotranspiration_state_annual_terra+wbm04_03min\"",
                            "\"brazil\".\"evapotranspiration_state_monthly_terra+wbm04_03min\"",
                            "\"brazil\".\"evapotranspiration_subbasin_annual_terra+wbm04_03min\"",
                            "\"brazil\".\"evapotranspiration_subbasin_monthly_terra+wbm04_03min\"",]
        

    # Move to integration category?
    def test_extract_tables(self):
        sed_tables = list(extract_tables(self.gpkg_sed))
        ghaas_tables = list(extract_tables(self.gpkg_ghaas))
        self.assertEqual(len(sed_tables), 10, "Should be 10 tables (of actual data)")
        self.assertEqual(len(ghaas_tables), 77, "Should be 77 tables (of actual data)")
    
    def test_group_geography_vs_model(self):
        #TODO: Mock data here
        sed_tables = list(extract_tables(self.gpkg_sed))
        ghaas_tables = list(extract_tables(self.gpkg_ghaas))

        sed_total = len(sed_tables)
        ghaas_total = len(ghaas_tables)
        sed_geo, sed_mod = group_geography_vs_model(sed_tables)
        ghaas_geo, ghaas_mod = group_geography_vs_model(ghaas_tables)

        self.assertEqual(len(sed_geo), 1, "Should be 1 geography table")
        self.assertEqual(len(sed_mod), 9, "Should be 10 tables model output")

        self.assertEqual(len(ghaas_geo), 11, "Should be 12 geography table")
        self.assertEqual(len(ghaas_mod), 66, "Should be 10 tables model output")

    def test_clean_tablenames(self):
        schema_group1 = ['"schema".my_table_1','"schema".my_table_2','"schema".my_table_3']
        schema_group2 = ['schema.my_table_1','schema.my_table_2','schema.my_table_3']
        tables_only = ['my_table_1','my_table_2','my_table_3']

        self.assertEqual(clean_tablenames(schema_group1), ('schema', tables_only), "should return 'schema' and [tablenames]")
        self.assertEqual(clean_tablenames(schema_group2), ('schema', tables_only), "should return 'schema' and [tablenames]")
        self.assertEqual(clean_tablenames(tables_only), (None, tables_only), "should return None and [tablenames]")


    def test_group_annual_monthly(self):

        sed_annual_monthly1 = group_annual_monthly(self.sed_mod_tables1)        
        self.assertEqual(sed_annual_monthly1["main.river-mouth_bedloadflux"], set(["main.River-Mouth_BedloadFlux_annual","main.River-Mouth_BedloadFlux_monthly","main.River-Mouth_BedloadFlux_daily"]), "should be set of annual,monthly,daily")
        self.assertEqual(sed_annual_monthly1["main.river-mouth_sedimentflux"], set(["main.River-Mouth_SedimentFlux_annual","main.River-Mouth_SedimentFlux_monthly","main.River-Mouth_SedimentFlux_daily"]), "should be set of annual,monthly,daily")
        self.assertEqual(sed_annual_monthly1["main.river-mouth_discharge"], set(["main.River-Mouth_discharge_annual","main.River-Mouth_discharge_monthly","main.River-Mouth_discharge_daily"]), "should be set of annual,monthly,daily")

        sed_annual_monthly2 = group_annual_monthly(self.sed_mod_tables2)        
        self.assertEqual(sed_annual_monthly2["river-mouth_bedloadflux"], set(["River-Mouth_BedloadFlux_annual","River-Mouth_BedloadFlux_monthly","River-Mouth_BedloadFlux_daily"]), "should be set of annual,monthly,daily")
        self.assertEqual(sed_annual_monthly2["river-mouth_sedimentflux"], set(["River-Mouth_SedimentFlux_annual","River-Mouth_SedimentFlux_monthly","River-Mouth_SedimentFlux_daily"]), "should be set of annual,monthly,daily")
        self.assertEqual(sed_annual_monthly2["river-mouth_discharge"], set(["River-Mouth_discharge_annual","River-Mouth_discharge_monthly","River-Mouth_discharge_daily"]), "should be set of annual,monthly,daily")


        ghaas_annual_monthly1 = group_annual_monthly(self.ghaas_mod_tables1)
        self.assertEqual(len(ghaas_annual_monthly1.keys()),7)

        self.assertEqual(ghaas_annual_monthly1["brazil.\"discharge_confluence_terra+wbm04_03min\""], set([
            "brazil.\"discharge_confluence_monthly_terra+wbm04_03min\"",
            "brazil.\"discharge_confluence_annual_terra+wbm04_03min\""
        ]))

        ghaas_annual_monthly2 = group_annual_monthly(self.ghaas_mod_tables2)
        self.assertEqual(len(ghaas_annual_monthly2.keys()),7)

        self.assertEqual(ghaas_annual_monthly2[ "\"brazil\".\"discharge_reservoirdam_terra+wbm04_03min\""], set([
            "\"brazil\".\"discharge_reservoirdam_annual_terra+wbm04_03min\"",
            "\"brazil\".\"discharge_reservoirdam_monthly_terra+wbm04_03min\""
        ]))

if __name__ == '__main__':
    unittest.main()