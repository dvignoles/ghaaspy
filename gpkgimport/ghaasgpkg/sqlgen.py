"""Dynamic sql generation for creating verbose tables / views"""

GROUP1 = {'hunits': ('hydrostn30_confluence', 'hydrostn30_mouth', 'grandv13hydrostn30_dam', 'rivermouth'),
          'outputs': ('discharge', 'riverwidth', 'riverdepth', 'bedloadflux', 'sedimentflux')}
GROUP2 = {'hunits': ('hydrostn30_basin', 'hydrostn30_subbasin', 'faogaul_country', 'faogaul_state'),
          'outputs': ('evapotranspiration', 'soilmoisture', 'relativesoilmoisture',
                      'rainpet', 'snowpack', 'runoff')}

#### SQL generator helper funcs ####
def select_years(start, end):
    template = "SELECT {year} UNION ALL"
    selects = [template.format(year=y) for y in range(end, start, -1)]
    selects.append("SELECT {}".format(start))
    return "\n".join(selects)


def pivot_table_columns(start, end, output):
    template = "ct.{output}_{year},"
    table_cols = [template.format(output=output, year=y)
                  for y in range(start, end)]
    table_cols.append(template[:-1].format(output=output, year=end))
    return "\n".join(table_cols)


def group1_crosstab_columns(start, end, output):
    template = "{output}_{year} model_output_annual_monthly,"
    ct_cols = [template.format(output=output, year=y)
               for y in range(start, end)]
    ct_cols.append(template[:-1].format(output=output, year=end))
    return "\n".join(ct_cols)


def group2_crosstab_columns(start, end, output):
    template = "{output}_{year} model_output_zonal_annual_monthly,"
    ct_cols = [template.format(output=output, year=y)
               for y in range(start, end)]
    ct_cols.append(template[:-1].format(output=output, year=end))
    return "\n".join(ct_cols)
#####################################

def group1_create_pivot(schema, output, monthly_table, annual_table, pivot_table_name, year_start=1958, year_end=2019):
    """Create pivot table for "group 1" outputs combining annual and monthly tables

    Args:
        schema (str): postgresql schema name
        output (str): model output name
        monthly_table (str): existing table with monthly data
        annual_table (str): existing table with annual data
        pivot_table_name (str): name of table to be created
        year_start (int): starting year of data
        year_end (int): ending year of data
    """

    GROUP1_TEMPLATE= """ 
SET search_path="{schema}", public;

CREATE TEMP TABLE aaa AS

SELECT a.sampleid as sampleid, a.year as year, ("annual_{output}","monthly_{output}")::model_output_annual_monthly as "{output}" FROM
(SELECT sampleid, year, array_agg("{output}" ORDER BY month ASC) as "monthly_{output}"
FROM "{schema}"."{monthly_table}"
GROUP BY sampleid,year) a
INNER JOIN
(SELECT sampleid, year, "{output}" as "annual_{output}"
    FROM "{schema}"."{annual_table}"
    ) b
ON a.sampleid=b.sampleid AND a.year = b.year
ORDER BY sampleid, year;

DROP TABLE IF EXISTS "{schema}"."{pivot_table_name}";
CREATE TABLE "{schema}"."{pivot_table_name}" AS
	SELECT ct.sampleid,
       {pivot_table_columns}
FROM crosstab('SELECT sampleid, year, "{output}"
        FROM aaa
        ORDER BY sampleid'::text, 
        '{sel_years}'::text) ct(sampleid bigint,  {crosstab_columns});

DROP TABLE aaa;

ALTER TABLE "{schema}"."{pivot_table_name}" ADD PRIMARY KEY (sampleid);

-- DROP monthly/annual tables
"""

    sel_years = select_years(year_start, year_end)
    ptbl_cols = pivot_table_columns(year_start, year_end, output)
    ct_cols = group1_crosstab_columns(year_start, year_end, output)

    sql = GROUP1_TEMPLATE.format(schema=schema, output=output, monthly_table=monthly_table, annual_table=annual_table, pivot_table_name=pivot_table_name,
    pivot_table_columns=ptbl_cols, sel_years=sel_years, crosstab_columns=ct_cols)

    return sql


def group1_create_yearly_views(schema, pivot_table_name, year_start=1958, year_end=2019):
    """Create yearly views for pivot tables created by group1_create_pivot

    Args:
        schema (str): schema of postgres database
        pivot_table_name (str): name of pivot table being referenced
        year_start (int, optional): Start year of data. Defaults to 1958.
        year_end (int, optional): End year of data. Defaults to 2019.
    """    

    # extract metadata from pivot_table_name
    output, hunit, model, resolution, _ = pivot_table_name.split('_')

    # account for dam naming variation
    hunit_table = ""
    if hunit == "reservoirdam":
        hunit_table = "grandv13hydrostn30_dam_{}".format(resolution)
    else:
        hunit_table = "hydrostn30_{}_{}".format(hunit, resolution)

    VIEW_TEMPLATE="""
CREATE OR REPLACE VIEW "{view_name}" AS
SELECT sampleid, 
        {unpack_columns}
        hstn.*
FROM "{schema}"."{pivot_table_name}"
INNER JOIN {hunit_table} hstn on sampleid=hstn.id
ORDER BY sampleid;
"""
    def _unpack_columns(output, year):
        annual_template = "(\"{output}_{year}\").annual as \"{output}_{year}_annual\","
        monthly_template = "(\"{output}_{year}\").monthly[{month_num}] as \"{output}_{year}_{month_num_zeropad}\","

        cols = [annual_template.format(output=output, year=year),]
        for i in range(1,13):
            month_col = monthly_template.format(output=output, year=year, month_num=i, month_num_zeropad=str(i).zfill(2))
            cols.append(month_col)
        
        return "\n".join(cols)
    
    sql = ['SET search_path="{}", public;'.format(schema),]
    view_names_full = []
    for y in range(year_start, year_end + 1):
        view_name = pivot_table_name.replace('pivot',str(y))
        view_names_full.append('"{}"."{}"'.format(schema,view_name))
        
        unp_cols = _unpack_columns(output, y)

        view_sql = VIEW_TEMPLATE.format(schema=schema, view_name=view_name, unpack_columns=unp_cols, 
        pivot_table_name=pivot_table_name, hunit_table=hunit_table)
        
        sql.append(view_sql)
    
    return "\n".join(sql), view_names_full


def group2_create_pivot(schema, output, monthly_table, annual_table, pivot_table_name, year_start=1958, year_end=2019):
    """Create pivot table for "group 2" outputs combining annual and monthly tables

    Args:
        schema (str): postgresql schema name
        output (str): model output name
        monthly_table (str): existing table with monthly data
        annual_table (str): existing table with annual data
        pivot_table_name (str): name of table to be created
        year_start (int): starting year of data
        year_end (int): ending year of data
    """

    GROUP2_TEMPLATE="""
SET search_path="{schema}", public;

CREATE TEMP TABLE bbb AS
SELECT a.sampleid as sampleid, a.year as year, (annual_zonalmean, annual_zonalmin, annual_zonalmax, monthly_zonalmean,
                                                monthly_zonalmin, monthly_zonalmax)::model_output_zonal_annual_monthly as zonal_output FROM
(SELECT sampleid, year, array_agg(zonalmean ORDER BY month ASC) as monthly_zonalmean,
        array_agg(zonalmin ORDER BY month ASC) as monthly_zonalmin,
        array_agg(zonalmax ORDER BY month ASC) as monthly_zonalmax
FROM "{schema}"."{monthly_table}"
GROUP BY sampleid,year) a
INNER JOIN
(SELECT sampleid, year, zonalmean as annual_zonalmean, zonalmin as annual_zonalmin, zonalmax as annual_zonalmax
    FROM "{schema}"."{annual_table}"
    ) b
ON a.sampleid=b.sampleid AND a.year = b.year
ORDER BY sampleid, year;

DROP TABLE IF EXISTS "{schema}"."{pivot_table_name}";
CREATE TABLE "{schema}"."{pivot_table_name}" AS
	SELECT ct.sampleid,
       {pivot_table_columns}
FROM crosstab('SELECT sampleid, year, zonal_output
        FROM bbb
        ORDER BY sampleid'::text, 
'{sel_years}'::text) ct(sampleid bigint, {crosstab_columns});

DROP TABLE bbb;

ALTER TABLE "{schema}"."{pivot_table_name}" ADD PRIMARY KEY (sampleid);

-- DROP monthly/annual tables
"""
    sel_years = select_years(year_start, year_end)
    ptbl_cols = pivot_table_columns(year_start, year_end, output)
    ct_cols = group2_crosstab_columns(year_start, year_end, output)

    sql = GROUP2_TEMPLATE.format(schema=schema, output=output, monthly_table=monthly_table, annual_table=annual_table, pivot_table_name=pivot_table_name,
    pivot_table_columns=ptbl_cols, sel_years=sel_years, crosstab_columns=ct_cols)

    return sql


def group2_create_yearly_views(schema, pivot_table_name, year_start=1958, year_end=2019):
    """Create yearly views for pivot tables created by group2_create_pivot

    Args:
        schema (str): schema of postgres database
        pivot_table_name (str): name of pivot table being referenced
        year_start (int, optional): Start year of data. Defaults to 1958.
        year_end (int, optional): End year of data. Defaults to 2019.
    """    

    # extract metadata from pivot_table_name
    output, hunit, model, resolution, _ = pivot_table_name.split('_')

    # account for dam naming variation
    hunit_table = ""
    if hunit in ('state','country'):
        hunit_table = "faogaul_{}_{}".format(hunit,resolution)
    else:
        hunit_table = "hydrostn30_{}_{}".format(hunit, resolution)

    VIEW_TEMPLATE="""
CREATE OR REPLACE VIEW "{view_name}" AS
SELECT sampleid, 
        {unpack_columns}
        hstn.*
FROM "{schema}"."{pivot_table_name}"
INNER JOIN {hunit_table} hstn on sampleid=hstn.id
ORDER BY sampleid;
"""
    def _unpack_columns(output, year):
        zonal_aggregations = ['zonalmean', 'zonalmin', 'zonalmax']

        annual_template = "(\"{output}_{year}\").annual_{zonal_agg} as \"{output}_{year}_annual_{zonal_agg}\","
        monthly_template = "(\"{output}_{year}\").monthly_{zonal_agg}[{month_num}] as \"{output}_{year}_{month_num_zeropad}_{zonal_agg}\","

        cols = []
        for z in zonal_aggregations:
            cols.append(annual_template.format(output=output, year=year, zonal_agg=z))
            for i in range(1,13):
                month_col = monthly_template.format(output=output, year=year, month_num=i, month_num_zeropad=str(i).zfill(2), zonal_agg=z)
                cols.append(month_col)
        
        return "\n".join(cols)
    
    sql = ['SET search_path="{}", public;'.format(schema),]
    view_names_full = []
    for y in range(year_start, year_end + 1):
        view_name = pivot_table_name.replace('pivot',str(y))
        view_names_full.append('"{}"."{}"'.format(schema,view_name))
        
        unp_cols = _unpack_columns(output, y)

        view_sql = VIEW_TEMPLATE.format(schema=schema, view_name=view_name, unpack_columns=unp_cols, 
        pivot_table_name=pivot_table_name, hunit_table=hunit_table)
        
        sql.append(view_sql)
    
    return "\n".join(sql), view_names_full

#### TESTS (well more like demos) ####
def _group1_create_pivot_test():
    output= "discharge"
    schema = "brazil"
    monthly_table = "discharge_confluence_monthly_terra+wbmpr_01min"
    annual_table = "discharge_confluence_annual_terra+wbmpr_01min"
    pivot_table_name = "discharge_confluence_terra+wbmpr_01min_pivot"

    sql = group1_create_pivot(schema, output, monthly_table, annual_table, pivot_table_name, 1958, 2019)

    with open("/tmp/pivot_group1.sql", 'w') as f:
        f.write(sql)


def _group2_create_pivot_test():
    output= "evapotranspiration"
    schema = "brazil"
    monthly_table = "evapotranspiration_subbasin_monthly_terra+wbm04_01min"
    annual_table = "evapotranspiration_subbasin_annual_terra+wbm04_01min"
    pivot_table_name = "evapotranspiration_subbasin_terra+wbm04_01min_pivot"

    sql = group2_create_pivot(schema, output, monthly_table, annual_table, pivot_table_name, 1958, 2019)

    with open("/tmp/pivot_group2.sql", 'w') as f:
        f.write(sql)


def _group1_create_yearly_views_test():
    schema = "brazil"
    pivot_table_name = "discharge_confluence_terra+wbmpr_01min_pivot"

    sql,_ = group1_create_yearly_views(schema, pivot_table_name, 1958, 2019)
    with open("/tmp/pivot_group1_views.sql", 'w') as f:
        f.write(sql)

def _group2_create_yearly_views_test():
    schema = "brazil"
    pivot_table_name = "evapotranspiration_subbasin_terra+wbm04_01min_pivot"

    sql,_ = group2_create_yearly_views(schema, pivot_table_name, 1958, 2019)
    with open("/tmp/pivot_group2_views.sql", 'w') as f:
        f.write(sql)

######################################


if __name__ == '__main__':
    _group1_create_pivot_test()
    _group2_create_pivot_test()
    _group1_create_yearly_views_test()
    _group2_create_yearly_views_test()