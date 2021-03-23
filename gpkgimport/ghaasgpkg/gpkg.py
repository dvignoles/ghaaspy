"""Import ghaas geopackage to postgis database
"""

from pathlib import Path
import subprocess as sp
import shlex

from .sqlgen import group1_create_pivot, group2_create_pivot, GROUP1, GROUP2, group1_create_yearly_views, group2_create_yearly_views
import itertools 


def read_constants():
    """Read in constants from .txt files

    Returns:
        tuple, tuple, dict
    """
    this_dir = Path(__file__).parent.resolve()
    with open(this_dir.joinpath('ghaas_modeloutputs.txt'), 'r') as mf:
        MODEL_OUTPUTS = tuple(mf.read().splitlines())
    with open(this_dir.joinpath('ghaas_spatialunits.txt'), 'r') as sf:
        SPATIAL_UNITS = tuple(sf.read().splitlines())
    with open(this_dir.joinpath('ghaas_model_shortnames.txt'), 'r') as msnf:
        MODEL_SHORTNAMES_raw = [n.split('=') for n in msnf.read().splitlines()]
        MODEL_SHORTNAMES = {k: v for k, v in MODEL_SHORTNAMES_raw}
    return MODEL_OUTPUTS, SPATIAL_UNITS, MODEL_SHORTNAMES


MODEL_OUTPUTS, SPATIAL_UNITS, MODEL_SHORTNAMES = read_constants()


def sanitize_path(p):
    """Make arbitrary path object into absolute path

    Args:
        p (Path): Any Path

    Returns:
        Path: Absolute Path object
    """
    return p.expanduser().resolve()


def list_to_file(mylist, file_out):
    """Write a list of 'names' to a file, one name per line

    Args:
        mylist (list): list of name strings to
        file_out (Path): output file to write to
    """
    file_out_path = sanitize_path(file_out)
    with open(file_out_path,'w') as f:
        for p in mylist:
            f.write(p)
            f.write('\n')


def file_to_list(file_in):
    with open(file_in, 'r') as f:
        tables_raw = f.readlines()
        tables = [x.strip() for x in tables_raw] 
        return tables


def extract_tables(gpkg):
    """Extract tablenames from geopackage without indexes or metadata tables.

    Args:
        gpkg (Path): Path to geopackages

    Yields:
        Generator: Generator of table names
    """
    import sqlite3
    conn = sqlite3.connect(gpkg)                                
    cursor = conn.cursor()

    res = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    for name_t in res:
        name= name_t[0]
        if not name.startswith('rtree') and not name.startswith('gpkg') and not name.startswith('sqlite'):
            yield name


def extract_gpkg_meta(gpkg):
    """Extract information from geopackage filepath and filename

    Args:
        gpkg (Path): geopackage file

    Returns:
        dict: metadata from filename
    """
    gpkg_info = [component.lower() for component in gpkg.name.split('_')]
    tables = extract_tables(gpkg)

    # Geography only geopackage (no model output)
    if gpkg_info[1] == 'geography':
        meta = dict(
            is_output=False,
            geography=gpkg_info[0],
            resolution=gpkg_info[2].split('.')[0],
            tables=tables
        )
        return meta
    else:
        # Model output geopackage
        model_pts = gpkg_info[1].split('+')

        if len(model_pts) == 2:
            model_pt1, model_pt2 = model_pts
            model_short = '+'.join([MODEL_SHORTNAMES[model_pt1],
                                    MODEL_SHORTNAMES[model_pt2]])
        
        if len(model_pts) == 3:
            model_pt1, model_pt2, model_pt3 = model_pts
            model_short = '+'.join([MODEL_SHORTNAMES[model_pt1],
                                    MODEL_SHORTNAMES[model_pt2],
                                    MODEL_SHORTNAMES[model_pt3]])

        meta = dict(
            is_output=True,
            geography=gpkg_info[0],
            model_short=model_short,
            resolution=gpkg_info[2].split('.')[0], 
            tables=tables
        )
        return meta


def _import_gpkg(pg_con, gpkg, table_name, target_gpkg_table):
    """Generate ogr2ogr command string

    Args:
        pg_con (str): gdal postgres driver connection string, see https://gdal.org/drivers/vector/pg.html
        gpkg (Path): geopackage file
        table_name (str): name of table to be created in postgres
        target_gpkg_table (str): name of table in geopackage to import

    Returns:
        str: ogr2ogr command string
    """

    template = 'ogr2ogr -overwrite -f "PostgreSQL" PG:"{pg_con}" \
        -lco OVERWRITE=YES \
        --config PG_USE_COPY YES \
        -nlt PROMOTE_TO_MULTI \
        -nln {table_name} \
        {gpkg} {target_gpkg_table}'
    cmd = template.format(pg_con=pg_con, table_name=table_name,
                          gpkg=gpkg, target_gpkg_table=target_gpkg_table)

    return cmd


def group_geography_vs_model(table_names):
    """Separate list of tablenames into two separate lists: models outputs vs geography tables

    Args:
        table_names (list): list of table names
    """
    geography = []
    model_out = []

    for t in table_names:
        if any(map(t.lower().__contains__, ['hydrostn','faogaul'])):
            geography.append(t)
        else:
            model_out.append(t)
    
    return geography, model_out


def import_gpkg(pg_con, gpkg):
    """Execute ogr2ogr commands importing all tables from geopackage with renamed tables

    Args:
        pg_con (str): gdal postgres driver connection string, see https://gdal.org/drivers/vector/pg.html
        gpkg (Path): geopackage file

    Returns:
        list: list of newly created/replaced postgres table names in schema.table form
    """

    gpkg_meta = extract_gpkg_meta(gpkg)
    cmds = []
    postgres_tables = []

    if gpkg_meta['is_output']:

        # model output geopackages can/do contain embedded geography tables as well
        geography_tables, model_tables = group_geography_vs_model(gpkg_meta['tables'])
        for mo in model_tables:
            pg_table_name = '{}."{}_{}_{}"'.format(
                gpkg_meta['geography'], mo.lower(), gpkg_meta['model_short'], gpkg_meta['resolution'])
            postgres_tables.append(pg_table_name)
            cmd = _import_gpkg(pg_con, gpkg, pg_table_name, mo)
            cmds.append(cmd)
        for geo in geography_tables:
            pg_table_name = '{}."{}_{}"'.format(
                gpkg_meta['geography'], geo.lower(), gpkg_meta['resolution'])
            postgres_tables.append(pg_table_name)
            cmd = _import_gpkg(pg_con, gpkg, pg_table_name, geo)
            cmds.append(cmd)
    else:
        for su in gpkg_meta['tables']:
            pg_table_name = '{}."{}_{}"'.format(
                gpkg_meta['geography'], su.lower(), gpkg_meta['resolution'])
            postgres_tables.append(pg_table_name)
            cmd = _import_gpkg(pg_con, gpkg, pg_table_name, su)
            cmds.append(cmd)

    for cmd, pg in zip(cmds, postgres_tables):
        sp.run(shlex.split(cmd))  # split preserving quoted strings
        print(pg)

    return postgres_tables, gpkg_meta




def clean_tablenames(table_names):
    """Deal with table names with/without embedded schema names. Assumes lists of tables will be self consistent.

    Args:
        table_names ([type]): list of table names
    
    Returns:
        schema (str): unquoted name of embedded schema or None
        table_names (list of str): table_names w/out embedded schemas

    """
    import re 

    def _remove_embedded_quotes(t):
        if t.startswith('"'):
            t = t[1:]
        if t.endswith('"'):
            t = t[:-1]
        return t

    # check first table name for schema
    if re.match(r'^"?(\w+|\"\w+\")"?\."?(\w|-)+"?$', table_names[0]):
        schema = table_names[0].split('.')[0].replace('"','')
        tables_clean = [_remove_embedded_quotes(t.split('.')[1]) for t in table_names]
        return schema, tables_clean
    else:
        return None, [_remove_embedded_quotes(t) for t in table_names]

def group_annual_monthly(table_names):
    """Convert a list of table names to a dictionary grouping together annual and monthly tables. Keys are
    the table names (lowercase) without the embedded annual/monthly/daily.

    Args:
        table_names (dict): dictionary of tablenames
    """

    def group_temporal(x):
        
        # remove temporal quanitifier from table name
        x_short = x.lower().replace('annual','')
        x_short = x_short.replace('monthly','')
        x_short = x_short.replace('daily','')
        x_short = x_short.replace('__','_')
        if x_short.endswith('_'):
            x_short = x_short[:-1]

        return x_short

    annual_monthly = dict()
    temporal_grouped = itertools.groupby(table_names, group_temporal)
    for key, group in temporal_grouped:
        if key not in annual_monthly:
            annual_monthly[key] = set(group)
        else: 
            annual_monthly[key] = annual_monthly[key].union(set(group))
    return annual_monthly

def create_pivot_tables(table_names, output_file):
    """Write sql to file generating pivot tables and accompanying yearly views for a list of postgres tables generated through import_gpkg

    Args:
        table_names (list): postgres table names prefexed with schema ie schema."my-table_name"
        output_file (Path): output file to write sql to

    Returns:
        pivot_tablenames, view_names_all: lists of tables/views generated by function
    """
    
    schema, tables_names_short = clean_tablenames(table_names)
    # group together annual/monthly table pairs
    annual_monthly = group_annual_monthly(table_names)

    # lists of all pivot tables and views created
    pivot_tablenames = []
    view_names_all = []

    with open(output_file, 'w') as f:
        for key, component_tables in annual_monthly.items(): 
            pivot_tablename = key+'_pivot' 
            pivot_tablenames.append(pivot_tablename)
            # account for list ordering
            annual_pos = 0
            monthly_pos = 1
            if 'monthly' in component_tables[0]:
                annual_pos = 1
                monthly_pos = 0

            # extract schema and output name 
            output = key.split('_')[0]

            table_sql = ""
            view_sql = ""

            # call appropriate sql gen function for group1/group2 outputs
            if output in GROUP1['outputs']:
                table_sql = group1_create_pivot(schema, output, component_tables[monthly_pos], component_tables[annual_pos], pivot_tablename)
                view_sql,view_names = group1_create_yearly_views(schema, pivot_tablename)
                view_names_all += view_names
            else:
                table_sql = group2_create_pivot(schema, output, component_tables[monthly_pos], component_tables[annual_pos], pivot_tablename)
                view_sql,view_names = group2_create_yearly_views(schema, pivot_tablename)
                view_names_all += view_names

            f.write(table_sql)
            f.write(view_sql)

    return pivot_tablenames, view_names_all
