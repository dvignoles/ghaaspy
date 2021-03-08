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


def extract_gpkg_meta(gpkg):
    """Extract information from geopackage filepath and filename

    Args:
        gpkg (Path): geopackage file

    Returns:
        dict: metadata from filename
    """
    gpkg_info = [component.lower() for component in gpkg.name.split('_')]

    # Geography only geopackage (no model output)
    if gpkg_info[1] == 'geography':
        meta = dict(
            is_output=False,
            geography=gpkg_info[0],
            resolution=gpkg_info[2].split('.')[0]
        )
        return meta
    else:
        # Model output geopackage
        model_pt1, model_pt2 = gpkg_info[1].split('+')
        model_short = '+'.join([MODEL_SHORTNAMES[model_pt1],
                                MODEL_SHORTNAMES[model_pt2]])
        meta = dict(
            is_output=True,
            geography=gpkg_info[0],
            model_short=model_short,
            resolution=gpkg_info[2].split('.')[0]
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
        --config PG_USE_COPY YES \
        -nlt PROMOTE_TO_MULTI \
        -nln {table_name} \
        {gpkg} {target_gpkg_table}'
    cmd = template.format(pg_con=pg_con, table_name=table_name,
                          gpkg=gpkg, target_gpkg_table=target_gpkg_table)

    return cmd


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
        for mo in MODEL_OUTPUTS:
            pg_table_name = '{}."{}_{}_{}"'.format(
                gpkg_meta['geography'], mo, gpkg_meta['model_short'], gpkg_meta['resolution'])
            postgres_tables.append(pg_table_name)
            cmd = _import_gpkg(pg_con, gpkg, pg_table_name, mo)
            cmds.append(cmd)
    else:
        for su in SPATIAL_UNITS:
            pg_table_name = '{}."{}_{}"'.format(
                gpkg_meta['geography'], su, gpkg_meta['resolution'])
            postgres_tables.append(pg_table_name)
            cmd = _import_gpkg(pg_con, gpkg, pg_table_name, su)
            cmds.append(cmd)

    for cmd, pg in zip(cmds, postgres_tables):
        sp.run(shlex.split(cmd))  # split preserving quoted strings
        print(pg)

    return postgres_tables, gpkg_meta


def _group_annual_monthly(table_names):
    """Convert a list of table names to a dictionary grouping together annual and monthly tables

    Args:
        table_names (dict): dictionary of tablenames
    """

    def group_temporal(x):
        parts = x.split('.')[1].split('_')
        exclude_temporal = [x for i,x in enumerate(parts) if i != 2]
        return '_'.join(exclude_temporal)

    annual_monthly = dict()
    temporal_grouped = itertools.groupby(table_names, group_temporal)
    for key, group in temporal_grouped:
        tables = list(map(lambda x: x.split('.')[1].replace('"',''), group))
        key_strip = key.replace('"','')
        if key not in annual_monthly:
            annual_monthly[key_strip] = tables
        else: 
            annual_monthly[key_strip] += tables
    return annual_monthly

def create_pivot_tables(table_names, output_file):
    """Write sql to file generating pivot tables and accompanying yearly views for a list of postgres tables generated through import_gpkg

    Args:
        table_names (list): postgres table names prefexed with schema ie schema."my-table_name"
        output_file (Path): output file to write sql to

    Returns:
        pivot_tablenames, view_names_all: lists of tables/views generated by function
    """
    
    # group together annual/monthly table pairs
    annual_monthly = _group_annual_monthly(table_names)

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
            schema = table_names[0].split('.')[0]
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
