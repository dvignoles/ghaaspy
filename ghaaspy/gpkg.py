"""Geopackage manipulation and import to PostGIS"""

from pathlib import Path
import subprocess as sp
import shlex
import pkg_resources

from .util import group_geography_vs_model


def read_constants():
    """Read in constants from .txt files

    Returns:
        tuple, tuple, dict
    """

    model_outputs_file = pkg_resources.resource_stream(__name__, "package_data/ghaas_modeloutputs.txt")
    spatialunits_file = pkg_resources.resource_stream(__name__, "package_data/ghaas_spatialunits.txt")
    model_shortnames_file = pkg_resources.resource_stream(__name__, "package_data/ghaas_model_shortnames.txt")

    model_outputs_file.read().decode().splitlines()

    MODEL_OUTPUTS = tuple(model_outputs_file.read().decode().splitlines())
    SPATIAL_UNITS = tuple(spatialunits_file.read().decode().splitlines())

    MODEL_SHORTNAMES_raw = [n.split('=') for n in    model_shortnames_file.read().decode().splitlines()]
    MODEL_SHORTNAMES = {k: v for k, v in MODEL_SHORTNAMES_raw}

    model_outputs_file.close()
    spatialunits_file.close()
    model_shortnames_file.close()

    return MODEL_OUTPUTS, SPATIAL_UNITS, MODEL_SHORTNAMES


MODEL_OUTPUTS, SPATIAL_UNITS, MODEL_SHORTNAMES = read_constants()


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


def _import_gpkg(pg_con, gpkg, table_name, target_gpkg_table, update=False):
    """Generate ogr2ogr command string

    Args:
        pg_con (str): gdal postgres driver connection string, see https://gdal.org/drivers/vector/pg.html
        gpkg (Path): geopackage file
        table_name (str): name of table to be created in postgres
        target_gpkg_table (str): name of table in geopackage to import
        update (bool, optional): If True, will try to truncate then append to table rather than overwriting by default

    Returns:
        str: ogr2ogr command string
    """

    template_create = 'ogr2ogr -overwrite -f "PostgreSQL" PG:"{pg_con}" \
        -lco OVERWRITE=YES \
        --config PG_USE_COPY YES \
        -nlt PROMOTE_TO_MULTI \
        -nln {table_name} \
        {gpkg} {target_gpkg_table}'

    template_update = 'ogr2ogr -append -f "PostgreSQL" PG:"{pg_con}" \
        --config PG_USE_COPY YES \
        --config OGR_TRUNCATE YES \
        -nlt PROMOTE_TO_MULTI \
        -nln {table_name} \
        {gpkg} {target_gpkg_table}'

    if update:
        cmd = template_update.format(pg_con=pg_con, table_name=table_name,
                            gpkg=gpkg, target_gpkg_table=target_gpkg_table)
        
        return cmd
    else:
        cmd = template_create.format(pg_con=pg_con, table_name=table_name,
                            gpkg=gpkg, target_gpkg_table=target_gpkg_table)

        return cmd


def import_gpkg(pg_con, gpkg, update=False, include_embedded_geography_tables=False):
    """Execute ogr2ogr commands importing all tables from geopackage with renamed tables

    Args:
        pg_con (str): gdal postgres driver connection string, see https://gdal.org/drivers/vector/pg.html
        gpkg (Path): geopackage file
        update (bool, optional): try to trunacate then append to table, rather than overwriting by default
        include_embedded_geography_tables (bool, optional): In the case of a model output geopackage, import embedded geography tables (hydrostn, faogaul..)
            as well as model tables. There are separate geopackages with just the geography, these are more likely to be up to date. 

    Returns:
        list: list of newly created/replaced postgres table names in schema.table form
    """

    gpkg_meta = extract_gpkg_meta(gpkg)
    cmds = []
    postgres_tables = []

    def _clean_pg_tablename(t):
        # for some reason hyphens show up as underscores in postgres after the ogr2ogr import
        # No idea why, just going to remove at least for now
        return t.replace('-','')

    if gpkg_meta['is_output']:

        # model output geopackages can/do contain embedded geography tables as well
        # we don't always want to import these as they may be out of date
        geography_tables, model_tables = group_geography_vs_model(gpkg_meta['tables'])
        for mo in model_tables:
            schema = '"{}"'.format(gpkg_meta['geography'])

            table_name = _clean_pg_tablename('"{}_{}_{}"'.format(
                mo.lower(), gpkg_meta['model_short'], gpkg_meta['resolution']))
            
            pg_table_name = ".".join([schema, table_name])

            postgres_tables.append(pg_table_name)
            cmd = _import_gpkg(pg_con, gpkg, pg_table_name, mo, update=update)
            cmds.append(cmd)
        if include_embedded_geography_tables:
            for geo in geography_tables:
                pg_table_name = '{}."{}_{}"'.format(
                    gpkg_meta['geography'], geo.lower(), gpkg_meta['resolution'])
                postgres_tables.append(pg_table_name)
                cmd = _import_gpkg(pg_con, gpkg, pg_table_name, geo, update=update)
                cmds.append(cmd)
    else:
        for su in gpkg_meta['tables']:
            pg_table_name = '{}."{}_{}"'.format(
                gpkg_meta['geography'], su.lower(), gpkg_meta['resolution'])
            postgres_tables.append(pg_table_name)
            cmd = _import_gpkg(pg_con, gpkg, pg_table_name, su, update=update)
            cmds.append(cmd)

    for cmd, pg in zip(cmds, postgres_tables):
        sp.run(shlex.split(cmd))  # split preserving quoted strings
        print(pg)

    return postgres_tables, gpkg_meta



