import argparse
from pathlib import Path

from ..gpkg import import_gpkg
from ..util import sanitize_path,list_to_file
from ..postgres import PostgresDB

def main():
    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--pg_con', help="postgres gdal driver connection string, \"dbname='databasename' host='addr' port='5432' user='x' password='y'\"")
    group.add_argument('--pgpass_id', help="identifying substring of of .pgpass entry. Could be a database name, host:port, etc.")

    parser.add_argument('--pgpass_file', type=Path, help="location of .pgpass. Defaults to ~/.pgpass", required=False)

    parser.add_argument('gpkg', type=Path,
                        help="GHAAS geopackage file or filepath")
    parser.add_argument('-t', '--tablenames', type=Path, help="file to write created table names to", required=False)
    parser.add_argument('--update', action='store_true', help="update table (truncate , then append) instead of overwriting existing tables")
    parser.add_argument('--include_geography', action='store_true', help="if importing a modeloutput geopackage, import embedded geography tables as well")
    args = parser.parse_args()

    if args.pg_con:
        db = PostgresDB.from_gdal_string(args.pg_con)
    elif args.pgpass_id:
        if args.pgpass_file:
            pgpass = args.pgpass_file.resolve(strict=True)
            db = PostgresDB.from_pgpass(args.pgpass_id,pgpass=pgpass)
        else:
            db = PostgresDB.from_pgpass(args.pgpass_id)

    pg_con = db.get_gdal_string()
    gpkg = sanitize_path(args.gpkg)

    if args.update:
        if args.include_geography:
            postgres_tables, gpkg_meta = import_gpkg(pg_con, gpkg, update=True, include_embedded_geography_tables=True)
        else:
            postgres_tables, gpkg_meta = import_gpkg(pg_con, gpkg, update=True, include_embedded_geography_tables=False)
    else:
        if args.include_geography:
            postgres_tables, gpkg_meta = import_gpkg(pg_con, gpkg, update=False, include_embedded_geography_tables=True)
        else: 
            postgres_tables, gpkg_meta = import_gpkg(pg_con, gpkg, update=False, include_embedded_geography_tables=False)

    if args.tablenames:
        list_to_file(postgres_tables, args.tablenames)


if __name__ == '__main__':
    main()
