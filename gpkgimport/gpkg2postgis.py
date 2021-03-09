from .ghaasgpkg.gpkg import import_gpkg, create_pivot_tables, sanitize_path, list_to_file
import argparse
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "pg_con", help="postgres gdal driver connection string, \"dbname='databasename' host='addr' port='5432' user='x' password='y'\"")
    parser.add_argument('gpkg', type=Path,
                        help="GHAAS geopackage file or filepath")
    parser.add_argument('-t', '--tablenames', type=Path, help="file to write created table names to", required=False)
    args = parser.parse_args()

    pg_con = args.pg_con
    gpkg = sanitize_path(args.gpkg)
    postgres_tables, gpkg_meta = import_gpkg(pg_con, gpkg)

    if args.tablenames:
        list_to_file(postgres_tables, args.tablenames)


if __name__ == '__main__':
    main()