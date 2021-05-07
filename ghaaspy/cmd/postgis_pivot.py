
import argparse
from pathlib import Path

from ..pivot import create_pivot_annual_monthly_tables
from ..util import sanitize_path, list_to_file

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('tablenames_file', type=Path,
                        help="file containing list of imported geopackage postgres tables")
    parser.add_argument('output_file', type=Path,
                        help="file to output sql to")
    parser.add_argument('-p', '--pivot_names', type=Path, help="file to write created pivot table names to", required=False)
    parser.add_argument('-v', '--view_names', type=Path, help="file to write created views to", required=False)
    parser.add_argument('--start_year', type=int, help="starting year of data, default=1958",required=False)
    parser.add_argument('--end_year', type=int, help="end year of data, default=2019", required=False)

    args = parser.parse_args()

    table_names = sanitize_path(args.tablenames_file)
    output_file = sanitize_path(args.output_file)

    with open(table_names, 'r') as f:
        tables_raw = f.readlines()
        tables = [x.strip() for x in tables_raw] 

        if args.start_year and args.end_year:
            pivot_table_names, view_names = create_pivot_annual_monthly_tables(tables, output_file, year_start=args.start_year, year_end=args.end_year)
        elif args.start_year or args.end_year:
            raise("must provide --start_year and --end_year")
        else:
            pivot_table_names, view_names = create_pivot_annual_monthly_tables(tables, output_file)
    
        if args.pivot_names:
            list_to_file(pivot_table_names, args.pivot_names)
        
        if args.view_names:
            list_to_file(view_names, args.view_names)

if __name__ == '__main__':
    main()

