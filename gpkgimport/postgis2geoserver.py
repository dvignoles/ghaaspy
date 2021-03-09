from .ghaasgpkg.gpkg import sanitize_path, file_to_list
from .ghaasgpkg.geoserver import publish_geoserver_sqlview_batch, connect_geoserver
import argparse
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Publish postgis tables/views as 'sql views' on a \
        geoserver instance using a simple 'SELECT * FROM _'. You will need to first create a geoserver store \
        for the postgis database.schema you are referencing")

    parser.add_argument('viewnames_file', type=Path,
                        help="file containing list of existing postgres views/table names")

    parser.add_argument('geoserver_url', help="url of geoserver")
    parser.add_argument('geoserver_user')
    parser.add_argument('geoserver_password')
    parser.add_argument('geoserver_store', help='geoserver postgis store where to connect sql views with')
    parser.add_argument('geoserver_workspace', help='existing geoserver workspace to publish to')
    
    args = parser.parse_args()
    
    geo = connect_geoserver(args.geoserver_url, username=args.geoserver_user, password=args.geoserver_password)

    views = file_to_list(args.viewnames_file)

    publish_geoserver_sqlview_batch(geo, views, args.geoserver_store, args.geoserver_workspace)


if __name__ == '__main__':
    main()
    