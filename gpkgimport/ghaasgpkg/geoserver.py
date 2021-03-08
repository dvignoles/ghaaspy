from geo.Geoserver import Geoserver

def connect_geoserver(geoserver_url, geoserver_user, geoserver_password):
    return Geoserver(geoserver_url, geoserver_user, geoserver_password)

def publish_geoserver_sqlview(geo, view_name, store_name, workspace):
    sql = 'SELECT * FROM {}'.format(view_name)
    name = view_name.split('.')[1].replace('+','-').replace('"','')
    geo.publish_featurestore_sqlview(name=name, store_name=store_name, sql=sql, key_column='sampleid', workspace=workspace)
    print(name)

def publish_geoserver_sqlview_batch(geo, views_list, store_name, workspace):
    for v in views_list:
        publish_geoserver_sqlview(geo, v, store_name, workspace)