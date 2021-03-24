from geo.Geoserver import Geoserver

def connect_geoserver(geoserver_url, user, password):
    return Geoserver(geoserver_url, user, password)

def publish_geoserver_sqlview(geo, view_name, store_name, workspace, geography=False):
    sql = 'SELECT * FROM {}'.format(view_name)

    # handle faogaul_country / state -9999 admin null rows
    if 'faogaul_country' in view_name or 'faogaul_state' in view_name:
        sql += (' where gridvalue != -9999')
    
    name = view_name.split('.')[1].replace('+','-').replace('"','')

    # geography tables PK distinction
    key_col = 'sampleid'
    if geography: 
        key_col = 'id'

    geo.publish_featurestore_sqlview(name=name, store_name=store_name, sql=sql, key_column=key_col, workspace=workspace)
    print(name)

def publish_geoserver_sqlview_batch(geo, views_list, store_name, workspace, geography=False):
    for v in views_list:
        publish_geoserver_sqlview(geo, v, store_name, workspace, geography=geography)
