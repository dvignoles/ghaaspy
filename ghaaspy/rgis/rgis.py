import os

if 'GHAASDIR' in os.environ:
    GHAASDIR = os.environ['GHAASDIR']
else:
    GHAADIR = '/usr/local/share/ghaas'

GHAASBIN = os.path.join(GHAADIR,'bin')
GHAASSCRIPTS = os.path.join(GHAASDIR, 'Scripts')
