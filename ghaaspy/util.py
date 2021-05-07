"""Utility Functions"""

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
    if re.match(r'^"?((\w|-)+|\"(\w|-)+\")"?\."?(\w|-|\+)+"?$', table_names[0]):
        schema = table_names[0].split('.')[0].replace('"','')
        tables_clean = [_remove_embedded_quotes(t.split('.')[1]) for t in table_names]
        return schema, tables_clean
    else:
        return None, [_remove_embedded_quotes(t) for t in table_names]
