import pandas as pd
from itertools import islice, chain

def batch(iterable, batch_size=100):
    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, batch_size))
        if not chunk:
            return
        yield chunk

def mem_usg(df):
    return df.memory_usage(deep=True).sum()

# subcollection to df
def subcollection_to_df(df, field_name, parent_field_name='id'):
    sub = []

    for row in df.itertuples():
        field_inx=row._fields.index(field_name)
        collection = row[field_inx] if row[field_inx] == row[field_inx] else [] # supress pandas nans

        parent_idx = row._fields.index(parent_field_name)
        for itm in collection:
            sub.append({
                **itm,
                'parent_id': row[parent_idx]
                })
            
    sub_df = pd.DataFrame(sub)
    del sub
    return sub_df

def read_mongo(db, collection, query={}, fields=None, id_to_index=False, limit: int = 0):
    """ Read from Mongo and Store into DataFrame """

    # Make a query to the specific DB and Collection
    
    # filter fields
    _fields = None
    if fields is not None:
        _fields = fields if isinstance(fields, dict) else {field:1 for field in fields}

    cursor = db[collection].find(query, _fields)

    if limit:
        cursor.limit(limit)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor)).rename(columns={'_id':'id'})

    # Delete the _id
    if id_to_index:
        df.set_index('id')

    return df


def read_mongo_batch(db, collection, query={}, fields=None, id_to_index=False, batch_size: int = 1000):
    """ Read from Mongo and Store into DataFrame """

    # Make a query to the specific DB and Collection
    
    # filter fields
    _fields = None
    if fields is not None:
        _fields = fields if isinstance(fields, dict) else {field:1 for field in fields}

    cursor = db[collection].find(query, _fields)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor)).rename(columns={'_id':'id'})

    # Delete the _id
    if id_to_index:
        df.set_index('id')

    return df