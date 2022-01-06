from typing import MutableMapping
import pandas as pd
import numpy as np

WITHOUT_LOTS = 'WITHOUT_LOTS'

def soft_get(dct, field):
    if not isinstance(dct, MutableMapping):
        return pd.NA
    return dct.get(field,pd.NA)

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

def base_trasformation(df):
    return (
        df
        .drop(columns=['bids', 'awards', 'items'])
        # converting dates
        .assign(
            date = lambda d: pd.to_datetime(d.date, utc=True),
            dateModified = lambda d: pd.to_datetime(d.dateModified, utc=True)
        )
        # filter nan values 
        .query('~value.isna()')
        # procuring entity
        .assign(
            date = lambda d: pd.to_datetime(d.date, utc=True),
            dateModified = lambda d: pd.to_datetime(d.dateModified, utc=True)
        )
        .assign(
            procuringEntity_name = lambda d: d.procuringEntity.apply(lambda v: soft_get(v,'name')),
            procuringEntity_kind = lambda d: d.procuringEntity.apply(lambda v: soft_get(v,'kind')),
            procuringEntity_identifier = lambda d: d.procuringEntity.apply(lambda v: soft_get(v,'identifier')),
            procuringEntity_address = lambda d: d.procuringEntity.apply(lambda v: soft_get(v,'address')),
            procuringEntity_contactPoint = lambda d: d.procuringEntity.apply(lambda v: soft_get(v,'contactPoint')),
            num_lots = lambda d: d.lots.apply(lambda v: len(v) if isinstance(v, list) else 0),
        )
        .drop(columns=['procuringEntity'])
        .astype({
            'procuringEntity_name': 'string', 'procuringEntity_kind': 'category', 'num_lots': np.int8
        })
        .assign(
            procuringEntity_identifier_id = lambda d: d.procuringEntity_identifier.apply(lambda v: soft_get(v,'id')),
            procuringEntity_identifier_scheme = lambda d: d.procuringEntity_identifier.apply(lambda v: soft_get(v,'scheme')),
            procuringEntity_contactPoint_email = lambda d: d.procuringEntity_contactPoint.apply(lambda v: soft_get(v,'email')),
            procuringEntity_contactPoint_name = lambda d: d.procuringEntity_contactPoint.apply(lambda v: soft_get(v,'name')),
            procuringEntity_contactPoint_telephone = lambda d: d.procuringEntity_contactPoint.apply(lambda v: soft_get(v,'telephone')),
        )
        .astype({
            'procuringEntity_identifier_id': 'string', 'procuringEntity_identifier_scheme': 'category',
            'procuringEntity_contactPoint_email': 'string', 'procuringEntity_contactPoint_name': 'string',
            'procuringEntity_contactPoint_telephone': 'string',
        })
        .drop(columns=['procuringEntity_identifier', 'procuringEntity_contactPoint'])
    )

def transform_dataset(df):
    bids_df = (   
        subcollection_to_df(df, 'bids')
        .add_prefix('bids_')
        .assign(
            bids_date = lambda d: pd.to_datetime(d.bids_date, utc=True),
        )
        .astype({
            'bids_id': 'string', 'bids_parent_id': 'string', 'bids_status': 'category',
        })
    )

    bids_df = bids_df.merge(
        (
            subcollection_to_df(bids_df, 'bids_tenderers', 'bids_id')
            .add_prefix('tenderers_')
            .assign(
                bids_tenderers_identifier_id = lambda d: d.tenderers_identifier.apply(lambda v: soft_get(v,'id')),
                bids_tenderers_identifier_scheme = lambda d: d.tenderers_identifier.apply(lambda v: soft_get(v,'scheme')),
                bids_tenderers_contactPoint_email = lambda d: d.tenderers_contactPoint.apply(lambda v: soft_get(v,'email')),
                bids_tenderers_contactPoint_name = lambda d: d.tenderers_contactPoint.apply(lambda v: soft_get(v,'name')),
                bids_tenderers_contactPoint_telephone = lambda d: d.tenderers_contactPoint.apply(lambda v: soft_get(v,'telephone')),
            )
            .astype({
                'bids_tenderers_identifier_id': 'string', 'bids_tenderers_identifier_scheme': 'category',
                'bids_tenderers_contactPoint_email': 'string', 'bids_tenderers_contactPoint_name': 'string',
                'bids_tenderers_contactPoint_telephone': 'string', 'tenderers_name': 'string'
            })
            .rename(columns={'tenderers_name':'bids_tenderers_name'})
            [[
                'tenderers_parent_id', 'bids_tenderers_identifier_id', 'bids_tenderers_identifier_scheme', 'bids_tenderers_contactPoint_email',
                'bids_tenderers_contactPoint_name', 'bids_tenderers_contactPoint_telephone', 'bids_tenderers_name', 'tenderers_address' #'bids_tenderers_geo'

            ]]
        )
        , left_on='bids_id', right_on='tenderers_parent_id'
    )[[
        'bids_date',
        'bids_id',
        'bids_value',
        'bids_lotValues',
        'bids_status',
        'bids_parent_id',
        'bids_tenderers_name',                       
        'bids_tenderers_identifier_id',               
        'bids_tenderers_identifier_scheme',        
        'bids_tenderers_contactPoint_email',       
        'bids_tenderers_contactPoint_name',        
        'bids_tenderers_contactPoint_telephone',
        'tenderers_address'
    ]]

    return (
        # main data + lots
        (
            pd.concat([
                # records without lots
                (
                    base_trasformation(
                        df[df.lots.isna()]
                    )
                    .assign(
                        lots_id = lambda d: WITHOUT_LOTS,
                        value_amount = lambda d: d.value.apply(lambda v: soft_get(v,'amount')),
                        value_currency = lambda d: d.value.apply(lambda v: soft_get(v,'currency')),
                        value_valueAddedTaxIncluded = lambda d: d.value.apply(lambda v: soft_get(v,'valueAddedTaxIncluded')),
                    )
                ),
                # records with lots
                (
                    base_trasformation(
                        df[~df.lots.isna()]
                    )
                    .merge(
                        (
                            subcollection_to_df(df[~df.lots.isna()], 'lots')
                            .add_prefix('lots_')
                            .query('~lots_value.isna()')
                            .assign(
                                value_amount = lambda d: d.lots_value.apply(lambda v: soft_get(v,'amount')),
                                value_currency = lambda d: d.lots_value.apply(lambda v: soft_get(v,'currency')),
                                value_valueAddedTaxIncluded = lambda d: d.lots_value.apply(lambda v: soft_get(v,'valueAddedTaxIncluded')),
                            )
                            [[
                                'lots_id','lots_parent_id','lots_status', 'value_amount', 'value_currency', 'value_valueAddedTaxIncluded'     
                            ]]
                        ), 
                        left_on="id", 
                        right_on="lots_parent_id"
                    )
                )

            ])
            .fillna({'numberOfBids':0})
            # converting base types
            .astype({
                'id': 'string', 'tenderID': 'string', 'owner': 'category', 
                'procurementMethod': 'category', 'submissionMethod': 'category', 'status': 'category',
                'numberOfBids': np.int8, 'procurementMethodType': 'category', 'mainProcurementCategory': 'category',
                'lots_id': 'string', 'value_currency': 'category', 'value_valueAddedTaxIncluded': bool, 
                'lots_status': 'category'
            })
            .drop(columns=[
                'lots_parent_id', 'value', 'lots'
            ])
        )
        
        # bids with values + lots
        .merge(
            (
                pd.concat([
                    # nested bid values from lots
                    bids_df[~bids_df.bids_lotValues.isna()].merge(
                        (
                            subcollection_to_df(bids_df[~bids_df.bids_lotValues.isna()], 'bids_lotValues', parent_field_name='bids_id')
                            .add_prefix('bids_value_')
                            .assign(
                                bids_value_amount = lambda d: d.bids_value_value.apply(lambda v: soft_get(v,'amount')),
                                bids_value_currency = lambda d: d.bids_value_value.apply(lambda v: soft_get(v,'currency')),
                                bids_value_valueAddedTaxIncluded = lambda d: d.bids_value_value.apply(lambda v: soft_get(v,'valueAddedTaxIncluded')),
                            )
                                        
                        )
                        [['bids_value_relatedLot', 'bids_value_parent_id', 'bids_value_amount', 'bids_value_currency', 'bids_value_valueAddedTaxIncluded']],     
                        left_on="bids_id", right_on="bids_value_parent_id"
                    ),
                    # nested bid values without lots
                    (
                        bids_df
                        [
                            (bids_df.bids_lotValues.isna()) & (~bids_df.bids_value.isna())
                        ]
                        .assign(
                            bids_value_amount = lambda d: d.bids_value.apply(lambda v: soft_get(v,'amount')),
                            bids_value_currency = lambda d: d.bids_value.apply(lambda v: soft_get(v,'currency')),
                            bids_value_valueAddedTaxIncluded = lambda d: d.bids_value.apply(lambda v: soft_get(v,'valueAddedTaxIncluded')),
                            bids_value_relatedLot = lambda d: WITHOUT_LOTS
                        )
                    )

                ])
                .drop(columns=['bids_value_parent_id','bids_lotValues', 'bids_value'])
                .astype({
                        'bids_tenderers_identifier_id': 'string', 'bids_tenderers_identifier_scheme': 'category',
                        'bids_tenderers_contactPoint_email': 'string', 'bids_tenderers_contactPoint_name': 'string',
                        'bids_tenderers_contactPoint_telephone': 'string',
                })
            ),
            left_on=["id", 'lots_id'], 
            right_on=['bids_parent_id', 'bids_value_relatedLot'],
            how="left"
        )
        .drop(columns=['bids_parent_id', 'bids_value_relatedLot'])

        # awards
        .merge(
            (
                subcollection_to_df(df, 'awards')
                .add_prefix('awards_')
                [[
                    'awards_id', 'awards_date', 'awards_status', 'awards_bid_id', 'awards_parent_id', 'awards_lotID', 'awards_value'
                ]]
                .fillna(value={'awards_lotID':WITHOUT_LOTS})
                .assign(
                    awards_value_amount = lambda d: d.awards_value.apply(lambda v: soft_get(v,'amount')),
                    awards_value_currency = lambda d: d.awards_value.apply(lambda v: soft_get(v,'currency')),
                    awards_value_valueAddedTaxIncluded = lambda d: d.awards_value.apply(lambda v: soft_get(v,'valueAddedTaxIncluded')),
                    awards_date = lambda d: pd.to_datetime(d.awards_date, utc=True),
                )
                .drop(columns=['awards_value', 'awards_id'])
                .groupby(by=['awards_parent_id', 'awards_lotID', 'awards_bid_id'])
                .agg(min) # if multible awards per tender/lot/bid
                .reset_index()
                .astype({
                    'awards_value_currency': 'category', 'awards_value_valueAddedTaxIncluded': bool, 
                    'awards_status': 'category', 
                })
            ),
            left_on=['id', 'lots_id', 'bids_id'], 
            right_on=['awards_parent_id', 'awards_lotID', 'awards_bid_id'],
            how='left'
        )
        .drop(columns=['awards_parent_id', 'awards_bid_id', 'awards_lotID'])

        #items
        .merge(
            (
                subcollection_to_df(df, 'items')
                .add_prefix('items_')
                .fillna(value={'items_relatedLot':WITHOUT_LOTS})
                .assign(
                    items_code = lambda d: d.items_classification.apply(lambda v: soft_get(v,'id'))
                )
                .query('~items_code.isna()')
                [[
                    'items_parent_id', 'items_relatedLot', 'items_code'
                ]]
                .groupby(['items_parent_id','items_relatedLot'])
                .agg(lambda x: list(x)[0])
                .reset_index()
                .astype({
                    'items_code': 'category'
                })
            ),
            left_on=['id', 'lots_id'], 
            right_on=['items_parent_id', 'items_relatedLot'],
            how='left'
        )
        .drop(columns=['items_parent_id', 'items_relatedLot'])
        
        .astype({
            'id': 'string', 'lots_id': 'string', 'awards_value_valueAddedTaxIncluded': bool,
            'bids_id': 'string', 'bids_value_currency': 'category'
        })
    )