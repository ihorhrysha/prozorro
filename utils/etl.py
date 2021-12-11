import pandas as pd
import numpy as np

WITHOUT_LOTS = 'WITHOUT_LOTS'

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

def base_trasformation(df, get_location):
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
            procuringEntity_name = lambda d: d.procuringEntity.apply(lambda v: v['name']),
            procuringEntity_kind = lambda d: d.procuringEntity.apply(lambda v:  v.get('kind',pd.NA)),
            procuringEntity_identifier = lambda d: d.procuringEntity.apply(lambda v: v['identifier']),
            procuringEntity_address = lambda d: d.procuringEntity.apply(lambda v: v['address']),
            procuringEntity_contactPoint = lambda d: d.procuringEntity.apply(lambda v: v['contactPoint']),
            num_lots = lambda d: d.lots.apply(lambda v: len(v) if isinstance(v, list) else 0),
        )
        .drop(columns=['procuringEntity'])
        .astype({
            'procuringEntity_name': 'string', 'procuringEntity_kind': 'category', 'num_lots': np.int8
        })
        .assign(
            procuringEntity_identifier_id = lambda d: d.procuringEntity_identifier.apply(lambda v: v['id']),
            procuringEntity_identifier_scheme = lambda d: d.procuringEntity_identifier.apply(lambda v:  v['scheme']),
            procuringEntity_contactPoint_email = lambda d: d.procuringEntity_contactPoint.apply(lambda v:  v.get('email',pd.NA)),
            procuringEntity_contactPoint_name = lambda d: d.procuringEntity_contactPoint.apply(lambda v:  v['name']),
            procuringEntity_contactPoint_telephone = lambda d: d.procuringEntity_contactPoint.apply(lambda v:  v.get('telephone',pd.NA)),
        )
        .astype({
            'procuringEntity_identifier_id': 'string', 'procuringEntity_identifier_scheme': 'category',
            'procuringEntity_contactPoint_email': 'string', 'procuringEntity_contactPoint_name': 'string',
            'procuringEntity_contactPoint_telephone': 'string',
        })
        .drop(columns=['procuringEntity_identifier', 'procuringEntity_contactPoint'])
        .assign(
            procuringEntity_geo = lambda d: d.procuringEntity_address.apply(lambda address:get_location(address)),
        )
        .drop(columns=['procuringEntity_address'])
    )

def transform_dataset(df, classifier, get_location):
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
                bids_tenderers_identifier_id = lambda d: d.tenderers_identifier.apply(lambda v: v['id']),
                bids_tenderers_identifier_scheme = lambda d: d.tenderers_identifier.apply(lambda v:  v['scheme']),
                bids_tenderers_contactPoint_email = lambda d: d.tenderers_contactPoint.apply(lambda v:  v.get('email',pd.NA)),
                bids_tenderers_contactPoint_name = lambda d: d.tenderers_contactPoint.apply(lambda v:  v['name']),
                bids_tenderers_contactPoint_telephone = lambda d: d.tenderers_contactPoint.apply(lambda v:  v.get('telephone',pd.NA)),
            )
            .astype({
                'bids_tenderers_identifier_id': 'string', 'bids_tenderers_identifier_scheme': 'category',
                'bids_tenderers_contactPoint_email': 'string', 'bids_tenderers_contactPoint_name': 'string',
                'bids_tenderers_contactPoint_telephone': 'string', 'tenderers_name': 'string'
            })
            .rename(columns={'tenderers_name':'bids_tenderers_name'})
            .assign(
                bids_tenderers_geo = lambda d: d.tenderers_address.apply(lambda address:get_location(address)),
            )
            [[
                'tenderers_parent_id', 'bids_tenderers_identifier_id', 'bids_tenderers_identifier_scheme', 'bids_tenderers_contactPoint_email',
                'bids_tenderers_contactPoint_name', 'bids_tenderers_contactPoint_telephone', 'bids_tenderers_name',  'bids_tenderers_geo'

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
        'bids_tenderers_geo'
    ]]

    return (
        # main data + lots
        (
            pd.concat([
                # records without lots
                (
                    base_trasformation(
                        df[df.lots.isna()], 
                        get_location=get_location
                    )
                    .assign(
                        lots_id = lambda d: WITHOUT_LOTS,
                        value_amount = lambda d: d.value.apply(lambda v:  v['amount']),
                        value_currency = lambda d: d.value.apply(lambda v:  v['currency']),
                        value_valueAddedTaxIncluded = lambda d: d.value.apply(lambda v:  v['valueAddedTaxIncluded']),
                    )
                ),
                # records with lots
                (
                    base_trasformation(
                        df[~df.lots.isna()], 
                        get_location=get_location
                    )
                    .merge(
                        (
                            subcollection_to_df(df[~df.lots.isna()], 'lots').add_prefix('lots_')
                            .assign(
                                value_amount = lambda d: d.lots_value.apply(lambda v:  v['amount']),
                                value_currency = lambda d: d.lots_value.apply(lambda v:  v['currency']),
                                value_valueAddedTaxIncluded = lambda d: d.lots_value.apply(lambda v:  v['valueAddedTaxIncluded']),
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
                                bids_value_amount = lambda d: d.bids_value_value.apply(lambda v:  v['amount']),
                                bids_value_currency = lambda d: d.bids_value_value.apply(lambda v:  v['currency']),
                                bids_value_valueAddedTaxIncluded = lambda d: d.bids_value_value.apply(lambda v:  v['valueAddedTaxIncluded']),
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
                            bids_value_amount = lambda d: d.bids_value.apply(lambda v:  v['amount']),
                            bids_value_currency = lambda d: d.bids_value.apply(lambda v:  v['currency']),
                            bids_value_valueAddedTaxIncluded = lambda d: d.bids_value.apply(lambda v:  v['valueAddedTaxIncluded']),
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
            right_on=['bids_parent_id', 'bids_value_relatedLot']
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
                    awards_value_amount = lambda d: d.awards_value.apply(lambda v:  v['amount']),
                    awards_value_currency = lambda d: d.awards_value.apply(lambda v:  v['currency']),
                    awards_value_valueAddedTaxIncluded = lambda d: d.awards_value.apply(lambda v:  v['valueAddedTaxIncluded']),
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
                    items_code = lambda d: d.items_classification.apply(lambda v: v.get('id',''))
                )
                [[
                    'items_parent_id', 'items_relatedLot', 'items_code'
                ]]
                .groupby(['items_parent_id','items_relatedLot'])
                .agg(lambda x: list(x)[0])
                .reset_index()
                .assign(
                    items_classification_l1 = lambda d: d.items_code.apply(lambda code: classifier.get_level_category(code=code, level=1).code),
                    items_classification_l2 = lambda d: d.items_code.apply(lambda code: classifier.get_level_category(code=code, level=2).code),
                    items_classification_l3 = lambda d: d.items_code.apply(lambda code: classifier.get_level_category(code=code, level=3).code)
                )
                .astype({
                    'items_classification_l1': 'category', 'items_classification_l2': 'category', 
                    'items_classification_l3': 'category', 'items_code': 'category'
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