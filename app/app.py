
# https://betterprogramming.pub/build-a-search-engine-for-medium-stories-using-streamlit-and-elasticsearch-b6e717819448
# https://docs.streamlit.io/library/get-started/create-an-app

from typing import Dict
import pathlib
import os
import requests

from elasticsearch.client import Elasticsearch
import streamlit as st
import pandas as pd

from es import ElasticsearchManager
from geolocation import CachedGeolocator, region_fixer, locality_fixer
from config import ES_URI, ES_INDEX

## BACKEND

@st.experimental_singleton
def get_locator():
    return CachedGeolocator(verbose=False)

@st.experimental_singleton
def get_es_manager():
    return ElasticsearchManager(
        client=Elasticsearch(hosts=ES_URI)
    )

@st.cache
def get_tender_info(uid:str) -> Dict:
    response = requests.get(url=f'https://public.api.openprocurement.org/api/2.5/tenders/{uid}')
    response.raise_for_status()

    return response.json()

@st.cache
def get_geopoint(locality, region, countryName, **kw):
    locator = get_locator()
    geo_query_info=" ".join([locality_fixer(locality),region_fixer(region), countryName])
    print(geo_query_info)
    return locator(geo_query_info)

def calc_recall(pred, true):
    tp = [1 for p in pred if p in true]
    return sum(tp)/len(true) if true else 0

## UI

def draw_search_widget(product_description, product_code=None, geo_point=None, benchmark_suppliers=None):
    es_mgr = get_es_manager()

    if st.button(label="Пошук постачальників"):

        hits = es_mgr.search(
            product_description=product_description,
            product_code=product_code,
            geo_point=geo_point
        )

        pred = [hit['_source']['edr_code'] for hit in hits]        
        recall = calc_recall(pred, benchmark_suppliers)

        col1, col2, col3 = st.columns(3)
        col1.metric("К-ть учасників", "~2.6")
        col2.metric("Успіх %", "83 %")
        col3.metric("Recall@10", "{:.2f} %".format(recall*100))

        df = pd.DataFrame.from_records([
            {
                'Вгадав?': 'Так' if hit['_source']['edr_code'] in benchmark_suppliers else 'Ні',
                'Постачальник':hit['_source']['company_name'],
                'Контактна особа': hit['_source']['contact_name'],
                'Email':hit['_source']['contact_email'],
            }
        for hit in hits
        ])  

        st.dataframe(df)

        st.balloons()

def show_inro():

    release_notes = f"""
    ---
    **Highlights**
    - 🧠 Introducing `st.session_state` and widget callbacks to allow you to add statefulness to your apps. Check out the [blog post](http://blog.streamlit.io/session-state-for-streamlit/)
    **Notable Changes**
    - 🪄 `st.text_input` now has an `autocomplete` parameter to allow password managers to be used
    **Other Changes**
    - Using st.set_page_config to assign the page title no longer appends “Streamlit” to that title ([#3467](https://github.com/streamlit/streamlit/pull/3467))
    - NumberInput: disable plus/minus buttons when the widget is already at its max (or min) value ([#3493](https://github.com/streamlit/streamlit/pull/3493))
    """
    st.write(
        f"""
        # Вітаємо у Прозоррому Пошуку Постачальників ! 👋
        """
    )

    st.write(
        f"""
        🪟🔎 Прозоррий Пошук
        """
    )

    # st.write(release_notes)

def show_search():
        
    # dk = get_dk021()

    geo_point = None
    region = st.selectbox(label='Select region', options=['--','Lviv Rgn', 'Дніпропетровська область'])
    if region!='--':
        locality = st.text_input(label='Locality')
        streetAddress = st.text_input(label='Street/Address',placeholder='вулю Ленона, 123')
        if locality:
            geo_point = get_geopoint(locality=locality, region=region, countryName='Україна')

    product_description = st.text_input('Enter product description:')

    product_code = None
    # codes1 = {c.description: c.code for c in dk.children.values()}
    # product_desc1 = st.selectbox(label='Code L1', options=list(codes1.keys()))
    
    # if product_desc1:

    #     codes2 = {c.description: c.code for c in dk.get_level_category(code=codes1[product_desc1],level=1).children.values()}
    #     product_desc2 = st.selectbox(label='Code L2', options=list(codes2.keys()))
        
    #     if product_desc2:

    #         codes3 = {c.description: c.code for c in dk.get_level_category(code=codes2[product_desc2],level=2).children.values()}
    #         product_desc3 = st.selectbox(label='Code L3', options=list(codes3.keys()))

    #         product_code=codes3[product_desc3]

    #         st.write(product_desc3)
    #         st.write(product_code)

    if product_description:
        draw_search_widget(product_description, product_code, geo_point)

def show_search_by_id():
    
    st.write(
        f"""
        ## 🪟🔎 Прозоррий Пошук
        """
    )
    
    parent_dir = pathlib.Path(__file__).parent.resolve()
    
    with st.expander("Як користуватися?"):
        st.write("""
            Отримує дані за допомогою унікального ідентифікатора тендеру.

            Старається визначити геолокацію покупця.
            
            Здійснює пошук по опису товарів, категоріях товарів та географічної близькості постачальників.

        """)

        st.write("""

            
            [Приклад тендеру.](https://prozorro.gov.ua/tender/UA-2021-03-23-009357-c)

        """)

        st.write("""

            `b313afb97ea646fda06176e82cbe9992` - продукти харчування ^^^
            `98d54c150056474fbedb5420f8aad0d9` - медичні товари

        """)

        st.image(image=os.path.join(parent_dir, 'img', 'ex1.png'), caption='Унікальний ідентифікатор')

    uid = st.text_input(label='Унікальний ідентифікатор', help='Шукай на https://prozorro.gov.ua/')

    if uid:
        tender_info = get_tender_info(uid=uid)['data']
        
        with st.expander("Деталі"):
            st.json(body=tender_info)

        buyer_name = tender_info["procuringEntity"]['name']
        buyer_address = tender_info["procuringEntity"]['address']
        benchmark_suppliers = list(set([bid['tenderers'][0]['identifier']['id'] for bid in tender_info['bids']]))
        item_description_code = {item['description']: item['classification']['id'] for item in tender_info['items']}

        product_description = " ".join(list(item_description_code.keys()))
        product_code = list(item_description_code.values())[0]

        geo_point = get_geopoint(**buyer_address)
        
        st.text(body=buyer_name)
        st.text(body=product_description)
        st.text(body=product_code)
        if geo_point:
            st.text(body=f"Геолокація {geo_point[0]}  {geo_point[1]}")
       
        draw_search_widget(product_description, product_code, geo_point, benchmark_suppliers)

def show_settings():
    es_mgr = get_es_manager()

    if st.button(label='Показати статистику'): 
        try:
            index_stat = es_mgr.client.indices.stats()
            doc_count = index_stat['indices'][ES_INDEX]['primaries']['docs']['count']
            st.warning(f"Проіднексовано документів {doc_count}(унікальних постачальників)")
        except RuntimeError as e:
            st.exception(e)
    
    dataset = st.file_uploader("Завантаж файл з даними",)
    if dataset is not None:
        if st.button(label='Створити індекс', on_click=es_mgr.create_index):
            st.success("Створено новий індекс!")

        if st.button(label='Заповнити індекс', on_click=es_mgr.populate_index, args=(dataset,)):
            st.success("Індекс успішно заповнено!")

pages ={
    '👁️ Про сервіс':show_inro, 
    # 'Search':show_search,
    '🧍 Пошук постачальників':show_search_by_id, 
    '⚙️ Налаштування':show_settings
}

def main():
    st.set_page_config(page_title=f"Прозоррий пошук v0.0001", initial_sidebar_state="collapsed") #, layout="wide"
    st.sidebar.title(f"Меню")
    
    selected_demo = st.sidebar.radio("", list(pages.keys()), index=1)

    pages[selected_demo]()


if __name__ == '__main__':
    main()