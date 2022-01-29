
# Data Science in Real World

## Task and milestones

**Goal**: The results should be in form of recommendation to the stakeholders 
* What and how can they improve in Prozorro working processes?
* Are there any patterns or anomalies that should attract attention?
* Is it possible and how to predict specific events or cases based on available data?


### Stage 1 | 04.11
*Formulate a problem you will work on.*

*Formulate working hypotheses (5-7).*

Our team split into two separate streams, one will work on supplier recommendation problem, another on retrieving structured data from attached e-documents

[File with hypotheses](./hypotheses.md)

### Stage 2 | 18.11
*Evaluation of the hypotheses. *

*Enreach the problem with additional data.*


### Stage 3 | 02.12
*Narrow the main hypotheses (2-3).*
### Stage 4 | 16.12
*Final presentation to the stakeholders.*

## Implementation

### Fast approach to get data from Prozorro

Install Scrapy and Pymongo

```bash
pip install -r requirements.txt 
```

[Install mongo](https://hub.docker.com/_/mongo) in docker

run spider

* `tender` grabs the whole json, `etender` filters eauctions
* ~ 2 days per 3 years on 1 IP
* you can reduce the amount of data by filtering input json by fields you need in settings 


```bash
cd scraper
scrapy crawl etenders -a start='2019-01-01' -a end='2021-10-01'
```

### Data transformation

* to plain dataframe suitable for data exploration. Transformation of denorm data into DS usable frame(describes relation between buyers and suppliers) in [etl_func.ipynb](./notebooks/etl_func.ipynb)\
* to suppliers oriented format. Main purpose to crate data frame suible for search engine, examle can be found here.

### Simple supplier search engine

Install ElasticSearch

Install App dependencies

```bash
pip install -r ./app/requirements.txt 
```
Run an App

```bash
streamlit run app/app.py
```

Create index and populate with the data created on **transformation step**

Try to use: put data manually or by id of existing tender

Detailed description of an App you can find [here](./app/README.MD)
