
# Data Science in Real World

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

## Fast approach to get data from Prozorro

Install Scrapy and Pymongo

```bash
pip install -r requirements.txt 
```

[Install mongo](https://hub.docker.com/_/mongo) in docker

run spider

```bash
cd scraper
scrapy crawl tenders -a start='2021-01-01' -a end='2021-10-01'
```

Example of usage in get.ipynb
