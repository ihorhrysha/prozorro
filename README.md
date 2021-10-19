
Install Scrapy

```bash
pip install scrapy
```

[Install mongo](https://hub.docker.com/_/mongo) in docker


Generate file with seed ids from notebook - **tender_ids.txt**

run spider

```bash
cd scraper
scrapy crawl tenders -a start='2021-01-01' -a end='2021-10-01'
```