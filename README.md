
Install Scrapy and Pymongo

```bash
pip install -r install -r requirements.txt 
```

[Install mongo](https://hub.docker.com/_/mongo) in docker

run spider

```bash
cd scraper
scrapy crawl tenders -a start='2021-01-01' -a end='2021-10-01'
```

Example of usage in readme