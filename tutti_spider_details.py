from datetime import datetime as dt
from datetime import timedelta as td
import scrapy
import pandas as pd

class TuttiSpiderDetails(scrapy.Spider):
    name = "tutti"

    def get_urls(self):
        '''get from dataframe'''
        df=pd.read_json('tutti_filtered.json')
        return df.url.values.tolist()

    def start_requests(self):
        urls=self.get_urls()
        print('URLS ',len(urls))
        for url in urls:
            yield scrapy.Request(url=f"https://www.tutti.ch{url}", callback=self.parse)

    def parse(self, response):

        for ad in response.css('table[class="S-gWT"]'):
            description=ad.xpath('tbody').xpath('tr')[1].xpath('td/text()').extract()
            dtext=description[0]
            for d in description[1:]:
                dtext+=f" {d}"
            yield {
                'url':response.url,
                'key':response.url[20:],
                'description':dtext
            }

        
