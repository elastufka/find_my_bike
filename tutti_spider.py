from datetime import datetime as dt
from datetime import timedelta as td
import scrapy
#import collections

class TuttiSpider(scrapy.Spider):
    name = "tutti"
    
    def __init__(self, start_date=dt(2022,2,22,0,0,0)): 
        super(TuttiSpider, self).__init__()
        self.start_date=start_date
        self.start_urls=['https://www.tutti.ch/de/li/ganze-schweiz/sport-outdoor?q=velo']
        
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        pagen=response.url[response.url.find("o=")+2:response.url.find("q=velo")]
        
        try:
            pagen=int(pagen[:-1])
        except ValueError:
            pagen=1
            
        #date_stolen=dt(2022,2,22,0,0,0) #would be nice to pass this as an arg or kwarg actually
        for ad in response.css('div[data-automation="ad"]'):
            ad_posted=self.datetime_posted(ad.xpath('div').xpath('div').xpath('span/text()').extract()[0])
            
            ad_url=ad.xpath('a').attrib['href']
            
            if ad_posted < self.start_date:
                print('STOPPED AT',ad_posted)
                break
            
            #follow ad url and get full description
            yield response.follow(ad_url, self.parse_full)
        last_page=int(response.css('button')[-2].attrib['aria-label'].split(' ')[-1])

        if last_page >1:
            pagen+=1
            next_page_url=f"https://www.tutti.ch/de/li/ganze-schweiz/sport-outdoor?o={pagen}&q=velo"
            yield response.follow(next_page_url, self.parse)
            
    def parse_full(self, response):
        '''get full ad description text'''
        ad=response.css('table[class="S-gWT"]')
        description=ad.xpath('tbody').xpath('tr')[1].xpath('td/text()').extract()
        dtext=description[0]
        for d in description[1:]:
            dtext+=f" {d}"
            
        title=response.css('h1').extract_first()
        title=title[title.find('>')+1:title.rfind('<')]
       
        ad_posted=self.datetime_posted(response.css("div[class='_9mKtt pRm6L']").xpath('span/text()').extract_first())
        region=response.css("div[class='M2A0K']").xpath('span/text()').extract_first().split(',')
        first_image= response.css('div[class="puEEg"]').xpath('div')[0].xpath('div').xpath('noscript').xpath('img').attrib['src']
        
        price_elem=response.css('dd[class="LGWk6"]')[1].get()
        pmap={ord('-'):'',ord('.'):''}
        
        attributes={"url":response.url,
            "title":title,
            "date_posted":dt.strftime(ad_posted.date(),'%d.%m.%Y'),
            "first_image":first_image,
            "region":region[0],
            "postzahl":int(region[1].strip()),
            "description":dtext,
            "price":int(price_elem[price_elem.find('>')+1:price_elem.rfind('<')].translate(pmap)),
    }
        yield attributes
            
    def datetime_posted(self,time_str):
        '''make time string (eg: Huete, 15:30) into datetime'''
        try:
            return dt.strptime(time_str,'%d.%m.%Y') #eg 03.03.2022
        except ValueError:
            if time_str.startswith('Heute'):
                return dt.now()
            elif time_str.startswith('Gestern'):
                return dt.now()-td(days=1)

