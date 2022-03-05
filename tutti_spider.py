from datetime import datetime as dt
from datetime import timedelta as td
import scrapy

class TuttiSpider(scrapy.Spider):
    name = "tutti"

    def start_requests(self):
        urls = ['https://www.tutti.ch/de/li/ganze-schweiz/sport-outdoor?q=velo',]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        pagen=response.url[response.url.find("o=")+2:response.url.find("q=velo")]
        
        try:
            pagen=int(pagen[:-1])
        except ValueError:
            pagen=1
        print("PAGEN",pagen,response.url)
            
        date_stolen=dt(2022,2,22,0,0,0) #would be nice to pass this as an arg or kwarg actually
        for ad in response.css('div[data-automation="ad"]'):
            imattrib=ad.xpath('a').xpath('div').xpath('div').xpath('noscript').xpath('img').attrib
            region=ad.xpath('div').xpath('div').xpath('span').xpath('span/text()').extract()
            ad_posted=self.datetime_posted(ad.xpath('div').xpath('div').xpath('span/text()').extract()[0])
            
            if ad_posted < date_stolen:
                print('STOPPED AT',ad_posted)
                break
            yield {
                'title': imattrib['alt'], #same as title of ad (unless no picture? check)
                'first_image': imattrib['src'],
                #'n_images':
                'short_text':ad.xpath('div').xpath('div').xpath('p/text()').extract()[0],
                'url':ad.xpath('a').attrib['href'],
                'region':region[0],
                'postzahl':region[-1],
                'date_posted':dt.strftime(ad_posted.date(),'%d.%m.%Y')
            }
        last_page=int(response.css('button')[-2].attrib['aria-label'].split(' ')[-1])

        if last_page >1:
            pagen+=1
            next_page_url=f"https://www.tutti.ch/de/li/ganze-schweiz/sport-outdoor?o={pagen}&q=velo"
            print('NEXT_URL',next_page_url)
            yield response.follow(next_page_url, self.parse)
            
    def datetime_posted(self,time_str):
        '''make time string (eg: Huete, 15:30) into datetime'''
        try:
            return dt.strptime(time_str,'%m.%d.%Y') #eg 03.03.2022
        except ValueError:
            if time_str.startswith('Heute'):
                return dt.now()
            elif time_str.startswith('Gestern'):
                return dt.now()-td(days=1)
