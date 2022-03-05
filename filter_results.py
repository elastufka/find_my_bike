from datetime import datetime as dt
from datetime import timedelta as td
import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess
from tutti_spider import TuttiSpider
from tutti_spider_details import TuttiSpiderDetails
import argparse
import os
import io
from google.cloud import vision
from google.oauth2 import service_account

#from twisted.internet import reactor

exclude_types=['kinder','kind','mädchen','kindervelo','kinderfahrrad','damenvelo','damen','citybike','city','mountain','mountainbike','mtb','fatbike','militär','bmx','e-bike','ebike','cruiser','fixie','einrad']
exclude_brands=['cube','giant','cannondale','totem','scott','wheeler','canyon','california','merida','bianchi','clio','bmc','gary fisher','crosswave','schwinn','puky']
exclude_colors=['weiss','schwarz','rot','gelb','grau'] #should use nltk eventually to make langauge-independent
exclude_other=['helm','veloanhänger','anhänger','14 zoll','16 zoll','18 zoll','20 zoll','26 zoll','28 zoll','licht','sattel','neu','laufrad','hometrainer','indoor','pedale','fahrradträger','veloträger','velotasche','taschen','suche','velonummer','velosschloss','stützräder','ketten']

def filter_results(df,exclude_types,exclude_brands=exclude_brands,exclude_colors=exclude_colors,exclude_other=exclude_other):
    '''basic filtering'''
    exclude=exclude_types
    if exclude_brands:
        exclude.extend(exclude_brands)
    if exclude_colors:
        exclude.extend(exclude_colors)
    if exclude_other:
        exclude.extend(exclude_other)
    
    #print(exclude)
    badrows=[]
    for i,row in df.iterrows():
        for e in exclude:
            if e in row.title.lower() or e in e in row.short_text.lower().replace('\n',' '):
                badrows.append(i)
                break
                
    bad_df = df.index.isin(badrows)
    return df[~bad_df].drop_duplicates(subset=['url'])#,badrows
    
def filter_details(df,exclude_types,exclude_brands=exclude_brands,exclude_colors=exclude_colors,exclude_other=exclude_other):
    '''basic filtering'''
    exclude=exclude_types
    if exclude_brands:
        exclude.extend(exclude_brands)
    if exclude_colors:
        exclude.extend(exclude_colors)
    if exclude_other:
        exclude.extend(exclude_other)

    badrows=[]
    for i,row in df.iterrows():
        for e in exclude:
            if e in row.description.lower():
                badrows.append(i)
                break
                
    bad_df = df.index.isin(badrows)
    return df[~bad_df]
    
def annotate(image_url):
    """Returns web annotations given the path to an image."""
    # [START vision_web_detection_tutorial_annotate]

    credentials = service_account.Credentials.from_service_account_file(os.environ['BIKE_CREDENTIALS'])

    client = vision.ImageAnnotatorClient(credentials=credentials)

    if image_url.startswith('http'):
        image = vision.Image()
        image.source.image_uri = image_url

    else:
        with io.open(path, 'rb') as image_file:
            content = image_file.read()

        image = vision.Image(content=content)

    web_detection = client.web_detection(image=image).web_detection
    # [END vision_web_detection_tutorial_annotate]
    return web_detection

def web_detect_velo(image_urls):
    """Prints detected features in the provided web annotations."""
    # [START vision_web_detection_tutorial_print_annotations]
    bike_urls=[]
    for im in image_urls:
        annotations=annotate(im)
        if annotations.web_entities:
            #print('\n{} Web entities found: '.format(len(annotations.web_entities)))
            for entity in annotations.web_entities[:3]: #if bike in top 3, keep
                if entity.description =='Bike' or entity.description == 'Road Bike' and entity.score >0.6:
                    bike_urls.append(im)
    return bike_urls
                    
def merge_results(filtered_df,original_df):
    mdf=filtered_df.merge(original_df,left_on='key',right_on='url',how='inner')
    mdf.rename(columns={'url_x':'full_url'},inplace=True)
    mdf.drop(columns='url_y',inplace=True)
    mdf=mdf.drop_duplicates(subset='full_url')
    return mdf
    
#generate md table

def gen_md_table(df,ncols=5,write=True):
    im_urls=df.first_image.values.tolist()
    ad_urls=df.full_url.values.tolist()
    headerrow=['|','|']
    for col in range(ncols):
        headerrow[0]+='     |'
        headerrow[1]+=' --- |'
    headerrow[0]+='\n'
    headerrow[1]+='\n'
    nrows=len(im_urls)//ncols
    table_data=[]
    for i,row in enumerate(range(nrows)):
        rowdata='|'
        for j,col in enumerate(range(ncols)):
            im_url=im_urls[(i*ncols)+j]
            ad_url=ad_urls[(i*ncols)+j]
            rowdata+=f"<img src='{im_url}' height='100px' width='150px'>[ad]({ad_url}) |"
        table_data.append(f"{rowdata}\n")
    if not write:
        return headerrow,table_data
    else:
        with(open('velo_images.md','w')) as f:
            f.writelines(headerrow)
            f.writelines(table_data)
        print('wrote velo_images.md')
  
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="is my stolen bike being resold?")
    parser.add_argument('-s1', '--scrape1', metavar="scrape1", type=bool, default=False,
        help="do the scrape for ads")
    parser.add_argument('-s2', '--scrape2', metavar="scrape2", type=bool, default=False,
    help="do the scrape for details")
    parser.add_argument('-cv', '--cv', metavar="cloud_vision", type=bool, default=False,
    help="use cloud vision API to see if the image is a bike")
    args = parser.parse_args()

    if args.scrape1:
        process=CrawlerProcess()
        process.crawl(TuttiSpider)
        process.start()
        
    tutti_df=pd.read_json('tutti_results.jl',lines=True)
    original_df=filter_results(tutti_df,exclude_types,exclude_brands=exclude_brands,exclude_colors=exclude_colors,exclude_other=exclude_other).drop_duplicates(subset='url')
    original_df.to_json('tutti_filtered.json')
    
    if args.scrape2:
        process=CrawlerProcess()
        process.crawl(TuttiSpiderDetails)
        process.start()
        
    tutti_details=pd.read_json('tutti_detail.jl',lines=True)
    filtered_df=filter_details(tutti_details,exclude_types,exclude_brands=exclude_brands,exclude_colors=exclude_colors,exclude_other=exclude_other)
    
    mdf=merge_results(filtered_df,original_df)
    if args.cv:
        bike_urls=web_detect_velo(mdf.first_image.values.tolist())
        bike_df = mdf[mdf.first_image.isin(bike_urls)]
        gen_md_table(bike_df)
    else:
        gen_md_table(mdf)
