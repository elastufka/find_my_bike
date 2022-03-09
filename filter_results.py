from datetime import datetime as dt
from datetime import timedelta as td
import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess
from tutti_spider import TuttiSpider
import argparse
import os
import io
from google.cloud import vision
from google.oauth2 import service_account

#from twisted.internet import reactor

exclude_types=['kinder','kind','mädchen','kindervelo','kinderfahrrad','damenvelo','citybike','mountain','mountainbike','mtb','fatbike','militär','bmx','e-bike','ebike','cruiser','fixie','einrad']
exclude_brands=['cube','giant','cannondale','totem','scott','wheeler','canyon','california','merida','bianchi','clio','bmc','gary fisher','crosswave','schwinn','puky','stoke']
exclude_colors=['weiss','schwarz','rot','gelb','grau'] #should use nltk eventually to make langauge-independent
exclude_other=['helm','veloanhänger','anhänger','20 zoll','26 zoll','28 zoll','neu','laufrad','hometrainer','indoor','suche','velonummer','velosschloss','stützräder']

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
            if e in row.title.lower() or e in e in row.description.lower().replace('\n',' '):
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
    urls,entities,scores=[],[],[]
    for im in image_urls:
        annotations=annotate(im)
        if annotations.web_entities:
            #print('\n{} Web entities found: '.format(len(annotations.web_entities)))
            for entity in annotations.web_entities[:3]: #if bike in top 3, keep
                if (entity.description =='Bike' or entity.description == 'Road Bike') and entity.score >0.6:
                    urls.append(im)
                    entities.append(entity.description)
                    scores.append(entity.score)
    bike_dict={"im_url":urls,"entity":entities,"score":scores}
    return bike_dict
                    
#def merge_results(filtered_df,original_df):
#    mdf=filtered_df.merge(original_df,left_on='key',right_on='url',how='inner')
#    mdf.rename(columns={'url_x':'full_url'},inplace=True)
#    mdf.drop(columns='url_y',inplace=True)
#    mdf=mdf.drop_duplicates(subset='full_url')
#    return mdf
    
#generate md table

def gen_md_table(df,ncols=5,write=True):
    spcial_char_map = {ord('ä'):'ae', ord('ü'):'ue', ord('ö'):'oe', ord('ß'):'ss'}
    im_urls=df.first_image.values.tolist()
    ad_urls=df.url.values.tolist()
    ad_dates=df.date_posted.values.tolist()
    ad_titles=df.title.values.tolist()
    ad_postz=df.postzahl.values.tolist()
    headerrow=['|','|']
    for col in range(ncols):
        headerrow[0]+='     |'
        headerrow[1]+=' --- |'
    headerrow[0]+='\n'
    headerrow[1]+='\n'
    nrows=len(im_urls)//ncols +1
    table_data=[]
    for i,row in enumerate(range(nrows)):
        rowdata='|'
        for j,col in enumerate(range(ncols)):
            try:
                im_url=im_urls[(i*ncols)+j]
                ad_url=ad_urls[(i*ncols)+j]
                ad_date=dt.strftime(pd.to_datetime(ad_dates[(i*ncols)+j]).date(),"%d.%m.%Y")
                ad_post=ad_postz[(i*ncols)+j]
                ad_title=ad_titles[(i*ncols)+j].translate(spcial_char_map)
                rowdata+=f"<img src='{im_url}' alt='{ad_title}' height='100px' width='150px'>[{ad_date} {ad_post}]({ad_url}) |"
            except IndexError:
                rowdata+=" |"
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
    parser.add_argument('-scrape', '--scrape', metavar="scrape", type=bool, default=False,
        help="do the scrape for ads")
    parser.add_argument('-cv', '--cv', metavar="cloud_vision", type=bool, default=False,
    help="use cloud vision API to see if the image is a bike")
    parser.add_argument('-w', '--w', metavar="write", type=bool, default=False,
    help="write images to markdown")
    args = parser.parse_args()

    if args.scrape:
        process=CrawlerProcess({'FEED_FORMAT': 'json','FEED_URI': 'tutti_results_temp.json'})
        process.crawl(TuttiSpider,start_date=dt.now())
        process.start()
        tutti_df=pd.read_json('tutti_results.jl')
        new_df=pd.read_json('tutti_results_temp.json')
        tutti_df.merge(new_df,on='url')
        tutti_df.reset_index().drop_duplicates(subset='url',inplace=True)
        tutti_df.to_json('tutti_results.jl')
        
    tutti_df=pd.read_json('tutti_results.jl')
    original_df=filter_results(tutti_df,exclude_types,exclude_brands=exclude_brands,exclude_colors=exclude_colors,exclude_other=exclude_other).drop_duplicates(subset='url')
    original_df.to_json('tutti_filtered.json')
    #filtered_df=filter_details(tutti_details,exclude_types,exclude_brands=exclude_brands,exclude_colors=exclude_colors,exclude_other=exclude_other)
    
    mdf=original_df#merge_results(filtered_df,original_df)
    
    if args.cv:
        bdf=pd.read_json('tutti_scored.json')
        new_ims=[i for i in mdf.first_image.values.tolist() if i not in bdf.first_image.values.tolist()]
        print(f"checking {len(new_ims)} images to see if they contain bicycles...")
        bike_dict=pd.DataFrame(web_detect_velo(new_ims)) #only run on ones that don't have score yet...
        bike_df = mdf.merge(bike_dict,left_on='first_image',right_on='im_url')
        bike_df['date_posted']=pd.to_datetime(bike_df['date_posted'],dayfirst=True)
        bike_df.sort_values(by='date_posted',ascending=False).drop_duplicates(subset='url',inplace=True)
        bike_df.to_json('tutti_scored.json')
        print(f"{len(bike_df)}/{len(mdf)} images contain bicycles")
        gen_md_table(bike_df)
    elif args.w:
        gen_md_table(mdf)
