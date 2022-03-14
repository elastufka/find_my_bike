from datetime import datetime as dt
from datetime import timedelta as td
import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess
from tutti_spider import TuttiSpider
import argparse
import os
import io
from google.cloud import vision #probably overkill to use Cloud Vision just to identify bikes... any ImageNet should be able to do that, and faster... but I havent' been billed for this yet
from google.oauth2 import service_account

def run_spider(start_date,mtb=False):
    suffix=''
    if mtb:
        suffix='_mtb'
    process=CrawlerProcess({'FEED_FORMAT': 'json','FEED_URI': f'tutti_results_temp.json'})
    process.crawl(TuttiSpider,start_date=start_date)
    process.start()
    new_df=pd.read_json(f'tutti_results_temp.json')
    try:
        tutti_df=pd.read_json(f'tutti_results{suffix}.jl')
        tdf=tutti_df.append(new_df).reset_index(drop=True).drop_duplicates(subset='url')
        tdf.to_json(f'tutti_results{suffix}.jl')
    except ValueError: #file doesn't exist
        new_df.to_json(f'tutti_results{suffix}.jl') #merge and write new scrape results


def get_filters(mtb=False):
    ## TODO: NLP this! need to account for language and POS! SpaCy is probably overkill, nltk should be enough for the basics
    include=[]
    exclude_types=['kinder','kind','mädchen','kindervelo','kinderfahrrad','damenvelo','citybike','fatbike','militär','bmx','e-bike','ebike','cruiser','fixie','einrad']
    low_spec_brands=['totem','rockrider','wheeler','merida','spirit']
    exclude_brands=['cube','giant','cannondale','totem','scott','wheeler','canyon','california','merida','bianchi','clio','bmc','gary fisher','crosswave','schwinn','puky','stoke','specialized'] #technically it's a Trek but only a really good bike thief would be able to find that out
    exclude_colors=['weiss','schwarz','rot','gelb','grau'] #should use nltk eventually to make langauge-independent
    exclude_other=['helm','veloanhänger','anhänger','20 zoll','26 zoll','28 zoll','laufrad','hometrainer','indoor','suche','velonummer','stützräder']

    if mtb: #look for mountainbike I might be interested in
        exclude=exclude_types
        exclude.extend(exclude_other)
        exclude.extend(low_spec_brands)
        include=['scheibenbremsen','rahmen m'] #might be too limiting
        
    else: #look for stolen bike
        exclude=exclude_types
        exclude.extend(['mountain','mountainbike','mtb'])
        exclude.extend(exclude_brands)
        exclude.extend(exclude_colors)
        exclude.extend(exclude_other)
        exclude.extend(['neu','velosschloss'])
        
    return exclude,include

def filter_results(df,exclude=[],include=[], pricemax=700,pricemin=100):
    '''basic filtering'''
    badrows=[]
    for i,row in df.iterrows():
        for e in exclude:
            if e in row.title.lower() or e in row.description.lower().replace('\n',' '):
                badrows.append(i)
                break
    
    bad_df = df.index.isin(badrows)
    fdf=df[~bad_df].drop_duplicates(subset=['url'])

    if include !=[]:
        #only keep ads with these keywords...
        goodrows=[]
        for i,row in fdf.iterrows():
            for e in include:
                if e in row.title.lower() or e in row.description.lower().replace('\n',' '):
                    goodrows.append(i)
                    break
        fdf=fdf[fdf.index.isin(goodrows)]

    if pricemax:
        fdf=fdf.where(fdf.price < pricemax).dropna(how='all')
        
    if pricemin:
        fdf=fdf.where(fdf.price > pricemin).dropna(how='all')

    return fdf
        
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
                    
def gen_md_table(df,ncols=5,write=True, mdname='velo_images.md'):
    spcial_char_map = {ord('ä'):'ae', ord('ü'):'ue', ord('ö'):'oe', ord('ß'):'ss',ord("'"):''}
    im_urls=df.first_image.values.tolist()
    ad_urls=df.url.values.tolist()
    ad_dates=df.date_posted.values.tolist()
    ad_titles=df.title.values.tolist()
    ad_postz=df.postzahl.values.tolist()
    ad_price=df.price.values.tolist()
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
        for j,_ in enumerate(range(ncols)):
            try:
                im_url=im_urls[(i*ncols)+j]
                ad_url=ad_urls[(i*ncols)+j]
                ad_date=dt.strftime(pd.to_datetime(ad_dates[(i*ncols)+j]).date(),"%d.%m.%Y")
                if 'mtb' in mdname:
                    first_info=ad_price[(i*ncols)+j]
                else:
                    first_info=ad_date
                ad_post=ad_postz[(i*ncols)+j]
                ad_title=ad_titles[(i*ncols)+j].lower().translate(spcial_char_map)
                rowdata+=f"<img src='{im_url}' alt='{ad_title}' height='100px' width='150px'>[{first_info} {ad_post}]({ad_url}) |"
            except IndexError:
                rowdata+=" |"
        table_data.append(f"{rowdata}\n")
    if not write:
        return headerrow,table_data
    else:
        with(open(mdname,'w')) as f:
            f.writelines(headerrow)
            f.writelines(table_data)
        print(f'wrote {mdname}')
  
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="is my stolen bike being resold?")
    parser.add_argument('-scrape', '--scrape', metavar="scrape", type=bool, default=False,
        help="do the scrape for ads")
    parser.add_argument('-d', '--d', metavar="d", type=str, default=dt.strftime(dt.now().date(),"%Y-%m-%d"),
        help="start date for scrape")
    parser.add_argument('-mtb', '--mtb', metavar="mtb", type=bool, default=False,
        help="look for mountain bikes with my filters")
    parser.add_argument('-min', '--min', metavar="min", type=int, default=100,
        help="price minimum for mountainbike ads")
    parser.add_argument('-max', '--max', metavar="max", type=int, default=700,
        help="price maximum for mountainbike ads")
    parser.add_argument('-cv', '--cv', metavar="cloud_vision", type=bool, default=False,
    help="use cloud vision API to see if the image is a bike")
    parser.add_argument('-w', '--w', metavar="write", type=bool, default=False,
    help="write images to markdown")
    args = parser.parse_args()

    start_date=dt.strptime(args.d,"%Y-%m-%d")

    if args.scrape:
        run_spider(start_date,args.mtb)
        
    if args.mtb:
        exclude,include=get_filters(mtb=True)
        mdname='mtb_images.md'
        pricemax=args.max
        pricemin=args.min
        suffix='_mtb'
    else:
        exclude,include=get_filters()
        mdname='velo_images.md'
        pricemax=None
        suffix=''
        
    tutti_df=pd.read_json(f'tutti_results{suffix}.jl')
    mdf=filter_results(tutti_df,exclude=exclude, include=include,pricemax=pricemax,pricemin=pricemin).drop_duplicates(subset='url')
    mdf.to_json(f'tutti_filtered{suffix}.json') #save two different ones?
    
    if args.cv:
        try:
            bdf=pd.read_json(f'tutti_scored{suffix}.json')
            bdf['date_posted']=pd.to_datetime(bdf['date_posted'],dayfirst=True,unit='ms') #cron job failed on 9.3, make sure to run scrape with that start date eventually
            new_ims=[i for i in mdf.first_image.values.tolist() if i not in bdf.first_image.values.tolist()]
        except ValueError: #no such file
            new_ims=mdf.first_image.values.tolist()
        print(f"checking {len(new_ims)} images to see if they contain bicycles...")
        bike_dict=pd.DataFrame(web_detect_velo(new_ims)) #only run on ones that don't have score yet...
        print(f"{len(bike_dict)}/{len(new_ims)} images contain bicycles")
        bike_dict.to_json(f'bike_scores_temp{suffix}.json') #for testing
        mdf['date_posted']=pd.to_datetime(mdf['date_posted'],dayfirst=True)
        bike_df = mdf.merge(bike_dict,left_on='first_image',right_on='im_url') #newly detected bikes
        try:
            bdf=bdf.append(bike_df)
        except NameError:
            bdf=bike_df
        mddf=bdf.sort_values(by='date_posted',ascending=False).drop_duplicates(subset='url').reset_index(drop=True)
        mddf.to_json(f'tutti_scored{suffix}.json')
        gen_md_table(mddf,mdname=mdname)
    elif args.w:
        mdf=pd.read_json(f'tutti_scored{suffix}.json')
        gen_md_table(mdf,mdname='mtb_images.md')
        
