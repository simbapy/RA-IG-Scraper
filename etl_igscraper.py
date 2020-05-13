from igramscraper.instagram import Instagram # pylint: disable=no-name-in-module
from collections import OrderedDict
from datetime import datetime
from sqlalchemy import create_engine

import time
import urllib
import pandas as pd
import pyodbc
import json
import string
import itertools as it
import logging


profiles = ['Builditburger','RemmanCafe','Lacasatwentyeight','Debs_w_remman','Orient_pearl_restaurant',
            'Sazeli_qatar','Basta.qatar','Baladnarest','Smatrestaurant','Jwalaqatar','Mokarabiaqatar',
            'gahwetnaqa','damascaone']

# If account is public you can query Instagram without auth
instagram = Instagram()

# If account is private and you subscribed to it, first login
def make_login(your_username, your_password):
    """Logs in using user credentials.
    """
    instagram.with_credentials(your_username, your_password)
    instagram.login()

# Scraps profile and populates fields
def scrap_profile(profile_name, number_of_posts):
    """Goes to profile and extracts post data.
    """
    scraped_medias = []
    post_data = OrderedDict()
    # Populate OrderedDict with post_data fields
    medias = instagram.get_medias(profile_name, count=number_of_posts)
    for media in medias:
        media_id_ = media.identifier
        post_data['url'] = media.link

        post_data['text'] = media.caption

        try:
            post_data['media_url'] = media.image_high_resolution_url
        except Exception:
            post_data['media_url'] = media.video_url
        
        comments = instagram.get_media_comments_by_id(media_id_, 10000)
        post_data['comments'] = [comment.text for comment in comments['comments']]

        #post_data['time'] = datetime.fromtimestamp(media.created_time).strftime("%d/%m/%Y")
        post_data['time'] = media.created_time

        post_data['likes'] = media.likes_count

        post_data['comments_count'] = len(post_data['comments'])
        
        post_data = dict(post_data)
        
        scraped_medias.append(post_data)
    
    return scraped_medias


# Used in in user interactions/prompts
def yes_or_no(question):
    while "The answer is invalid.":
        reply = str(input(question+' [y/n]: ')).lower().strip()
        if reply[0] == 'y':
            return True
            print("\n*********************************\n")
            break
        elif reply[0] == 'n':
            return False
            print("\n*********************************\n")
            break
        else:
            print('Please enter a valid answer. Either "y" or "n"')
            continue


# Saves scraped data as json
def save_data(data):
    """Converts data to JSON.
    """
    with open('ig_posts_data.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)

# Saves The latest times/dates of the previous scraping session so
# that the ETL tool won't repeat insertions of already scraped data
def save_latest_time(data):
    """Converts data to JSON.
    """
    with open('latest_time.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)


# Removes none ascii characters from string
def rem_nascii(d):
    printable = set(string.printable)
    d = ''.join(filter(lambda x: x in printable, d))
    return d


# Removes none ascii characters from list of strings
def rem_nascii_list(d):
    for i,a_string in enumerate(d):
        printable = set(string.printable)
        d[i] = ''.join(filter(lambda x: x in printable, a_string))
    return d


# Drops trailing characters from a string
def rem_c(d):
    d = "".join(it.dropwhile(lambda x: not x.isalpha(), d))
    return d

# Drops trailing characters from a list of strings
def rem_c_list(d):
    for i,a_string in enumerate(d):
        d[i] = "".join(it.dropwhile(lambda x: not x.isalpha(), a_string))
    return d

# Deletes old posts from a dataframe
def del_old_posts(x):
    y = datetime.fromtimestamp(x)
    return y

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with open('latest_time.json', 'r') as f:
        latest_json = json.load(f)
    
    latest_times = []
    df_list = []
    count = 1
    n_posts = 10

    logging.info('Beginning IG scraping.')

    for profile in profiles:
        profile_name_ = profile

        logging.info('Scraping {} page.'.format(profile))

        try:
            posts_data = scrap_profile(profile_name_, n_posts)
        except Exception:
            logging.info('''Last request to IG server returned a timeout: MAXIMUM requests limit reached.
                        Scraper will now rest for 400 seconds and try again.
                        {} will be skipped and scraped at the end'''.format(profile))
            time.sleep(400)
            profiles.append(profile)
            continue
        df = pd.DataFrame(posts_data)
        df['page_name'] = profile
        df['site'] = 'instagram'
        
        for i in latest_json:
            if list(i.keys())[0] == profile:
                df = df[df.time.apply(del_old_posts)> datetime.strptime(i[profile],"%d/%m/%Y")]
            else:
                pass
        
        if len(df) == 0:
            for i in latest_json:
                if list(i.keys())[0] == profile:
                    latest_time = {profile: i[profile]}
                    latest_times.append(latest_time)
            continue

        df.text = df.text.apply(rem_nascii)
        df.comments = df.comments.apply(rem_nascii_list)
        df.text = df.text.apply(rem_c)
        df.comments = df.comments.apply(rem_c_list)
        df.time = df.time.apply(lambda x: datetime.fromtimestamp(x).strftime("%d/%m/%Y"))

        for i in ['url', 'text', 'media_url', 'comments', 'time']:
            df[i].astype(str)
        
        df.comments = df.comments.apply(lambda x: " ".join(x))

        latest_time = {profile: df.loc[0,'time']}
        latest_times.append(latest_time)
        df_list.append(df)
    save_latest_time(latest_times)
    df_super = pd.concat(df_list)

    if len(df_super) == 0:
        logging.info("The session returned no new data. Terminating ETL process.")
        exit()

    logging.info('Connecting to database.')

    quote = urllib.parse.quote_plus('Driver={SQL Server};'
                      'Server=10.10.2.63;'
                      'Database=ReportServer;'
                      'Trusted_Connection=yes;')

    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quote))
    
    logging.info('Inserting new data into database.')
    df_super.to_sql('SOCIAL_MEDIA', con = engine, if_exists = 'append')
    
    logging.info('Closing connection.')
    engine.dispose()