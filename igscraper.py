from igramscraper.instagram import Instagram # pylint: disable=no-name-in-module
from collections import OrderedDict
from datetime import datetime

import pandas as pd
import json
import string
import itertools as it

# If account is public you can query Instagram without auth
instagram = Instagram()

# If account is private and you subscribed to it, first login
def make_login(your_username, your_password):
    """Logs in using user credentials.
    """
    instagram.with_credentials(your_username, your_password)
    instagram.login()

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

        post_data['time'] = datetime.fromtimestamp(media.created_time).strftime("%d/%m/%Y")

        post_data['likes'] = media.likes_count

        post_data['comments_count'] = len(post_data['comments'])
        
        post_data = dict(post_data)
        
        scraped_medias.append(post_data)
    
    return scraped_medias


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


def save_data(data):
    """Converts data to JSON.
    """
    with open('ig_posts_data.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)


def rem_nascii(d):
    printable = set(string.printable)
    d = ''.join(filter(lambda x: x in printable, d))
    return d


def rem_c(d):
    d = "".join(it.dropwhile(lambda x: not x.isalpha(), d))
    return d

if __name__ == "__main__":
    if yes_or_no("Do you wish to use login credentials: ") is True:
        this_username = input("Username: ")
        this_password = input("Password: ")
        print("\n*********************************\n")
        make_login(this_username, this_password)
    else:
        print("\n*********************************\n")
        print("Great. Note that you're continuing as an instagram guest.\nPrivate profiles will not be available.")
        print("\n*********************************\n")
    
    profile_name_ = input("Please enter the name of the profile you wish to scrap: ")
    n_posts = int(input("Please enter the number of posts you wish to scrap: "))
    print("\n*********************************\n")

    posts_data = scrap_profile(profile_name_, n_posts)
    df = pd.DataFrame(posts_data)

    df.text = df.text.apply(rem_nascii)
    df.text = df.text.apply(rem_c)

    excel_name = input("Please enter the name you'd like your saved excel file to have: ")
    df.to_excel(excel_name+".xlsx")
    save_data(posts_data)

    print("Thanks for using RA-IG Scraper!")
