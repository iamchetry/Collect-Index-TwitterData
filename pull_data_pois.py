from constants import *

import tweepy
import pickle
import pandas as pd

with open('crowdsourced_keywords.pickle', 'rb') as f:
    dict_kwd = pickle.load(f)

covid_kws = [_.lower() for _ in list(set(dict_kwd['covid']))]
vaccine_kws = [_.lower() for _ in list(set(dict_kwd['vaccine']))]
kwd_list = list(set(covid_kws+vaccine_kws))

consumer_key = '6Z92hF35TvItRHtqjO4tbK6py'
consumer_secret = 'pojgrahx0Z0neqjZucLBHrQ8raAFlor7zQHxzI9h3ntYUDaIcP'
access_token = '1432509056514670598-9BDuXLBimFl5tsUHIlHq8VgROiG0a9'
access_token_secret = 'pyFHPfhptJfOOP53PCOYXZh0TWjA9fLUgwj6zRq9um5mh'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

country_dict = {'en': 'USA', 'hi': 'INDIA', 'es': 'MEXICO'}


def get_tweets_by_poi_screen_name():
    '''
    Use user_timeline api to fetch POI related tweets, some postprocessing may be required.
    :return: List
    '''
    screen_name_list = list()
    poi_id = list()
    verified = list()
    country_list = list()
    id_list = list()

    replied_to_tweet_id = list()
    replied_to_user_id = list()
    reply_text = list()
    tweet_text = list()
    tweet_lang = list()

    hashtags = list()
    mentions = list()
    tweet_urls = list()
    tweet_emoticons = list()
    tweet_date = list()
    geolocation = list()

    for dict_ in poi_list:
        screen_name = '@'+dict_['screen_name']
        country = dict_['country']

        count_ = 0
        kwd_ct = 0

        for status in tweepy.Cursor(api.user_timeline, screen_name=screen_name, tweet_mode="extended").items(3200):
            raw_text = status.full_text

            if status.lang in ['en', 'hi', 'es']:
                # with open('poi_log.txt', 'a') as f:
                #     f.write('{}\n'.format(raw_text))

                hash_list = list()
                l = status.entities['hashtags']
                if l:
                    for _ in l:
                        hash_list.append('#' + _['text'])

                mention_list = list()
                l = status.entities['user_mentions']
                if l:
                    for _ in l:
                        mention_list.append('@' + _['screen_name'])

                url_list = list()
                l = status.entities['urls']
                if l:
                    for _ in l:
                        url_list.append(_['expanded_url'])

                screen_name_list.append(screen_name)
                poi_id.append(status.user.id)
                verified.append(status.user.verified)
                country_list.append(country)
                id_list.append(str(status.id))
                replied_to_tweet_id.append(status.in_reply_to_status_id)
                replied_to_user_id.append(status.in_reply_to_user_id)
                if status.in_reply_to_status_id:
                    reply_text.append(raw_text)
                else:
                    reply_text.append(None)

                tweet_text.append(raw_text)
                tweet_lang.append(status.lang)
                hashtags.append(hash_list)
                mentions.append(mention_list)
                tweet_urls.append(url_list)
                tweet_date.append(status.created_at)
                geolocation.append(status.coordinates)

                raw_text_lower = raw_text.lower()
                for _ in kwd_list:
                    if _ in raw_text_lower:
                        kwd_ct = kwd_ct + 1
                        break

                count_ = count_ + 1
                print(count_)

                if count_ > 1000 and kwd_ct > 100:
                    break

        print('========= {} done ==========\n'.format(screen_name))
        # with open('poi_log.txt', 'a') as f:
        #     f.write('========= {} done ==========\n'.format(screen_name))

    data_ = pd.DataFrame({'poi_name': screen_name_list,
                          'poi_id': poi_id,
                          'verified': verified,
                          'country': country_list,
                          'id': id_list,
                          'replied_to_tweet_id': replied_to_tweet_id,
                          'replied_to_user_id': replied_to_user_id,
                          'reply_text': reply_text,
                          'tweet_text': tweet_text,
                          'tweet_lang': tweet_lang,
                          'hashtags': hashtags,
                          'mentions': mentions,
                          'tweet_urls': tweet_urls,
                          'tweet_date': tweet_date,
                          'geolocation': geolocation})

    return data_


if __name__ == '__main__':
    data_ = get_tweets_by_poi_screen_name()
    print(data_)
    data_.to_csv('dummy_csv_poi_delta.csv', index=False)

