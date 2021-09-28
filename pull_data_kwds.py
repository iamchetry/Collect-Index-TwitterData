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


def get_tweets_by_lang_and_keyword():
    '''
    Use search api to fetch keywords and language related tweets, use tweepy Cursor.
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

    user_name = list()

    count_ = 0

    try:
        for kwd_ in kwd_list:

            for status in tweepy.Cursor(api.search, q=kwd_).items(30000):

                if status.lang in ['en', 'hi', 'es']:
                    count_ = count_ + 1

                    if count_ > 15000:
                        break

                    with open('kwd_log_4.txt', 'a') as f:
                        f.write('{}\n'.format(count_))

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

                    screen_name_list.append(None)
                    poi_id.append(None)
                    verified.append(status.user.verified)
                    country_list.append(country_dict[status.lang])
                    id_list.append(str(status.id))
                    replied_to_tweet_id.append(status.in_reply_to_status_id)
                    replied_to_user_id.append(status.in_reply_to_user_id)
                    if status.in_reply_to_status_id:
                        reply_text.append(status.text)
                    else:
                        reply_text.append(None)

                    tweet_text.append(status.text)
                    tweet_lang.append(status.lang)
                    hashtags.append(hash_list)
                    mentions.append(mention_list)
                    tweet_urls.append(url_list)
                    tweet_date.append(status.created_at)
                    geolocation.append(status.coordinates)

                    user_name.append('@' + status.user.screen_name)

            with open('kwd_log_4.txt', 'a') as f:
                f.write('========== done ==========\n'.format())

            if count_ > 15000:
                break

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
                              'geolocation': geolocation,
                              'user_name': user_name})

        return data_
    except Exception as e:
        print(e)


if __name__ == '__main__':
    data_ = get_tweets_by_lang_and_keyword()
    print(data_)
    data_.to_csv('dummy_csv_search_4.csv', index=False)

