import tweepy
import sys
import pika
import json
import time
from nltk import wordpunct_tokenize
from nltk.corpus import stopwords

#get your own twitter credentials at dev.twitter.com
consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


def _calculate_languages_ratios(text):
        languages_ratios = {}

        tokens = wordpunct_tokenize(text)
        words = [word.lower() for word in tokens]

	for language in stopwords.fileids():
                stopwords_set = set(stopwords.words(language))
                words_set = set(words)
                common_elements = words_set.intersection(stopwords_set)
        	languages_ratios[language] = len(common_elements)

        return languages_ratios


def detect_language(text):
	ratios = _calculate_languages_ratios(text)

        most_rated_language = max(ratios, key=ratios.get)

        return most_rated_language




class CustomStreamListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()

        #setup rabbitMQ Connection
	credentials = pika.credentials.PlainCredentials('login', 'password', erase_on_connect=False)
        connection = pika.BlockingConnection(
           # pika.ConnectionParameters('localhost', 15672, '/', credentials)
            pika.ConnectionParameters('localhost')
	)
        self.channel = connection.channel()

        #set max queue size
        args = {"x-max-length": 5000}

        self.channel.queue_declare(queue='twitter_topic_feed', arguments=args)

    def on_status(self, status):
	
	tweet = (status.text).encode('utf-8')	
	language = detect_language(tweet)
	#print type(status.text)
	print language	
        print tweet
        
	data = {}
        data['text'] = status.text
        data['created_at'] = time.mktime(status.created_at.timetuple())
        data['geo'] = status.geo
        data['source'] = status.source

        #queue the tweet
        self.channel.basic_publish(exchange='',
                                    routing_key='twitter_topic_feed',
                                    body=json.dumps(data))

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True  

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True 

sapi = tweepy.streaming.Stream(auth, CustomStreamListener(api))
sapi.filter(track=['fifa', 'tf1'])
