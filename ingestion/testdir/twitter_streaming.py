#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API 
consumer_key = 'vihDfBy9BOeg2LzhjhgiTr5E4'
consumer_secret = 'ZltmFbuJKXgbxVIeKztKAyF1HXg3ZpKh1T8JOUR6S25ujjso8h'
access_token = '30690165-SSwDltrpnbEXatQOCK4F2Ulti31LV2NHwezbwtzNR'
access_token_secret = 'VSNlgkEGw8G9wswvVANthlJhcLtX0ErSDBNTWIl87MbYF'

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print (data)
        return True

    def on_error(self, status):
        print (status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['javascript', 'ruby', 'python'])
