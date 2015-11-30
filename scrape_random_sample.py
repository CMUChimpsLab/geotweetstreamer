#!/usr/bin/env python

# Gets data from the twitter API in a given region.

import argparse, random, ConfigParser, os, time, datetime, pytz, traceback, json
import psycopg2, psycopg2.extensions, psycopg2.extras, ast, time, utils, httplib
from collections import defaultdict
from twython import TwythonStreamer
import twython.exceptions
import logging
import sys
import threading
import smtplib

logger = logging.getLogger('random-sample.log')
logger.setLevel(logging.INFO)

fh = logging.FileHandler('scraper-log.log')
fh.setLevel(logging.WARNING)

ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)

logger.info('hi')
config = ConfigParser.ConfigParser()
config.read('config.txt')

totalTweets = 0
totalSeconds = 1

OAUTH_KEYS = [{'consumer_key': config.get('twitter-' + str(i), 'consumer_key'),
              'consumer_secret': config.get('twitter-' + str(i), 'consumer_secret'),
              'access_token_key': config.get('twitter-' + str(i), 'access_token_key'),
              'access_token_secret': config.get('twitter-' + str(i), 'access_token_secret')}
             for i in range(int(config.get('num_twitter_credentials', 'num')))]

class MyStreamer(TwythonStreamer):
    psql_connection = None
    psql_cursor = None
    psql_table = None
    oauth_keys = {}

    # c = consumer, at = access_token
    def __init__(self, oauth_keys, psql_conn, note, skip):
        self.oauth_keys = oauth_keys
        self.psql_connection = psql_conn
        self.counter = 0
        self.skip = skip
        self.note = note
        self.bypass = False
        self.totalTweets = 0
        keys_to_use_index = random.randint(0, len(oauth_keys)-1)
        logger.warning("Connecting with keys: " + str(keys_to_use_index))
        keys_to_use = oauth_keys[keys_to_use_index]
        TwythonStreamer.__init__(self,
            keys_to_use['consumer_key'], keys_to_use['consumer_secret'],
            keys_to_use['access_token_key'], keys_to_use['access_token_secret'])
        
        self.psql_cursor = self.psql_connection.cursor()
        self.psql_table = table
        
        psycopg2.extras.register_hstore(self.psql_connection)
        # self.min_lon, self.min_lat, self.max_lon, self.max_lat =\
        #     [float(s.strip()) for s in utils.CITY_LOCATIONS[city_name]['locations'].split(',')]

    def on_success(self, data):    
        self.counter += 1 if not self.bypass else 0  
        self.totalTweets = self.totalTweets + 1 
        str_data = str(data)
        message = ast.literal_eval(str_data) 
        if ((self.counter % self.skip) != 0) and not self.bypass:
            pass;      
        else:
            if message.get('limit'):
                logger.warning('Rate limiting caused us to miss %s tweets (%s)' % (message['limit'].get('track'), self.note))
                self.counter = self.counter + int(message['limit'].get('track'))
            elif message.get('disconnect'):
                raise Exception('Got disconnect: %s (%s)' % (message['disconnect'].get('reason'), self.note))
            elif message.get('warning'):
                logger.warning('Got warning: %s (%s)' % (message['warning'].get('message'), self.note))
            elif 'delete' in message:
                pass
            else:
                ts = datetime.datetime.now()
                time = datetime.datetime.strptime(message['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
                print "%s - %s"%(self.totalTweets*1.0/totalSeconds, (ts-time).seconds )
                # Check to make sure the point is actually in the bbox.
                if 'coordinates' not in message or message['coordinates'] == None or 'coordinates' not in message['coordinates']:
                    message['coordinates'] = {'coordinates': [-999, -999]}
                self.save_to_postgres(dict(message))
                #logger.info('Got tweet: %s (%s, %s, %s)' % (message.get('text').encode('utf-8'), self.note, message['coordinates']['coordinates'][0], message['id']))
                
            
        logger.info('(%s) - done-success (%s)' % (self.note,self.counter))

    def on_timeout(self):
        print 'timed out'
        
    def on_error(self, status_code, data):
        logger.warning(data)
        if status_code == 420: # "Enhance your calm" aka rate-limit
            logger.warning("Rate limit, will try again (%s)." % self.note)
            time.sleep(3)
        elif status_code == 401: # "Unauthorized": maybe the IP is blocked
            # for an arbitrarily large amount of time due to too many
            # connections. 
            logger.warning("Unauthorized; sleeping for an hour.")
            time.sleep(60*60)
        self.disconnect() 

    # Given a tweet from the Twitter API, saves it to Postgres DB table |table|.
    def save_to_postgres(self, tweet):
        insert_str = utils.tweet_to_insert_string(tweet, self.psql_table, self.psql_cursor)
        try:
            self.psql_cursor.execute(insert_str)
            self.psql_connection.commit()
       
        except Exception as e:
            print "Error running this command: %s" % insert_str
            traceback.print_exc()
            traceback.print_stack()
            self.psql_connection.rollback() # just ignore the error
            # because, whatever, we miss one tweet

def streamingGeoFunction(psql_conn, args, side):
    print 'starting geo'
    sleep_time = 0
     
    while True:
        stream = MyStreamer(OAUTH_KEYS, psql_conn, 'geo', 20)
        try:
            if(side == "west"):
                stream.statuses.filter(locations="-126.045569,27.458166,-92.8921722,45.621386", stall_warning=True)
            else:
                stream.statuses.filter(locations="-92.8921722,27.458166,-66.646275,45.621386", stall_warning=True)
        except Exception as e:
            logger.warning("Error (geo)")
            logger.warning(e)
            logger.info("(geo) Sleeping for %d seconds." % sleep_time)
        time.sleep(sleep_time)
        sleep_time = 5

def streamingNonFunction(psql_conn, args):
    print 'starting non'
    sleep_time = 0
     
    while True:
        stream = MyStreamer(OAUTH_KEYS, psql_conn, 'non', 20)
        try:
            stream.statuses.sample(language = "en", stall_warning=True)
        except Exception as e:
            logger.warning("Error (non)")
            logger.warning(e)
            logger.info("(non) Sleeping for %d seconds." % sleep_time)
        time.sleep(sleep_time)
        sleep_time = 5

def emailError(diffGeo, diffNon):
    FROM_EMAIL = config.get('error_handling', 'email')
    TO_EMAILS = config.get('error_handling_to_addr', 'email').split(',')
    PSWD = config.get('error_handling', 'password')
    
    s = smtplib.SMTP('smtp.gmail.com', 587)  
    s.ehlo()
    s.starttls()
    s.ehlo
    s.login(FROM_EMAIL, PSWD)

    headers = ["from: " + FROM_EMAIL,
               "subject: Error: stopped collecting data",
               "to: " + ', '.join(TO_EMAILS),
               "mime-version: 1.0",
               "content-type: text/html"]
    headers = "\r\n".join(headers)
    body = "Data collection seems to be not working. \r\n\r\n" \
           + "Difference Geo: " + str(diffGeo) + "\r\n\r\n" \
           + "Difference Non: " + str(diffNon)

    s.sendmail(FROM_EMAIL, TO_EMAILS, headers + "\r\n\r\n" + body)
    s.quit()
    return
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--table', required=True)
    args = parser.parse_args()
    
    global table
    table = args.table
    psql_conn = psycopg2.connect("dbname='"+args.db+"'")


    
    # geoThreadWest = threading.Thread(target=streamingGeoFunction, args=(psql_conn, args, 'west'))
    # geoThreadEast = threading.Thread(target=streamingGeoFunction, args=(psql_conn, args, 'east'))
    nonThread = threading.Thread(target=streamingNonFunction, args=(psql_conn, args))
    
    # geoThreadWest.setDaemon(True)
    # geoThreadEast.setDaemon(True)    
    nonThread.setDaemon(True)
    
    # geoThreadWest.start()
    # geoThreadEast.start()
    nonThread.start()
    
    psql_cur = psql_conn.cursor()
    psql_cur.execute("SELECT * FROM "+args.table+" WHERE ST_X(coordinates) > -500")
    totalGeoTweets = psql_cur.rowcount
    psql_cur.execute("SELECT * FROM "+args.table+" WHERE ST_X(coordinates) < -500")
    totalNonTweets = psql_cur.rowcount
    
    alreadyEmailed = False
    while threading.active_count() > 0:
        totalSeconds = totalSeconds + 1
        # psql_cur.execute("SELECT * FROM "+args.table+" WHERE ST_X(coordinates) > -500")
        # newGeoTweets = psql_cur.rowcount
        # psql_cur.execute("SELECT * FROM "+args.table+" WHERE ST_X(coordinates) < -500")
        # newNonTweets = psql_cur.rowcount
        # diffGeo = (newGeoTweets - totalGeoTweets)
        # diffNon = (newNonTweets - totalNonTweets)
        # totalGeoTweets = newGeoTweets
        # totalNonTweets = newNonTweets
        
        # logger.warning("in the last few seconds got %s geo tweets %s non tweets" % (diffGeo,diffNon))
        # if((diffGeo == 0 or diffNon == 0) and not alreadyEmailed):
        #     alreadyEmailed = True
        #     emailError(diffGeo, diffNon)
        time.sleep(1)


  
    
