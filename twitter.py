#!/usr/bin/python
# -*- coding: utf-8 -*-

# imports
from __future__ import absolute_import, print_function
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from sqlalchemy import create_engine
from sqlalchemy import exc


import json #wird benötigt um bestimmte Informationen aus den Tweets zu extrahieren
import pandas as pd 


class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    
    #Initialisierung eines Counters um Menge an gelesenen tweets zu begrenzen:
    def __init__(self):
        #super().__init__()
        self.counter = 0
        self.limit = 10 #hier wird das Limit angegeben
        
    def on_data(self, data):
        #print(data)
        
        #ID des Tweets:
        id_str = json.loads(data)['id_str']
        id_str = id_str.encode('utf-8')
        
        #Text des Tweets
        text = json.loads(data)['text']
        text = text.encode('utf-8')
        text = text.replace("\n"," ")
        text = text.replace(";","")
        
        #Erstellungsdatum des Tweets
        created_at = json.loads(data)['created_at']
        created_at = created_at.encode('utf-8')
        
        #Variable welche in die CSV Datei geschrieben wird
        write = id_str + ";" + text + ";" + created_at
        
        #Ausgabe des Tweets in CSV Datei:
        with open('tweets.csv', 'a') as tw:
            tw.write(write)
            tw.write("\n") #In neue Zeile springen, damit jeder Tweet in einer neuen Zeile steht
            
        #Aufrufen des Counters/Limiters:
        self.counter += 1
        if self.counter < self.limit:
            return True
        else:
            return False

    def on_error(self, status):
        print(status)

def connect():

    
    try:
        #Mit Datenbank verbinden:
        engine = create_engine('postgresql://postgres:postgres@localhost:5432/dbs')
        connection = engine.connect()

        #Csv einlesen:
        reader = pd.read_csv('tweets.csv', sep=';',header=None,encoding='latin1')
        frame = pd.DataFrame(reader)
        frame.columns = ["ID", "Text", "Date"]

        frame.to_sql(name='Tweets', con=engine,if_exists='replace')
        print("Everything is written")
    
    except exc.SQLAlchemyError as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection closed.')

def main():

    consumer_key        = "t3l8llVayMUCRLwh217fUY1p3"
    consumer_secret     = "JwqWePyGZuUJzXaGId7jv2iPe3EmLhkFoIbWBDGTf3W0I89lef"
    access_token        = "849205479331033088-WdPOGAMZ7dPLuP7WnpejEN62wEsVzQn"
    access_token_secret = "e3FXvPQeWlmmBQ5ylxTzuCZS6WdpVNLKeG1MYsLwMKpMa"

    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, listener)
    stream.filter(track=['@realDonaldTrump'])
    
    connect()

if __name__ == '__main__':
    main()
