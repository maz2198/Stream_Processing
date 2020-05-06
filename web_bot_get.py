#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 29 19:33:27 2020

@author: prasanthchakka
"""

import requests
import sys
import time
import socket
import argparse
from pymongo import MongoClient

parser = argparse.ArgumentParser()
parser.add_argument("--hashtag", help="provide the hastag you want to filter", default = 'None')
parser.add_argument("--username", help="provide the username of the user whose messages you want to filter", default = 'None')

args = parser.parse_args()
hashtag = args.hashtag
username = args.username

#%%
### Take arguments from user for username or hashtag
#try:
#    hashtag = (sys.argv[1]).lower()
#except IndexError:
#    hashtag = 'none'
#        
#try:
#    username = sys.argv[2]
#except IndexError:
#    username = 'none'
    
print("Hashtag used is %s" % hashtag)
print("Username used is %s" % username)

#%%
def send_to_spark(resp, tcp_session):
    try:
        #tcp_session.send(resp+'\n')
        tcp_session.send(bytes("{}\n".format(resp), "utf-8"))
        #tcp_session.send("Hola")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
            

def set_tcp_session(IP,PORT):
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((IP, PORT))
    s.listen(1)
    print("Waiting for TCP connection...")
    conn, addr = s.accept()
    print("Connected... Starting listener for messages")
    return conn

def get_data():
    resp = requests.get("https://api.telegram.org/bot1020963580:AAGF6VQGbpzDz3OlgxtRyVqrhUI1LgyzS-s/getupdates?limit=100")
    return resp.json()["result"]

def filter_tags(my_list, tags):
    new_list = []
    for i in my_list:
        if 'text' in i['message']:
            if ("#"+tags) in i['message']['text']:
                new_list.append(i)
    return new_list

def filter_usernames(my_list,username):
    new_list = []   
    for i in my_list:
        if "username" in i['message']['from']:
            if i['message']['from']['username'] == username:
                new_list.append(i)
    return new_list

def post_data(my_list, connection, hashtag = 'None', username = 'None'):
    ##Write the code to post new data list from python to spark's TCP connection
    my_text = ' '
    if (hashtag != 'None'):
        my_list = filter_tags(my_list,hashtag)
    if (username != 'None'):
        my_list = filter_usernames(my_list,username)
    for i in my_list:
        if 'text' in i['message']:
            my_text += i['message']['text']+" "
    #print("My text value is "+my_text+"\n")
    send_to_spark(my_text, connection)

def push_to_mongo(list_of_dict):
    client = MongoClient('127.0.0.1', 27017)
    print("mongoClient: " + str(client))
    db = client['ie']
    print("db: " + str(db))
    collectionNm = 'globaldata'
    collection = db[collectionNm]
    print("Adding messages to collection: " + str(collection))
    if (len(list_of_dict)!=0):
        print("Global_data")
        print(list_of_dict)
        for i in list_of_dict:
            print("Inserting message to Mongo ")
            print(i)
            temp = i["update_id"]
            if (collection.find({"update_id":temp}).count() == 0):
                collection.insert_one(i)
    client.close()
    print()

global_data_list = []
old_index = len(global_data_list)

host = "localhost"
port_number = 10000

connection = set_tcp_session(host,port_number)

##Mongo connection


#%%

#%%
while True:
    #Add the new data fetched in this 
    new_data = []
    response = get_data()
    last_index = len(response) -1
    print("last index is" + str(last_index))

#%%
    while (old_index != last_index):
        if old_index == 0:
            global_data_list.append(response[old_index])
            old_index += 1
            continue
        #print("Old index + 1 is "+str(old_index+1))
        global_data_list.append(response[old_index+1])
        new_data.append(response[old_index+1])
        old_index+=1
    
    # Make last index to be the new old index as we perform get_data again 
    # after this loop iteration
    if (len(new_data) != 0):
        print("Posting new data to the TCP \n")
        post_data(new_data, connection, hashtag, username)
        old_index = last_index
    push_to_mongo(global_data_list)
    #print(global_data_list)
    #post_data_new()
    #post_data(new_data,connection)
    #send_to_spark("Hi",connection)
    time.sleep(5)


#%%
## to be removed

resp = requests.get("https://api.telegram.org/bot1020963580:AAGF6VQGbpzDz3OlgxtRyVqrhUI1LgyzS-s/getupdates?limit=100")
resp.json()["result"]
