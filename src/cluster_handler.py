'''
Created on Jan 30, 2015

@author: vivek
'''
from random import randint

from cassandra_cluster import CassandraCluster
from cql_query import *


NODE_LIST = ['127.0.0.1']
CLUSTER_KEYSPACE_NAME = 'hotelstore'

ALPHABET_LIST = ('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O',\
                 'P','Q','R','S','T','U','V','W','X','Y','Z')


class DatabaseHandler(object):
    def __init__(self):
        self.cassandra_db = CassandraCluster(NODE_LIST)
        self.cassandra_db.create_session()
        self.set_hotelstore_keyspace()
        self.create_vendor_table()
    
    def set_hotelstore_keyspace(self):
        try:
            self.cassandra_db.execute(CREATE_HOTELSTORE_KEYSPACE)
            self.cassandra_db.execute("USE %s" % CLUSTER_KEYSPACE_NAME)
        except Exception, e:
            print "could not create or load the keyspace !!!" + str(e) 
            
    def create_vendor_table(self):
        try:
            self.set_hotelstore_keyspace()
            self.cassandra_db.execute(CREATE_VENDOR_TABLE % ('data'))
            self.cassandra_db.execute(CREATE_CITY_SEARCH_INDEX % ('data', 'data'))
        except Exception, exception_message:
            print "could not create table to store data " + str(exception_message)
        
    def write_info(self):
        param = {}
        param["vendor_handle"] = 'data'
        param["checkin"] = self.get_random_integer(8)
        param["city_search_key"] = self.get_random_word(5)
        param["pax"] = self.get_random_integer(3)
        param["stayrange"] = self.get_random_integer(2)
        param["hotelcode"] = self.get_random_word(4)
        param["hotelname"] = self.get_random_word(6)
        param["vendor_location_key"] = self.get_random_word(4)
        try:
            self.cassandra_db.execute(INSERT_HOTEL_DATA % param)
        except Exception, e:
            print "could not WRITE random data!!!" + str(e)
        
    def read_info(self,checkin_date ,stay_range ,hotel_code, package):
        param = {}
        param["vendor_handle"] = 'data'
        param["checkin"] = checkin_date
        param["stayrange"] = stay_range
        param["hotelcode"] = hotel_code
        param["pax"] = package
        try:
            result = self.cassandra_db.execute(GET_HOTEL_DATA % param)
            print result
        except Exception, e:
            print "could not Read the data"
        
    def get_random_integer(self,length):
        random_number = 0
        seed =1
        for counter in range(0,length):
            random = randint(0, 9)
            random_number = + seed *random 
            seed = seed * 10 
        return random_number

    def get_random_word(self,length):
        word = ""
        for counter in range(0,length):
            random_number = randint(0, 25)
            word = word + ALPHABET_LIST[random_number]
        return word
    
    def read_random_data(self):
        param = {}
        param["vendor_handle"] = 'data'
        get_random_row = self.cassandra_db.execute(SELECT_COMPLETE_DATA % param)
        length = len(get_random_row)
        if length == 0:
            print "No data present "
            return
        data_from_db = get_random_row[randint(0,length-1)]
        CHECK_IN_DATE = data_from_db.cid
        print CHECK_IN_DATE
        VOYAGER_CITY_CODE = data_from_db.vct
        print VOYAGER_CITY_CODE
        HOTEL_CODE = data_from_db.hc
        print HOTEL_CODE
        PACKAGE = data_from_db.pax
        print PACKAGE
    
if __name__ == "__main__":
    handler = DatabaseHandler()
