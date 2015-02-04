'''
Created on Feb 2, 2015

@author: vivek
'''
import threading

from cluster_handler import DatabaseHandler


CONDITION_VAR = threading.Condition()
CHECK_IN_DATE = None
VOYAGER_CITY_CODE = None
HOTEL_CODE = None
PACKAGE = None

db_handler = DatabaseHandler()

class Reader(threading.Thread):
    def __init__(self, threadID):
        threading.Thread.__init__(self)
        self.type = 'reader'
        self.thread_id = threadID
    def run(self):
        db_handler.read_random_data()
            
class Writer(threading.Thread):
    def __init__(self,threadID):
        threading.Thread.__init__(self)
        self.type = 'writer'
        self.thread_id = threadID
    def run(self):
        db_handler.write_info()

while(1):
    print "press 1 to read random "
    print "press 2 to write random"
    inp = raw_input()
    if inp == '1':
        db_handler.read_random_data()
    else:
        db_handler.write_info()
