'''
Created on Feb 2, 2015

@author: vivek
'''
import threading

from cluster_handler import DatabaseHandler

db_handler = DatabaseHandler()

while(1):
    print "press 1 to read random "
    print "press 2 to write random"
    inp = raw_input()
    if inp == '1':
        db_handler.read_random_data()
    else:
        db_handler.write_info()
