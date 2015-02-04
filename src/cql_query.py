'''
Created on Jan 30, 2015
PRE143001102811
@author: vivek
'''

'''
ACRONYMS DEMYSTIFIED:
cid :   check-in date 
vct :   voyger city code
pax :   Number of Adults + Children + Room(like 101, 201)
los :   lenght of stay or stayrange
hc  :   hotelcode
hn  :   hotelname
#lud :   last updated date
#rtp :   rate plan
#rtc :   room type code
#rpc :   rate plan code
#sf  :   special offers
#rmt :   room total
#tax :   taxes
#org :   original rate
#net :   net amount        
vlk :   vendor location key
#hcn :   hotel chain
#rs  :   rate supplier
'''

PING_CLUSTER = '''
    SELECT now() FROM system.local;
    '''

CREATE_HOTELSTORE_KEYSPACE = '''
    CREATE KEYSPACE IF NOT EXISTS hotelstore WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3} AND DURABLE_WRITES =true;
    '''

CREATE_VENDOR_TABLE = '''
    CREATE TABLE IF NOT EXISTS hotelstore.%s (cid int, vct text, pax int, los int, hc text, hn text, vlk text, PRIMARY KEY ((cid, los, pax), hc)) WITH compaction={'class': 'SizeTieredCompactionStrategy'} AND compression={'sstable_compression': 'LZ4Compressor'};
    '''

CREATE_CITY_SEARCH_INDEX = '''
    CREATE INDEX IF NOT EXISTS %s_city_search ON hotelstore.%s (vct);
    '''
     
INSERT_HOTEL_DATA = '''
    INSERT INTO hotelstore.%(vendor_handle)s (cid, vct, pax, los, hc, hn, vlk) VALUES (%(checkin)i, '%(city_search_key)s', %(pax)i, %(stayrange)i, '%(hotelcode)s', '%(hotelname)s', '%(vendor_location_key)s');
    '''

GET_HOTEL_DATA = '''
    SELECT hc, hn, vct, vlk, FROM hotelstore.%(vendor_handle)s WHERE cid = %(checkin)i AND los = %(stayrange)i AND pax = %(pax)i AND hc in %(hotelcodes)s;
    '''
    
DELETE_HOTEL_DATA = '''
    DELETE FROM hotelstore.%(vendor_handle)s WHERE cid = %(checkin)i AND los = %(stayrange)i AND pax = %(pax)i AND hc = '%(hotelcode)s';
    ''' 
SELECT_COMPLETE_DATA = '''
    SELECT * FROM hotelstore.%(vendor_handle)s ;
    '''
