from mongoQueries import *
from rethinkQueries import *
import pymongo
from rethinkdb import RethinkDB
import redis

red = redis.Redis(host='433-25.csse.rose-hulman.edu',port=6379,username='csse',password='gek7Xaeb')

log = "log"
mongoLSN = 'mongoLSN'
rethinkLSN = 'rethinkLSN'
currLSN = 'currLSN'

def checkLog():
    mongo_db = None
    rethink_db = None
    
    if red.get(mongoLSN) == None:
        red.set(mongoLSN,0) # Latest LSN mongo has successfully run
    if red.get(rethinkLSN) == None:
        red.set(rethinkLSN,0) # Latest LSN mongo has successfully run
    if red.get(currLSN) == None:
        red.set(currLSN,1) #Highest LSN in the log


    # Loop through 
    while True:
        # Try to connect to Rethink if there is no connection
        if rethink_db == None:
            try:
                rethink_db = RethinkDB()
                rethink_db.connect("433-25.csse.rose-hulman.edu",28015).repl()
            except:
                rethink_db = None
                print("RethinkDB is down")
                
        # Try to connect to MongoDB if there is no connection
        if mongo_db == None:
            try:
                mongo_db = pymongo.MongoClient('mongodb://433-27.csse.rose-hulman.edu:40002,433-25.csse.rose-hulman.edu:40000,433-26.csse.rose-hulman.edu:40001,433-28.csse.rose-hulman.edu:40003/moviedb?replicaSet=movieapp')
            except:
                mongo_db = None
                print("MongoDB is down")
        
        m_LSN = int(red.get(mongoLSN)) # LSN mongo is currently on
        r_LSN = int(red.get(rethinkLSN)) # LSN rethink is currently on
        latest_LSN = int(red.get(currLSN)) - 1 #Highest LSN in the log

        if mongo_db != None: #rethink_db == None
            #print("RethinkDB only down")
            if m_LSN <= latest_LSN:
                msgs = red.zrangebyscore(log,m_LSN,m_LSN) #returns command at lsn
                if len(msgs) == 0:
                    red.incr(mongoLSN,1)
                    continue

                msg = msgs[0].decode().split(' ') #should only be one
                print('Mongo Reading LSN',m_LSN,":",msgs[0])

                #run msg on mongo_db
                rethinkOnlyCommands = ['ADDUSER','DELUSER','EDITUSER','DELMOVIEFROMLIST']
                if msg[0] == 'DELMOVIE':
                    if len(msg) < 2:
                        print("Bad formatted DELMOVIE log command at lsn",m_LSN)
                    else:
                        res = delmovie(msg,mongo_db)
                        if res == 0:
                            red.incr(mongoLSN,1)
                        else:
                            mongo_db = None

                        #need to delete from collections in rethinkDB
                elif msg[0] == 'ADDMOVIE':
                    if len(msg) < 11:
                        print("Bad formatted ADDMOVIE log command at lsn",m_LSN)
                    else:
                        res = addmovie(msg,mongo_db)
                        if res == 0: #success
                            red.incr(mongoLSN,1)
                        else:
                            mongo_db = None

                elif msg[0] == 'EDITMOVIE':
                    if len(msg) < 14:
                        print("Bad formatted EDITMOVIE log command at lsn",m_LSN)
                    else:
                        res = editmovie(msg,mongo_db)
                        if res == 0: #success
                            red.incr(mongoLSN,1)
                        else:
                            mongo_db = None
                        #if mongo errors, res = 1, and loop again
                
                elif msg[0] == 'ADDMOVIETOLIST': #Verify movie exists, else remove from log so rethink doesn't run it
                    print('Mongo will verify movie ID is valid just before adding to user list')
                    red.incr(mongoLSN,1)
                elif msg[0] in rethinkOnlyCommands: #skip and move on
                    print("No operation necessary for MongoDB")
                    red.incr(mongoLSN,1)
                else:
                    print("Unknown command, skipped lsn",m_LSN)
                    red.incr(mongoLSN,1)


        if rethink_db != None:
            if r_LSN <= latest_LSN:
                msgs = red.zrangebyscore(log,r_LSN,r_LSN) #returns command at lsn
                if len(msgs) == 0:
                    red.incr(rethinkLSN,1)
                    continue

                msg = msgs[0].decode().split(' ') #should only be one
                print('Rethink Reading LSN',r_LSN,":",msgs[0])
                
                mongoOnlyCommands = ['ADDMOVIE','EDITMOVIE']
                
                #have a wait variable if m_LSN < r_LSN and need mongo to do a validity search (ADDMOVIETOLIST)
                if msg[0] == 'DELUSER':
                    if len(msg) < 2:
                        print("Bad formatted DELUSER log command at lsn",r_LSN)
                    else:
                        res = deluser(msg,rethink_db)
                        if res == 0:
                            red.incr(rethinkLSN,1)
                        else:
                            rethink_db = None

                elif msg[0] == 'ADDUSER':
                    if len(msg) < 4:
                        print("Bad formatted ADDUSER log command at lsn",r_LSN)
                    else:
                        res = adduser(msg,rethink_db)
                        if res == 0: #success
                            red.incr(rethinkLSN,1)
                        else:
                            rethink_db = None

                elif msg[0] == 'EDITUSER':
                    if len(msg) < 4:
                        print("Bad formatted EDITUSER log command at lsn",r_LSN)
                    else:
                        res = edituser(msg,rethink_db)
                        if res == 0: #success
                            red.incr(rethinkLSN,1)
                        else:
                            rethink_db = None
                
                elif msg[0] == 'DELMOVIEFROMLIST':
                    if len(msg) <3:
                        print("Bad formatted DELMOVIEFROMLIST log command at lsn",r_LSN)
                    else:
                        res = delMovieFromUserList(msg,rethink_db)
                        if res == 0: #success
                            red.incr(rethinkLSN,1)
                        else:
                            rethink_db = None

                elif msg[0] == 'ADDMOVIETOLIST': #Verify movie exists, else remove from log so rethink doesn't run it
                    if len(msg) <3:
                        print("Bad formatted DELMOVIEFROMLIST log command at lsn",r_LSN)
                    else:

                        #search for movie, if it exists, then move on
                        res = verifyMovie(msg,mongo_db)
                        if res == 0:
                            print("Movie ID",msg[2],"exists")
                        elif res == 1:
                            print("Movie ID",msg[2],"does not exist")
                            red.incr(rethinkLSN,1) #skip it
                            continue
                        else: #mongo down - can't connect; loop until get a connection
                            print("Mongo down - cannot verify movieID is valid; Must wait for mongoDB to verify valid movie ID")
                            continue

                        #if movie exists, add it to user list
                        res = addMovieToUserList(msg,rethink_db)
                        if res == 0: #success
                            red.incr(rethinkLSN,1)
                        else:
                            rethink_db = None
                
                elif msg[0] == 'DELMOVIE':
                    if len(msg) < 2:
                        print("Bad formatted DELMOVIE log command at lsn",m_LSN)
                    else:
                        res = delMovieIDFromAllLists(msg,rethink_db)
                        if res == 0:
                            red.incr(rethinkLSN,1)
                        else:
                            mongo_db = None
                elif msg[0] in mongoOnlyCommands: #skip and move on
                    print("No operation necessary for RethinkDB")
                    red.incr(rethinkLSN,1)
                else:
                    print("Unknown command, skipped lsn",r_LSN)
                    red.incr(rethinkLSN,1)

        removeMessages(m_LSN,r_LSN,latest_LSN,red)
            
def removeMessages(m_LSN,r_LSN,latest_LSN,red):
    lowestLSN = min(m_LSN,r_LSN) - 1 #delete messages <= lowestLSN 
    msgs = red.zremrangebyscore(log,0,lowestLSN)
    if msgs > 0:
        print(msgs,"messages deleted from message queue")

if __name__ == "__main__":
    #below statement used for testing log, re-runs mongo through log every startup
    #red.set(mongoLSN,0)
    checkLog()
           