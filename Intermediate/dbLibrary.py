import pymongo
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
                #r = RethinkDB(host=,port=,...)
                #print("rethink conn successful") 
                x=1
                #catchUpLog()
            except:
                print("RethinkDB is down")
                
        # Try to connect to  if there is no connection
        if mongo_db == None:
            try:
                mongo_db = pymongo.MongoClient('mongodb://433-27.csse.rose-hulman.edu:40002,433-25.csse.rose-hulman.edu:40000,433-26.csse.rose-hulman.edu:40001,433-28.csse.rose-hulman.edu:40003/moviedb?replicaSet=movieapp')
                #catchUpLog()
            except:
                print("MongoDB is down")
        
        m_LSN = int(red.get(mongoLSN)) # Latest LSN mongo has successfully run
        r_LSN = int(red.get(rethinkLSN)) # Latest LSN mongo has successfully run
        latest_LSN = int(red.get(currLSN)) - 1 #Highest LSN in the log

        if mongo_db == None and rethink_db == None: #Both DBs down
            print("Both DBs are down")

        elif mongo_db != None and rethink_db != None: #Both DBs up and running
            if m_LSN < r_LSN:
                print("Both DBs up, mongo behind rethink")
            elif r_LSN < m_LSN:
                print("Both DBs up, rethink behind mongo")
            else: #m_LSN = r_LSN
                print("Both DBs up and caught up")
        elif mongo_db == None:
            x=1
            #print("MongoDB only down")
        else: #rethink_db == None
            #print("RethinkDB only down")
            if m_LSN <= latest_LSN:
                msgs = red.zrangebyscore(log,m_LSN,m_LSN) #returns command at lsn
                if len(msgs) == 0:
                    red.incr(mongoLSN,1)
                    continue

                msg = msgs[0].decode().split(' ') #should only be one
                print('Mongo Reading LSN',m_LSN,":",msgs[0])

                #run msg on mongo_db
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


                else:
                    print("Unknown command, skipped lsn",m_LSN)
            
            
                #check if r_LSN is later than m_LSN, if so delete message
                     #zpopmin(log,1) #pops lowest lsn from log



# Mongo Queries
# Creates & Runs Add Query for adding a Movie in MongoDB
def delmovie(msg,mongo_db):

    if msg[1] != None:     
        movieID = int(msg[1])
        queryparams = {'id':movieID}
        print(queryparams)
        try:
            result = mongo_db.moviedb.movies.delete_one(queryparams)
            if result.deleted_count == 0:
                print("DB up and running, but could not delete movie")
            else:
                print("deleted movie successfully with id",movieID)
            return 0 
        except Exception as e:
            print(e)
            print("Error deleting movie, mongo_db:",mongo_db)
            return 1
    else:
        print("invalid ID, could not delete")
        return 0


# Creates & Runs Add Query for adding a Movie in MongoDB
def addmovie(msg,mongo_db):

    #find max id in mongo and add 1 to it
    try:
        res = mongo_db.moviedb.movies.find().sort([('id',-1)]).limit(1)
        movieID = int(res[0]['id']) + 1
        queryparams = {'id':movieID}
    except:
        print("Error getting valid ID for adding movie")
        return 1
    
    if msg[1] != 'None':
        queryparams['title'] = msg[1]
    if msg[2] != 'None':
        queryparams['year'] = int(msg[2])
    if msg[3] != 'None':
        queryparams['rating'] = int(msg[3])
    if msg[4] != 'None':
        queryparams['netflix'] = int(msg[4])
    if msg[5] != 'None':
        queryparams['hulu'] = int(msg[5])
    if msg[6] != 'None':
        queryparams['prime'] = int(msg[6])
    if msg[7] != 'None':
        queryparams['disney'] = int(msg[7])
    if msg[8] != 'None':
        res = [item.replace('_',' ') for item in msg[8].split(',')] #parse string-list into list
        queryparams['directors'] = res
    if msg[9] != 'None':
        res = [item.replace('_',' ') for item in msg[9].split(',')] #parse string-list into list
        queryparams['genres'] = res
    if msg[10] != 'None':
        queryparams['runtime'] = int(msg[10])
                                
    print('query params', queryparams)
    try:
        result = mongo_db.moviedb.movies.insert_one(queryparams)
        if not result.inserted_id:
            print("DB up and running, but could not insert movie")
        else:
            print("added movie successfully with id",movieID)
        return 0 
    except:
        print("Error adding movie, mongo_db:",mongo_db)
        return 1


# Creates & Runs Update Query for Editing a Movie in MongoDB
def editmovie(msg,mongo_db):
    print("editmovie running")
    movieID = int(msg[1])
    queryparams = {'$set':{}}
    removeattr = {'$pull':{}}

    if msg[2] != 'None':
        queryparams['$set']['title'] = msg[2]
    if msg[3] != 'None':
        queryparams['$set']['year'] = int(msg[3])
    if msg[4] != 'None':
        queryparams['$set']['rating'] = int(msg[4])
    if msg[5] != 'None':
        queryparams['$set']['netflix'] = int(msg[5])
    if msg[6] != 'None':
        queryparams['$set']['hulu'] = int(msg[6])
    if msg[7] != 'None':
        queryparams['$set']['prime'] = int(msg[7])
    if msg[8] != 'None':
        queryparams['$set']['disney'] = int(msg[8])
    if msg[9] != 'None':
        res = [item.replace('_',' ') for item in msg[9].split(',')] #parse string-list into list
        queryparams['$addToSet'] = {'directors': {'$each': res}}
    if msg[10] != 'None':
        res = [item.replace('_',' ') for item in msg[10].split(',')] #parse string-list into list
        removeattr['$pull']['directors'] = {'$in': res}
    if msg[11] != 'None':
        res = [item.replace('_',' ') for item in msg[11].split(',')] #parse string-list into list
        queryparams['$addToSet'] = {'genres': {'$each': res}}
    if msg[12] != 'None':
        res = [item.replace('_',' ') for item in msg[12].split(',')] #parse string-list into list
        removeattr['$pull']['genres'] = {'$in': res}
    if msg[13] != 'None':
        queryparams['$set']['runtime'] = int(msg[13])
    if len(queryparams['$set']) == 0:
        del queryparams['$set']
                                
    print('query params', queryparams)
    print('remove param', removeattr)
    try:
        if len(queryparams) != 0:
            result = mongo_db.moviedb.movies.update_one({'id':movieID},queryparams) #adds/sets new fields
            if result.matched_count == 0:
                print("No Movie with that ID")
                return
        if len(removeattr['$pull']) != 0:
            mongo_db.moviedb.movies.update_one({'id':movieID},removeattr) #removes from fields
        return 0
    except:
        print("Error on update")
        return 1


if __name__ == "__main__":
    #below statement used for testing log, re-runs mongo through log every startup
    #red.set(mongoLSN,0)
    checkLog()
           