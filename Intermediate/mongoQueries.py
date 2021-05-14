# Mongo Queries
# Creates & Runs Add Query for deleting a Movie in MongoDB
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

def verifyMovie(msg,mongo_db):
    try:
        movieID = int(msg[2])
        cur = mongo_db.moviedb.movies.find({'id':movieID})
        if cur.count() > 0:
            return 0
        else:
            return 1
    except:
        print("Cannot currently connect to MongoDB:")
        return 2