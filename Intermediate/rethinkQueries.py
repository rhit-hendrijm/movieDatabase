# Rethink Queries
# Creates & Runs Add Query for deleting a User in RethinkDB
def deluser(msg,rethink_db):

    username = msg[1]

    try:
        rethink_db.db('users').table('usertable').filter({'username':username}).delete().run()
        print("User",username,'deleted')
        return 0
    except:
        print("Error adding user - may have lost connection to rethink")
        return 1


# Creates & Runs Add Query for adding a User in RethinkDB
def adduser(msg,rethink_db):

    username = msg[1]
    first = msg[2]
    last = msg[3]

    try:
        #verify username doesn't exist already
        cur = list(rethink_db.db('users').table("usertable").filter({'username':username}).run())
        if len(cur) > 0:
            print('Username',username,'exists, could not add')
        else:
            rethink_db.db('users').table('usertable').insert({'username':username,'first':first,'last':last,'movieList': []}).run()
            print("User added with username",username)
        return 0
    except:
        print("Error adding user - may have lost connection to rethink")
        return 1
    
    return 0


# Creates & Runs Update Query for Editing a User in RethinkDB
def edituser(msg,rethink_db):
    
    username = msg[1]
    first = msg[2]
    last = msg[3]

    try:
        #verify username exists
        cur = list(rethink_db.db('users').table("usertable").filter({'username':username}).run())
        if len(cur) == 0:
            print('Username',username,' does not exist, edit failed')
        else:
            rethink_db.db('users').table("usertable").filter({'username':username}).update({'first': first, 'last':last}).run()
            print("User edited with username",username)
        return 0
    except:
        print("Error editing user information")
        return 1

    return 0

# Creates & Runs del Query for Editing a User in RethinkDB
def delMovieIDFromAllLists(msg,rethink_db):
    
    movieID = int(msg[1]) #validated on front end
    try:
        #removes movieID from all user lists
        res = rethink_db.db('users').table("usertable").replace(lambda doc: doc.merge({'movieList': doc['movieList'].set_difference([movieID])})).run()
        print("Deleted movie",movieID,"removed from all user lists")
        print(res)
        return 0
    except:
        print("Error deleting movieID from user lists")
        return 1

    return 0

# Creates & Runs Update Query for Editing a User in RethinkDB
def delMovieFromUserList(msg,rethink_db):
    
    username = msg[1]
    movieID = int(msg[2])
    try:
        res = rethink_db.db('users').table("usertable").filter({'username':username}).replace(lambda doc: doc.merge({'movieList': doc['movieList'].set_difference([movieID])})).run()
        print(res)
        if res['unchanged'] == 1:
            print("Movie was not in user list")
        elif res['deleted'] == 1:
            print("Movie",movieID,"removed from",username,"list")
        else:
            print("Unknown username")
        return 0
    except:
        print("Error deleting movie from user list")
        return 1

    return 0

def addMovieToUserList(msg,rethink_db):
    
    username = msg[1]
    movieID = int(msg[2])
    try:
        #verify username exists
        cur = list(rethink_db.db('users').table("usertable").filter({'username':username}).run())
        if len(cur) > 0:
            res = rethink_db.db('users').table("usertable").filter({'username':username}).update({'movieList': rethink_db.row['movieList'].append(movieID)}).run()
            print(res)
            print("Movie",movieID,"added to",username,"list")
        else:
            print('Username',username,' does not exist, add to list failed')
        return 0

    except:
        print("Error adding movie to user list")
        return 1

    return 0
