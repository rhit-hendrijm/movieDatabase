from logLibrary import *
from rethinkdb import RethinkDB

rethink_db = None

# Users
def addUser():
    print("Enter username of User")
    username = input()
    print("Enter first name")
    first = input()
    print("Enter last name")
    last = input()

    #In intermediate, make sure username is unique
    print("")
    msg = " ".join(["ADDUSER",username,first,last])
    print(msg)
    logCommand(msg) # logs command into redis


def deleteUser():
    print("Enter username of User to Delete")
    username = input()
    print("")
    msg = " ".join(["DELUSER", username])
    print(msg)
    logCommand(msg) # logs command into redis


def editUser():
    print("Enter username of User")
    username = input()
    print("Enter first name")
    first = input()
    print("Enter last name")
    last = input()

    print("")
    msg = " ".join(["EDITUSER",username,first,last])
    print(msg)
    logCommand(msg) # logs command into redis

def searchForUser():
    rethink_db = None #persistent connections attempt
    print("Enter Username")
    username = input()
    searchby = {'username':username}
    
    print()
    # Search for users in db (if rethink is down, report a crash)
    try:
        if rethink_db == None:
            rethink_db = RethinkDB()
            rethink_db.connect("433-25.csse.rose-hulman.edu",28015).repl()
            print('search query:', searchby)
        cur = list(rethink_db.db('users').table("usertable").filter(searchby).run())
        if len(cur) > 0:
            for x in cur:
                 print(x)
            return
        else:
             print("There are no user with that username in the system")
    except Exception as e:
        print("Cannot currently connect to RethinkDB:")

# Movie List
def addToMovieList():
    print("Enter username of User to add to their movie list")
    username = input()
    print("Enter movieID to add to collection")
    movieID = input()
    try:
        int(movieID)
    except: #if year is not able to convert to int
        print("Invalid movieID, Aborted")
        return


    # Search mongoDB for valid movieID in the Intermediate on addusercollection
    print("")
    msg = " ".join(["ADDMOVIETOLIST",username,movieID])
    print(msg)
    logCommand(msg) # logs command into redis

def removeFromMovieList():
    print("Enter username of User to remove from their collection")
    username = input()
    print("Enter movieID to remove from collection")
    movieID = input()
    try:
        int(movieID)
    except: #if year is not able to convert to int
        print("Invalid movieID, Aborted")
        return

    print("")
    msg = " ".join(["DELMOVIEFROMLIST",username,movieID])
    print(msg)
    logCommand(msg) # logs command into redis