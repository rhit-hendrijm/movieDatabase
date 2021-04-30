from logLibrary import *
import pymongo

#c = pymongo.MongoClient('mongodb://433-27.csse.rose-hulman.edu:40002,433-25.csse.rose-hulman.edu:40000,433-26.csse.rose-hulman.edu:40001,433-28.csse.rose-hulman.edu:40003/moviedb?replicaSet=movieapp')
mongo_db = None# c.moviedb

def addMovie():
    #print("Enter ID")
    #movieID = input()
    print("Enter Title")
    title = input()
    print("Enter Year Released")
    year = input()
    print("Enter Rating")
    rating = input()
    print("Is this movie on Netflix? y/n")
    if input() == 'y':
        netflix = '1'
    else:
        netflix = '0'
    print("Is this movie on Hulu? y/n")
    if input() == 'y':
        hulu = '1'
    else:
        hulu = '0'
    print("Is this movie on Prime? y/n")
    if input() == 'y':
        prime = '1'
    else:
        prime = '0'
    print("Is this movie on Disney+? y/n")
    if input() == 'y':
        disney = '1'
    else:
        disney = '0'
    directors = []
    while True:
        print("Enter a Director")
        director = input()
        directors.append(director)
        print("Add another director? y/n")
        if input() == 'n':
            break
    genres = []
    while True:
        print("Enter a Genre")
        genre = input()
        genres.append(genre)
        print("Add another genre? y/n")
        if input() == 'n':
            break
    print("What is the runtime? (minutes)")
    runtime = input()
    print("")
    msg = " ".join(["ADDMOVIE", title, year, rating, netflix, hulu, prime, disney, 
                   convertListToString(directors), convertListToString(genres), runtime])
    print(msg)
    logCommand(msg) # logs command into redis


def deleteMovie():
    print("Enter ID of Movie to Delete")
    movieID = input()
    try:
        int(movieID)
    except: #if movieID is not able to convert to int
        print("Invalid movie id")
        return
    print("")
    msg = " ".join(["DELMOVIE",movieID])
    print(msg)
    logCommand(msg) # logs command into redis


def editMovie():
    movieID = 'None'
    title = 'None'
    year = 'None'
    rating = 'None'
    netflix = 'None'
    hulu = 'None'
    prime = 'None'
    disney = 'None'
    directorsToAdd = []
    directorsToRemove = []
    genresToAdd = []
    genresToRemove = []
    runtime = 'None'

    print("Enter ID of Movie to Edit")
    movieID = input()
    print("Would you like to edit the title? y/n")
    if input() == 'y':
        print("Enter updated movie title")
        title = input()
    print("Would you like to edit the year? y/n")
    if input() == 'y':
        print("Enter updated year")
        year = input()
    print("Would you like to edit the rating? y/n")
    if input() == 'y':
        print("Enter updated rating")
        rating = input()
    print("Would you like to edit the streaming services this movie is available on? y/n")
    if input() == 'y':
        print("Is this movie available on Netflix? y/n")
        if input() == 'y':
            netflix = '1'
        else:
            netflix = '0'
        print("Is this movie available on Hulu? y/n")
        if input() == 'y':
            hulu = '1'
        else:
            hulu = '0'
        print("Is this movie available on Prime? y/n")
        if input() == 'y':
            prime = '1'
        else:
            prime = '0'
        print("Is this movie available on Disney+? y/n")
        if input() == 'y':
            disney = '1'
        else:
            disney = '0'
    print("Would you like to add a director? y/n")
    if input() == 'y':
        while True:
            print("Enter a Director")
            director = input()
            directorsToAdd.append(director)
            print("Add another director? y/n")
            if input() == 'n':
                break
    print("Would you like to remove a director? y/n")
    if input() == 'y':
        while True:
            print("Enter a Director")
            director = input()
            directorsToRemove.append(director)
            print("Remove another director? y/n")
            if input() == 'n':
                break
    print("Would you like to add a genre? y/n")
    if input() == 'y':
        while True:
            print("Enter a Genre")
            genre = input()
            genresToAdd.append(genre)
            print("Add another genre? y/n")
            if input() == 'n':
                break
    print("Would you like to remove a genre? y/n")
    if input() == 'y':
        while True:
            print("Enter a Genre")
            genre = input()
            genresToRemove.append(genre)
            print("Remove another genre? y/n")
            if input() == 'n':
                break
    print("Would you like to edit the runtime? y/n")
    if input() == 'y':
        print("Enter updated runtime")
        runtime = input()
    print("")
    print(directorsToAdd,directorsToRemove,genresToAdd,genresToRemove)
    msg = " ".join(["EDITMOVIE",movieID, title, year, rating, netflix, hulu, prime, disney, 
                    convertListToString(directorsToAdd), convertListToString(directorsToRemove),
                    convertListToString(genresToAdd),convertListToString(genresToRemove), runtime])
    print(msg)
    logCommand(msg) # logs command into redis


def searchForMovie():
    mongo_db = None #persistent connections attempt
    print("Enter Search Field (id, title, year, rating, streaming service, director, genre, runtime)")
    searchField = input()
    searchby = 'None'
    if searchField == 'id':
        print("Enter id to search for")
        searchby = int(input())
    if searchField == 'title':
        print("Enter title to search for")
        searchby = input()
    if searchField == 'year':
        print("Enter year to search for")
        searchby = int(input())
    if searchField == 'rating':
        print("Enter rating to search for")
        searchby = int(input())
    if searchField == 'streaming service':
        print("Enter streaming service to search for")
        searchField = input()
        searchby = 1
    if searchField == 'director':
        print("Enter director to search for")
        searchField = 'directors'
        d = input()
        searchby = {'$in':[d]}
    if searchField == 'genre':
        print("Enter genre to search for")
        searchField = 'genres'
        g = input()
        searchby = {'$in':[g]}
    if searchField == 'runtime':
        print("Enter runtime to search for")
        searchby = int(input())
    
    search = {searchField:searchby}
    print("")
    # Search for movies in db (if mongo is down, report a crash)
    try:
        if mongo_db == None:
            c = pymongo.MongoClient('mongodb://433-27.csse.rose-hulman.edu:40002,433-25.csse.rose-hulman.edu:40000,433-26.csse.rose-hulman.edu:40001,433-28.csse.rose-hulman.edu:40003/moviedb?replicaSet=movieapp')
            mongo_db = c.moviedb
            print('find',search)
        cur = mongo_db.movies.find(search)
        if cur.count() > 0:
            for x in cur:
                print(x)
            return
        else:
            print("There are no movies matching that query in the system")
    except Exception as e:
        print("Cannot currently connect to MongoDB:")


def convertListToString(ls): #to send in log file - replaces spaces with _ so they can be broken up later on spaces
    if len(ls) == 0:
        return "None"
    else:
        string = ""
        for item in ls:
            item = item.replace(" ", "_")
            string = string + item+","
        return string[:-1]