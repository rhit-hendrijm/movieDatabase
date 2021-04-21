import pymongo

c = pymongo.MongoClient()
db = c.movieDB


def createMovie(movieID, title, year, rating, netflix, hulu, prime, disney, directors, genres, runtime):
    if db.movies.find({"movieID": movieID}).count() > 0:
        print("There is already a movie with that ID in the system")
        return
    else:
        toInsert = {"movieID": movieID, "title": title, "year": year, "rating": rating, "netflix": netflix,
                    "hulu": hulu, "prime": prime, "disney": disney, "directors": directors, "genres": genres,
                    "runtime": runtime}
        x = db.movies.insert_one(toInsert).inserted_id
        return


def movieDelete(movieID):
    db.movies.delete_one({"movieID": movieID})
    return


def updateMovie(movieID, title, year, rating, netflix, hulu, prime, disney, directorsToAdd, directorsToRemove,
                genresToAdd, genresToRemove, runtime):
    if db.movies.find({"movieID": movieID}).count() > 0:
        query = {"movieID": movieID}
        if title != '':
            updatedTitle = {"$set": {"title": title}}
            db.movies.update_one(query, updatedTitle)

        if year != -1:
            updatedYear = {"$set": {"year": year}}
            db.movies.update_one(query, updatedYear)

        if rating != -1:
            updatedRating = {"$set": {"rating": rating}}
            db.movies.update_one(query, updatedRating)

        if netflix != -1:
            updatedNetflix = {"$set": {"netflix": netflix}}
            db.movies.update_one(query, updatedNetflix)

        if hulu != -1:
            updatedHulu = {"$set": {"hulu": hulu}}
            db.movies.update_one(query, updatedHulu)

        if prime != -1:
            updatedPrime = {"$set": {"prime": prime}}
            db.movies.update_one(query, updatedPrime)

        if disney != -1:
            updatedDisney = {"$set": {"disney": disney}}
            db.movies.update_one(query, updatedDisney)

        if directorsToAdd != []:
            for director in directorsToAdd:
                if db.movies.find({"movieID": movieID, "directors": director}).count() == 0:
                    db.movies.update_one(query, {"$push": {"directors": director}})

        if directorsToRemove != []:
            for director in directorsToRemove:
                if db.movies.find({"movieID": movieID, "directors": director}).count() > 0:
                    db.movies.update_one(query, {"$pull": {"directors": director}})

        if genresToAdd != []:
            for genre in genresToAdd:
                if db.movies.find({"movieID": movieID, "genres": genre}).count() == 0:
                    db.movies.update_one(query, {"$push": {"genres": genre}})

        if genresToRemove != []:
            for genre in genresToRemove:
                if db.movies.find({"movieID": movieID, "genres": genre}).count() > 0:
                    db.movies.update_one(query, {"$pull": {"genres": genre}})

        if runtime != -1:
            updatedRuntime = {"$set": {"runtime": runtime}}
            db.movies.update_one(query, updatedRuntime)
        return
    else:
        print("")
        print("There are no movies associated with that ID")
        return


def movieSearch(searchField, searchCriteria):

    if searchField == 'id':
        if db.movies.find({"movieID": searchCriteria}).count() > 0:
            for x in db.movies.find({"movieID": searchCriteria}):
                print(x)
            return
        else:
            print("There are no movies with that ID in the system")

    if searchField == 'title':
        if db.movies.find({"title": searchCriteria}).count() > 0:
            for x in db.movies.find({"title": searchCriteria}):
                print(x)
            return
        else:
            print("There are no movies with that title in the system")

    if searchField == 'year':
        if db.movies.find({"year": searchCriteria}).count() > 0:
            for x in db.movies.find({"year": searchCriteria}):
                print(x)
            return
        else:
            print("There are no movies with that year in the system")

    if searchField == 'rating':
        if db.movies.find({"rating": searchCriteria}).count() > 0:
            for x in db.movies.find({"rating": searchCriteria}):
                print(x)
            return
        else:
            print("There are no movies with that rating in the system")

    if searchField == 'streaming service':
        if db.movies.find({searchCriteria: 1}).count() > 0:
            for x in db.movies.find({searchCriteria: 1}):
                print(x)
            return
        else:
            print("There are no movies on this streaming service in the system")

    if searchField == 'director':
        if db.movies.find({"directors": searchCriteria}).count() > 0:
            for x in db.movies.find({"directors": searchCriteria}):
                print(x)
            return
        else:
            print("There are no movies with that director in the system")

    if searchField == 'genre':
        if db.movies.find({"genres": searchCriteria}).count() > 0:
            for x in db.movies.find({"genres": searchCriteria}):
                print(x)
            return
        else:
            print("There are no movies with that genre in the system")

    if searchField == 'runtime':
        if db.movies.find({"runtime": searchCriteria}).count() > 0:
            for x in db.movies.find({"runtime": searchCriteria}):
                print(x)
            return
        else:
            print("There are no movies with that runtime in the system")
