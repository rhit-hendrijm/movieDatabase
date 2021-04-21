from dbLibrary import *


def addMovie():
    print("Enter ID")
    movieID = int(input())
    print("Enter Title")
    title = input()
    print("Enter Year Released")
    year = int(input())
    print("Enter Rating")
    rating = int(input())
    print("Is this movie on Netflix? y/n")
    if input() == 'y':
        netflix = 1
    else:
        netflix = 0
    print("Is this movie on Hulu? y/n")
    if input() == 'y':
        hulu = 1
    else:
        hulu = 0
    print("Is this movie on Prime? y/n")
    if input() == 'y':
        prime = 1
    else:
        prime = 0
    print("Is this movie on Disney+? y/n")
    if input() == 'y':
        disney = 1
    else:
        disney = 0
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
    runtime = int(input())
    print("")
    createMovie(movieID, title, year, rating, netflix, hulu, prime, disney, directors, genres, runtime)


def deleteMovie():
    print("Enter ID of Movie to Delete")
    movieID = input()
    print("")
    movieDelete(movieID)


def editMovie():
    movieID = -1
    title = ''
    year = -1
    rating = -1
    netflix = -1
    hulu = -1
    prime = -1
    disney = -1
    directorsToAdd = []
    directorsToRemove = []
    genresToAdd = []
    genresToRemove = []
    runtime = -1

    print("Enter ID of Movie to Edit")
    movieID = int(input())
    print("Would you like to edit the title? y/n")
    if input() == 'y':
        print("Enter updated movie title")
        title = input()
    print("Would you like to edit the year? y/n")
    if input() == 'y':
        print("Enter updated year")
        year = int(input())
    print("Would you like to edit the rating? y/n")
    if input() == 'y':
        print("Enter updated rating")
        rating = int(input())
    print("Would you like to edit the streaming services this movie is available on? y/n")
    if input() == 'y':
        print("Is this movie available on Netflix? y/n")
        if input() == 'y':
            netflix = 1
        else:
            netflix = 0
        print("Is this movie available on Hulu? y/n")
        if input() == 'y':
            hulu = 1
        else:
            hulu = 0
        print("Is this movie available on Prime? y/n")
        if input() == 'y':
            prime = 1
        else:
            prime = 0
        print("Is this movie available on Disney+? y/n")
        if input() == 'y':
            disney = 1
        else:
            disney = 0
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
        runtime = int(input())
    print("")
    updateMovie(movieID, title, year, rating, netflix, hulu, prime, disney, directorsToAdd, directorsToRemove,
                genresToAdd, genresToRemove, runtime)


def searchForMovie():
    print("Enter Search Field (id, title, year, rating, streaming service, director, genre, runtime)")
    searchField = input()
    if searchField == 'id':
        print("Enter id to search for")
        movieID = int(input())
        print("")
        movieSearch('id', movieID)
    if searchField == 'title':
        print("Enter title to search for")
        title = input()
        print("")
        movieSearch('title', title)

    if searchField == 'year':
        print("Enter year to search for")
        year = int(input())
        print("")
        movieSearch('year', year)

    if searchField == 'rating':
        print("Enter rating to search for")
        rating = int(input())
        print("")
        movieSearch('rating', rating)

    if searchField == 'streaming service':
        print("Enter streaming service to search for")
        streamingService = input()
        print("")
        movieSearch('streaming service', streamingService)

    if searchField == 'director':
        print("Enter director to search for")
        director = input()
        print("")
        movieSearch('director', director)

    if searchField == 'genre':
        print("Enter genre to search for")
        genre = input()
        print("")
        movieSearch('genre', genre)

    if searchField == 'runtime':
        print("Enter runtime to search for")
        runtime = int(input())
        print("")
        movieSearch('runtime', runtime)
