from movieCommandLibrary import *
from userCommandLibrary import *
from logLibrary import initializeLSN

print("Welcome to the Library!")
print("")
initializeLSN() # initializes the LSN's for mongo, rethink and the current LSN tracker if they are not already


while True:
    cmd = ''
    while cmd == '' or cmd not in ['q','1','2','3','4','5','6','7','8','9','10','11','12']: #for input of enter key
        print("Please enter a command:")
        print("1 - Add New Movie   \t\t5 - Add New User   \t\t9  - Add Movie to User List")
        print("2 - Delete Movie    \t\t6 - Delete User    \t\t10 - Delete Movie from User List")
        print("3 - Edit Movie      \t\t7 - Edit User      \t\tq  - Quit")
        print("4 - Search For Movie\t\t8 - Search For User")
        cmd = input()
        print()

    functions = {'1': addMovie,
                 '2': deleteMovie,
                 '3': editMovie,
                 '4': searchForMovie,
                 '5': addUser,
                 '6': deleteUser,
                 '7': editUser,
                 '8': searchForUser,
                 '9': addToMovieList,
                 '10': removeFromMovieList
                 }

    if cmd == 'q':
        break
    else:
        function = functions[cmd]
        if function is None:
            continue
        else:
            function()

    print("")
