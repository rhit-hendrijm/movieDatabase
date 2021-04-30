from commandLibrary import *
from logLibrary import initializeLSN

print("Welcome to the Library!")
print("")
initializeLSN() # initializes the LSN's for mongo, rethink and the current LSN tracker if they are not already


while True:
    print("Please enter a command:")
    print("1 - Add New Movie")
    print("2 - Delete Movie")
    print("3 - Edit Movie")
    print("4 - Search For Movie")
    print("q - Quit")

    cmd = ''
    while cmd == '': #for input of enter key
        cmd = input()

    functions = {'1': addMovie,
                 '2': deleteMovie,
                 '3': editMovie,
                 '4': searchForMovie
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
