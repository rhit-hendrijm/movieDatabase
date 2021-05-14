import redis

r = redis.Redis(host='433-25.csse.rose-hulman.edu',port=6379,username='csse',password='gek7Xaeb')
# Pub/Sub (Messaging System)
# idea is to use sorted sets with priority as LSN? (get last one in REDIS and increment by one)
# 4 - ADD ... (one DB has last LSN of 3, should run commands when )
# 5 - DEL ...
# 6 - UPD ...

# Mongo, Rethink will have a key to the last LSN they have performed
# also a currLSN to keep track of highest LSN (in case queue is empty)
log = "log"
mongoLSN = 'mongoLSN'
rethinkLSN = 'rethinkLSN'
currLSN = 'currLSN'
# Once both of the commands are run on the dbs, log their last performed LSN and delete LSN's that are <= the min of the two
# Upon crash, python will detect error (try/catch) and post the LSN to 


def initializeLSN():
    if r.get(currLSN) == None:
        r.set(currLSN,1)
    if r.get(mongoLSN) == None:
        r.set(mongoLSN,0)
    if r.get(rethinkLSN) == None:
        r.set(rethinkLSN,0)

def logCommand(command): #increments currLSN after adding to log
    lsn = r.get(currLSN)
    res = r.zadd(log,{command:lsn},nx=True,xx=False,ch=False,incr=False) #nx - add commands, don't edit scores if same!
    #res = r.execute_command('ZADD', log, 'NX', lsn, command)
    if res <= 0:
        print(res)
        print("Error adding command to log")
    else:
        r.incr(currLSN,1)
        print("Successfully added command to log")
