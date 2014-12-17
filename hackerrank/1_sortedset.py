#! /usr/bin/python

'''
Problem Statement: https://www.hackerrank.com/contests/quora-haqathon/challenges/sortedset
'''

import socket, sys, threading, struct


SERVER_SOCKET_PATH = "./socket"

class RankingSystem:
    __sets = None
    
    def __str__(self):
        return str(RankingSystem.__sets)
    
    def __init__(self):
        print >> sys.stderr, "RankingSystem initialized.."
        if RankingSystem.__sets == None:
            RankingSystem.__sets = {}

    def add(self, setv, keyv, scorev):
        if setv not in RankingSystem.__sets:
            RankingSystem.__sets[setv] = {}
        RankingSystem.__sets[setv][keyv] = RankingSystem.__sets[setv].get(keyv, 0) + scorev
        return RankingSystem.__sets[setv][keyv]

    def remove(self, setv, keyv):
        if setv in RankingSystem.__sets and keyv in RankingSystem.__sets[setv]:
            RankingSystem.__sets[setv].pop(keyv)

    def size(self, setv):
        if setv in RankingSystem.__sets:
            return len(RankingSystem.__sets[setv])
        return 0
    
    def score(self, setv, keyv):
        if setv in RankingSystem.__sets and keyv in RankingSystem.__sets[setv]:
            return RankingSystem.__sets[setv][keyv]
        return 0

    def range(self, sets, ranges):
        res = []
        for s in sets:
            if s in RankingSystem.__sets:
                res.extend([(k,v) for k,v in RankingSystem.__sets[s].items() if v>=ranges[0] and v<=ranges[1]])
        return res

class CloseConnection(Exception):
    pass

rankingSystem = RankingSystem()

def GetRankingSystem():
    global rankingSystem
    return rankingSystem

def RequestHandler(*args):
    # print >> sys.stderr, "\nRankingSystem", GetRankingSystem()
    print >> sys.stderr, "\nargs", args
    print >> sys.stderr, "\nRequestHandler..", 
    rankSys = GetRankingSystem()
    opt = args[0]
    args = args[1:]
    print >> sys.stderr, "opt", opt,
    if opt == 1:
        rankSys.add(*args)
        print >> sys.stderr, 0,
        return struct.pack("!i", 0)
    elif opt == 2:
        rankSys.remove(*args)
        print >> sys.stderr, 0,
        return struct.pack("!i", 0)
    elif opt == 3:
        res = rankSys.size(*args)
        print >> sys.stderr, (1 , res),
        return struct.pack("!i", 1) + struct.pack("!i", res)
    elif opt == 4:
        res = rankSys.score(*args)
        print >> sys.stderr, (1 , res),
        return struct.pack("!i", 1) + struct.pack("!i", res)
    elif opt == 5:
        i = args.index(0)
        rangeset = rankSys.range(args[:i],args[i+1:])
        rangeset.sort()
        res = struct.pack("!i", len(rangeset)*2)
        print >> sys.stderr, len(rangeset)*2 , rangeset,
        for k, s in rangeset:
            res +=  struct.pack("!i", k) + struct.pack("!i", s)
        return res
    elif opt == 6:
        print >> sys.stderr, "Close Connection"
        raise CloseConnection


def HandleClient(connection, client_address):
    try:
        data = connection.recv(4)
        while data:
            querysize = struct.unpack("!i", data)[0]
            remainingsize = querysize
            query = []
            while remainingsize>0:
                data = connection.recv(4)
                query.append(struct.unpack("!i", data)[0])
                remainingsize -= 1
            connection.sendall(RequestHandler(*query))
            data = connection.recv(4)
    except Exception, e:
        print >> sys.stderr,  e
    finally:
        print >> sys.stderr, "Closing connection: %s" % connection
        connection.close()
    

def main():
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_address = SERVER_SOCKET_PATH
    print >> sys.stderr, "IP: %s, PORT: %s", server_address
    sock.bind(server_address)
    sock.listen(1)
    while True:
        print >> sys.stderr, 'Waiting for a client'
        
        try:
            connection, client_address = sock.accept()
            print >> sys.stderr, "Client: %s Connection: %s" % (client_address, connection)
            thrd = threading.Thread(target=HandleClient, args=(connection, client_address))
            thrd.start()
        except Exception, e:
            print >> sys.stderr, e
        # finally:
        #     print >> sys.stderr, "Closing connection: %s" % connection
        #     connection.close()
        


if __name__ == '__main__':
    main()
