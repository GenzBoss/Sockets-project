import sys
import socket
import random
from random import randint
import string
import json
import threading
import math
from colorama import Fore, Back, Style

"""

def get_random_string(length):
# choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str
    #print("Random string of length", length, "is:", result_str)

"""


def find_first_prime(l):
    x = (2 * l) + 1
    while True:
        check = is_prime(x)
        if not check:
            x +=1
        else:
            return x 





def is_prime(n):
    if n <= 1:
        return False
    if n % 2 == 0:
        return n == 2
 
    max_div = math.floor(math.sqrt(n))
    for i in range(3, 1 + max_div, 2):
        if n % i == 0:
            return False
    return True


#for the thread so we can have peer communication and manager communication
def recieve(peer):

    while True:      
        mesg, addr = peer.peersocket.recvfrom(1024)  #it recieves from another peer
        cmnd = mesg.decode()
        spltcmnd = cmnd.split(' ')
        if spltcmnd[0] == 'set-id':
            ntuple, addr = peer.peersocket.recvfrom(1024)  
            dethpeerlist = json.loads(ntuple.decode())  #opposite dump make into object again from bytes 
            #print(dethpeerlist[1][1])
            #print(f'{spltcmnd[1]}, {spltcmnd[2]}')
            peer.set_id(int(spltcmnd[1]), int(spltcmnd[2]), dethpeerlist)
            continue

        if spltcmnd[0] == 'Ring':
            print('ring complete')
            peer.hash_table_start()
            continue

        if spltcmnd[0] == 'Store':
            hashrcd, addr = peer.peersocket.recvfrom(1024)
            id = int(spltcmnd[1])
            if id == peer.i:
                pos = int(spltcmnd[2])
                record = json.loads(hashrcd.decode())
                #print(record)
                s = int(spltcmnd[3])
                peer.store(id, pos, record, s)
                #print(peer.localht)
                continue
            
            else:
                nextaddr = (peer.dhtinfo[peer.right][1], peer.dhtinfo[peer.right][2])
                peer.peersocket.sendto(mesg, nextaddr)
                peer.peersocket.sendto(hashrcd, nextaddr)
                continue

        if cmnd == 'Record':
            print(f'number of record in local ht {peer.numrcd}')
            if peer.right != 0:
                nextaddr = (peer.dhtinfo[peer.right][1], peer.dhtinfo[peer.right][2])
                peer.peersocket.sendto(mesg, nextaddr)

        #query find-event

        if spltcmnd[0] == 'find-event':
            
            eventid = spltcmnd[1]
            origaddr = addr
            peer.find_event(origaddr, eventid)

        if cmnd == 'find-next':

            listindex, addr = peer.peersocket.recvfrom(1024)
            pathlist, addr = peer.peersocket.recvfrom(1024)
            origaddr, addr = peer.peersocket.recvfrom(1024)
            eventid, addr = peer.peersocket.recvfrom(1024)

            peer.next_list(json.loads(listindex.decode()), json.loads(pathlist.decode()), json.loads(origaddr.decode()), eventid.decode())


        # teardown
        if spltcmnd[0] =='teardown':
            peer.localht = {} # delete local hash table
            peer.numrcd = 0

            if spltcmnd[1] == str(peer.i): # teardown message has reached leader
                returnaddr = (peer.dhtinfo[peer.i][1], peer.dhtinfo[peer.i][2])
                finish_msg = "teardown-complete"
                peer.teardowndone = True
                continue

            nextaddr = (peer.dhtinfo[peer.right][1], peer.dhtinfo[peer.right][2])
            peer.peersocket.sendto(cmnd.encode(), nextaddr) # send teardown command to right neighbor

        
        if spltcmnd[0] == 'resetnext':
            newdhtinfobytes, addr = peer.peersocket.recvfrom(1024)
            returnaddr, addr = peer.peersocket.recvfrom(1024)
            newdhtinfo = json.loads(newdhtinfobytes.decode())
            #print(newdhtinfo)
            nextaddr = (peer.dhtinfo[peer.right][1], peer.dhtinfo[peer.right][2] )
            newdhtinfo.append(peer.dhtinfo[peer.i])
            newdhtinfobytes = json.dumps(newdhtinfo).encode()
            peer.i = len(newdhtinfo) - 1
            peer.n -=1
            peer.right = peer.i + 1
            peer.left = peer.i - 1

            if len(newdhtinfo) == peer.n:
                peer.right = 0
                leadaddr = (newdhtinfo[0][1],newdhtinfo[0][2])
                peer.peersocket.sendto(b'updatetupleinfo', leadaddr)
                peer.peersocket.sendto(newdhtinfobytes, leadaddr)
                peer.peersocket.sendto(returnaddr, leadaddr)
                continue

            if len(newdhtinfo) == 0:
                peer.left = -1

            peer.peersocket.sendto(mesg, nextaddr)
            peer.peersocket.sendto(newdhtinfobytes, nextaddr)
            peer.peersocket.sendto(returnaddr, nextaddr)

        
        if cmnd == 'updatetupleinfo':
            newdhtinfobytes, addr = peer.peersocket.recvfrom(1024)
            returnaddr, addr = peer.peersocket.recvfrom(1024)
            newdhtinfo = json.loads(newdhtinfobytes.decode())
            peer.dhtinfo = newdhtinfo
            #print(newdhtinfo)
            nextaddr = (peer.dhtinfo[peer.right][1], peer.dhtinfo[peer.right][2])
            #print(peer.right)
            if peer.right == 0:
                returnaddr = json.loads(returnaddr.decode())
                returnaddrtuple = (returnaddr[0], returnaddr[1])
                peer.peersocket.sendto(b'finished reset',returnaddrtuple)
                peer.peersocket.sendto(newdhtinfobytes, returnaddrtuple)
                continue
            #print(nextaddr)
            #print(mesg)
            peer.peersocket.sendto(mesg, nextaddr)
            peer.peersocket.sendto(newdhtinfobytes, nextaddr)
            peer.peersocket.sendto(returnaddr, nextaddr)


        if cmnd == 'finished reset':
            newdhtinfobytes, addr = peer.peersocket.recvfrom(1024)
            peer.dhtinfo = json.loads(newdhtinfobytes.decode())
            peer.resetdone = True
        
        if spltcmnd[0] == 'rebuild':
            peer.rebuild(spltcmnd[1])

        if spltcmnd[0] == 'RESPONSE':
            if cmnd == 'RESPONSE SUCCESS':
                recd, addr = peer.peersocket.recvfrom(1024)
                entry = json.loads(recd.decode())
                print("[SUCESSS]..." + str(entry) + Fore.WHITE )
            
            else:
                print(Fore.RED + '[NOT FOUND]...' + Fore.WHITE)



        if cmnd == 'updatejoin':
            newdhtinfobytes, addr = peer.peersocket.recvfrom(1024)
            peer.dhtinfo = json.loads(newdhtinfobytes.decode())
            peer.n = len(peer.dhtinfo)

            if peer.i == len(peer.dhtinfo) - 2:
                peer.right = peer.i + 1

            if peer.i == len(peer.dhtinfo) -1:
                finish = b'finished reset'
                nextaddr = (peer.dhtinfo[peer.i][1], peer.dhtinfo[peer.i][2]) 
                peer.peersocket.sendto(finish, nextaddr)
                peer.peersocket.sendto(json.dumps(peer.dhtinfo).encode(), nextaddr)
                continue


            updateid = b'updatejoin'
            nextaddr = (peer.dhtinfo[peer.right][1], peer.dhtinfo[peer.right][2]) 
            peer.peersocket.sendto(updateid, nextaddr)
            peer.peersocket.sendto(json.dumps(peer.dhtinfo).encode(), nextaddr)
            

        if cmnd == 'EXIT':
            sys.exit()
            #print(mesg.decode())
        
        """ 
        while True:
            mesg = self.peersocket.recvfrom(1024).decode()
            print(mesg)"""





class peer:

    mansocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    peersocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    peer_name = ""
    pport = -1
    mport = -1
    ipv4 = ""

    registerd = False  #boolean which check if we already registered ourself using command

    #-------dth data
    n = -1  #number of people in ring
    i = -1   #index of the current peer in ring
    left = -1  #index of left peer in ring
    right = -1  #and right peer in ring index

    dhtinfo = []  #all info off peers in ring like name, ipv4add, pport
    

    #------hash table info

    year = 0
    l= -1
    s = -1
    pos =-1
    id =-1
    
    keys = []

    hashrecord = {}
    localht = {}
    hastableinf0=[]
    numrcd = 0

    #--------------
    teardowndone = False
    resetdone = False


   

    



    def register(self, peer_name, ipv4, m_port, p_port):
        self.peer_name = peer_name
        self.ipv4 = ipv4
        self.pport =int(p_port)
        self.mport = int(m_port)
        self.mansocket.bind((self.ipv4, self.mport))
        self.peersocket.bind((self.ipv4, self.pport))
        self.registerd = True

    # def deregister(self):
        # self.registerd = False
        

    
    def Leader(self, tuples, year):    #not a python tuple but the idea tuple
        self.i = 0
        self.right = 1
        self.n = len(tuples)
        self.dhtinfo = tuples
        nextip = self.dhtinfo[self.right][1]  #ipv4 addres 
        port =  self.dhtinfo[self.right][2]   #pport
        self.year = year
        #print(self.peer_name)
        #print(self.year)

        nextaddr = (nextip,port)  #now give information to next peer that is suppose to participate in ring

        state = f'set-id {self.right} {self.n}'   # set-id 1 3 for example

        self.peersocket.sendto(state.encode(), nextaddr)   #make into bytes

        #the recieve function will handle all the msgs its gonna send

        self.peersocket.sendto(json.dumps(self.dhtinfo).encode(), nextaddr)

        #json dumps to send in like object instead of just string
        






    def set_id(self, i, n, ntuple):
        self.i = i  #next peer repeating the process
        self.n = n
        self.left = i-1
        if i+1 < n:                 
            self.right = i+1  #if not last peer
        else:
            self.right = 0  #for the last peer what we do

        self.dhtinfo = ntuple
        #print(ntuple)
        #print(self.dhtinfo)
        #print(self.right)

        if self.right != 0:
            nextip = self.dhtinfo[self.right][1]   #we do what the leader function did
            port =  self.dhtinfo[self.right][2]     #[0] is name [1] is ipv4 [2] is pport

            nextaddr = (nextip,port)

            state = f'set-id {self.right} {self.n}'

            self.peersocket.sendto(state.encode(), nextaddr)

            self.peersocket.sendto(json.dumps(self.dhtinfo).encode(), nextaddr)
        
        else:
            nextip = self.dhtinfo[self.right][1]
            port =  self.dhtinfo[self.right][2]

            nextaddr = (nextip,port)      #last peer sends to leader peer ring complete msg
            state = f'Ring complete'
            self.peersocket.sendto(state.encode(), nextaddr)
        


    def hash_table_start(self):
        #print(self.peer_name)
        path = f'./1950-1952/details-{self.year}.csv'
        with open(path, 'r') as fp:
            for count, line in enumerate(fp):
                pass
            
        print('Total Lines', count + 1)
        self.l = count
        self.s = find_first_prime(self.l)
        #print(self.s)

        fp = open(path, 'r')

        lines = fp.readlines()
        x = 0
        for line in lines:
            hashrecord={}
            if x == 0:
                self.keys = line.strip().split(',')
                x +=1
            
            else:
                record = line.strip().split(',')
                for key, value in zip(self.keys, record):
                    hashrecord.setdefault(key, value)

                #print(hashrecord)
                self.pos = int(hashrecord["EVENT_ID"]) % self.s
                self.id = self.pos % self.n
                #print(self.id)
                #print(self.i)
                if self.id == self.i:
                    #print(f'Stored record at pos {self.pos}')
                    if bool(self.localht.get(f'{self.pos}')):
                        self.localht[f'{self.pos}'].append(hashrecord)
                        self.numrcd +=1
                    else:
                        self.localht[f'{self.pos}'] = [hashrecord]
                        self.numrcd +=1
                else:
                    nextaddr = (self.dhtinfo[self.right][1], self.dhtinfo[self.right][2])
                    #print(nextaddr)
                    cmnd = f'Store {self.id} {self.pos} {self.s}'
                    #print(cmnd)
                    self.peersocket.sendto(cmnd.encode(), nextaddr)
                    recrd = json.dumps(hashrecord).encode()
                    self.peersocket.sendto(recrd, nextaddr)

                x +=1
                #print(f'x= {x}')

        self.ringprintrecords()
        finish = "dht-complete"
        self.mansocket.sendto(finish.encode(), addr)
        
        


    def store(self,id, pos, hashrcd, s):
        self.id = id
        self.pos = pos
        self.s = s
        if bool(self.localht.get(f'{self.pos}')):
            self.localht[f'{self.pos}'].append(hashrcd)
            #print(f'Stored record at {pos}')
            self.numrcd +=1
            return
        else:
            self.localht[f'{self.pos}'] = [hashrcd]
            #print(f'Stored record at {pos}')
            self.numrcd +=1
            return

    

    def ringprintrecords(self):
        print(f'number of record in local ht {self.numrcd}')
        
        nextaddr = (self.dhtinfo[self.right][1], self.dhtinfo[self.right][2])
        cmnd = f'Record'
        self.peersocket.sendto(cmnd.encode(), nextaddr)

    def find_event(self, origaddr, eventid):
        pos = int(eventid) % self.s
        id = pos % self.n
        #print(self.s)
        #print("pos")
        #print(pos)
        #print("event s peer id")
        #print(id)
        listrange = list(range(self.n))
        pathlist = []
        #print(eventid)

        if id == self.i:
            pathlist.append(self.i)
            if bool(self.localht.get(f'{pos}')):
                if self.localht[f'{pos}'][0]['EVENT_ID'] == eventid:
                    print( f'Path of the query {pathlist}')
                    print(self.localht[f'{pos}'])
                    self.peersocket.sendto(b'RESPONSE SUCCESS', tuple(origaddr))
                    self.peersocket.sendto(json.dumps(self.localht[f'{pos}']).encode(), tuple(origaddr))
                else:
                    print(f'record not found in the list {eventid}')
                    print(f'path of query {pathlist}')
                    self.peersocket.sendto(b'RESPONSE FAILURE', tuple(origaddr))
            else:
                print(f'record not found in the list {eventid}')
                print(f'path of query {pathlist}')
                self.peersocket.sendto(b'RESPONSE FAILURE', tuple(origaddr))
        
        else:
            #print("list here")
            #print(listrange)
            listrange.remove(self.i)
            pathlist.append(self.i)
            nextid = random.choice(listrange)
            #print(nextid)
            nextaddr = (self.dhtinfo[nextid][1], self.dhtinfo[nextid][2])
            #print(self.dhtinfo)
            self.peersocket.sendto(b'find-next', nextaddr)
            self.peersocket.sendto(json.dumps(listrange).encode(), nextaddr)
            self.peersocket.sendto(json.dumps(pathlist).encode(), nextaddr)
            self.peersocket.sendto(json.dumps(origaddr).encode(), nextaddr)
            self.peersocket.sendto(eventid.encode(), nextaddr)



    def next_list(self, listrange, pathlist, origaddr, eventid):
        pos = int(eventid) % self.s
        #print(self.s)
        #print("pos")
        #print(pos)
        #print(self.n)
        id = pos % self.n
        #print("next index id")
        #print(id)
        #print(self.i)
        #print(eventid)
         
        if id == self.i:
            pathlist.append(self.i)
            if bool(self.localht.get(f'{pos}')):
                if self.localht[f'{pos}'][0]['EVENT_ID'] == eventid:
                    print( f'Path of the query {pathlist}')
                    print(self.localht[f'{pos}'])
                    self.peersocket.sendto(b'RESPONSE SUCCESS', tuple(origaddr))
                    self.peersocket.sendto(json.dumps(self.localht[f'{pos}']).encode(), tuple(origaddr))
                else:
                    print(f'record not found in the list {eventid}')
                    print(f'path of query {pathlist}')
                    self.peersocket.sendto(b'RESPONSE FAILURE', tuple(origaddr))
                    
            else:
                print(f'record not found in the list {eventid}')
                print(f'path of query {pathlist}')
                self.peersocket.sendto(b'RESPONSE FAILURE', tuple(origaddr))

        
        else:
            pathlist.append(self.i)

            if len(listrange) == 0:
                print(f'record not found in the list {eventid}')
                print(f'path of query {pathlist}')
                self.peersocket.sendto(b'RESPONSE FAILURE', origaddr)
            #print(self.i)
            #print("listthere")
            #print(listrange)
            listrange.remove(self.i)
            nextid = random.choice(listrange)
            nextaddr = (self.dhtinfo[nextid][1], self.dhtinfo[nextid][2])
            self.peersocket.sendto(b'find-next', nextaddr)
            self.peersocket.sendto(json.dumps(listrange).encode(), nextaddr)
            self.peersocket.sendto(json.dumps(pathlist).encode(), nextaddr)
            self.peersocket.sendto(json.dumps(origaddr).encode(), nextaddr)
            self.peersocket.sendto(eventid.encode(), nextaddr)
        

    def teardown(self):
        command = f'teardown {self.i}'
        nextpeeraddr = (self.dhtinfo[self.right][1], self.dhtinfo[self.right][2] )
        self.peersocket.sendto(command.encode(), nextpeeraddr )
        

        while True:
           
            if self.teardowndone:
                self.teardowndone = False
                print('teardown-complete')
    
                return





    def resetid(self, manaddr):

        newdhtinfo = []
        command = f'resetnext {self.i}'
        returnaddr = (self.dhtinfo[self.i][1], self.dhtinfo[self.i][2])
        nextaddr = (self.dhtinfo[self.right][1], self.dhtinfo[self.right][2])
        self.n = -1  #number of people in ring
        self.i = -1   #index of the current peer in ring
        self.left = -1  #index of left peer in ring
        self.right = -1  #and right peer in ring index
        self.dhtinfo = []
        self.peersocket.sendto(command.encode(), nextaddr)
        self.peersocket.sendto(json.dumps(newdhtinfo).encode(), nextaddr)
        self.peersocket.sendto(json.dumps(returnaddr).encode(), nextaddr)
        #print("im here")
        while True:
            
            if self.resetdone:
                self.resetdone = False
                print('finsihed reset')
                return



    def start_rebuild(self, year):
        rebd = f'rebuild {year}'
        print(rebd)
        self.peersocket.sendto(rebd.encode(), (self.dhtinfo[0][1], self.dhtinfo[0][2]))

    def rebuild(self,year):
        print(year)
        self.Leader(self.dhtinfo, year)

    
    def addid(self,dhtinfobytes):
        #my algorithm is add the new peer at the end of the ring

        self.dhtinfo = json.loads(dhtinfobytes.decode())

        self.dhtinfo.append([self.peer_name, self.ipv4, self.pport])

        self.i = len(self.dhtinfo) - 1
        self.left = self.i - 1
        self.right = 0
        self.n = len(self.dhtinfo)
        updateid = b'updatejoin'
        #update info for all peers
        firstadd = (self.dhtinfo[0][1], self.dhtinfo[0][2])

        self.peersocket.sendto(updateid, firstadd)
        self.peersocket.sendto(json.dumps(self.dhtinfo).encode(), firstadd)
        
        while True:
            
            if self.resetdone:
                self.resetdone = False
                print('finsihed reset')
                return
                
                
                



#main function the peer like c++


if len(sys.argv) != 3:
    print("usage: python .'\'peers.py <Managerip> <port>")
    

else:
    
    managerIP = sys.argv[1]   #manager ip
    managerPORT = int(sys.argv[2])


    register_peer = False

    peerprocess = peer()  #create peer object

    managersocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  #socket to talk to manager


        
    while not peerprocess.registerd:

        message = input()  #whatever we write when peer is running

        addr = (managerIP,managerPORT)  #from parameters
        managersocket.sendto(message.encode(), addr)  #sending input to sockets.py manager



        reciept, addr = managersocket.recvfrom(1024)  #manager usually just sends success/failure
        print(reciept.decode())

        #user our own commanp

        handle = message.split(' ')

        if reciept.decode() == 'SUCCESS' and handle[0] == 'Register':
            peerprocess.register(handle[1], handle[2], handle[3], handle[4])

        if reciept.decode() == 'SUCCESS' and handle[0] == 'exit':
            peerprocess.peersocket.close()
            sys.exit()



    t1 = threading.Thread(target=recieve, args=(peerprocess,))

    t1.start()


    #managersocket.sendto(b'OKAY THIS WORKS', ('172.27.125.154', 32006))

        
    while peerprocess.registerd:

        message = input()

        addr = (managerIP,managerPORT)  #from parameters
        #print(addr)
        peerprocess.mansocket.sendto(message.encode(), addr)

        

        reciept, addr = peerprocess.mansocket.recvfrom(1024)
       
        print(reciept.decode())
 
        handle = message.split(' ')
        

        if reciept.decode() == 'SUCCESS' and handle[0] == 'setup-dht':
            reciept, addr = peerprocess.mansocket.recvfrom(1024) #manager send tuples of everyone in dht list $1.2.1 in pdf
            dhtpeerlist = json.loads(reciept.decode())
            print(dhtpeerlist)
            peerprocess.year = handle[3]
            peerprocess.Leader(dhtpeerlist, peerprocess.year)


        if reciept.decode() == 'SUCCESS' and handle[0] == 'query-dht':
            peertuple,addr = peerprocess.mansocket.recvfrom(1024) #manager sends the tuple of random peer to qeury
            peerinfo = json.loads(peertuple.decode())
            print(peerinfo)
            message = input(Fore.GREEN + "[QUERY]..." )
            peeraddr = (peerinfo[1], peerinfo[2])
            peerprocess.peersocket.sendto(message.encode(), peeraddr)


        if reciept.decode() == 'SUCCESS' and handle[0] == 'teardown-dht':
            peerprocess.teardown()
            complete = f'teardown-complete {peerprocess.peer_name}'
            peerprocess.mansocket.sendto(complete.encode(), addr)

        

        
        if reciept.decode() == 'SUCCESS' and handle[0] == 'leave-dht':
            #take year to help rebuild ht
            year,addr = peerprocess.mansocket.recvfrom(1024)
            #iinitiate teardown
            peerprocess.teardown()
            #resetid AND after completing send the reciept to manager
            peerprocess.resetid(addr)
            #now rebuld the new dht
            peerprocess.start_rebuild(year.decode())

            recieptrebuilt = f'dht-rebuilt {peerprocess.peer_name} {peerprocess.dhtinfo[0][0]}'
            peerprocess.mansocket.sendto(recieptrebuilt.encode(), addr)
            peerprocess.mansocket.sendto(json.dumps(peerprocess.dhtinfo).encode(), addr)
            print('[SENDING]... rebuilt reciept')
        
        

        if reciept.decode() == 'SUCCESS' and handle[0] == 'join-dht':
            #take year to help rebuild ht
            year,addr = peerprocess.mansocket.recvfrom(1024)
            dhtinfobytes , addr = peerprocess.mansocket.recvfrom(1024)
            #first we add the peer to the dht

            peerprocess.addid(dhtinfobytes)

            #then we can iinitiate teardown

            peerprocess.teardown()
                
            

            #and finally rebuild the DHT

            peerprocess.start_rebuild(year.decode())

            recieptrebuilt = f'dht-rebuilt {peerprocess.peer_name} {peerprocess.dhtinfo[0][0]}'
            peerprocess.mansocket.sendto(recieptrebuilt.encode(), addr)
            peerprocess.mansocket.sendto(json.dumps(peerprocess.dhtinfo).encode(), addr)
            print('[SENDING]... rebuilt reciept')
        


     # user process exists application
        if reciept.decode() == 'SUCCESS' and handle[0] == 'deregister':
           
           #lets close our thread first
            peerprocess.peersocket.sendto(b'EXIT', (peerprocess.ipv4, peerprocess.pport))
           #close the sockets
            peerprocess.peersocket.close()
            peerprocess.mansocket.close()
           
           #safely terminate
            sys.exit()

    

