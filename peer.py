import sys
import socket
import random
from random import randint
import string
import json
import threading
import math

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


def recieve(peer):

        while True:      
            mesg, addr = peer.peersocket.recvfrom(1024)
            cmnd = mesg.decode()
            spltcmnd = cmnd.split(' ')
            if spltcmnd[0] == 'set-id':
                ntuple, addr = peer.peersocket.recvfrom(1024)
                dethpeerlist = json.loads(ntuple.decode())
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
                    peer.store(id, pos, record)
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

    registerd = False

    #-------dth data
    n = -1
    i = -1
    left = -1
    right = -1

    dhtinfo = []

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

    



    def register(self, peer_name, ipv4, m_port, p_port):
        self.peer_name = peer_name
        self.ipv4 = ipv4
        self.pport =int(p_port)
        self.mport = int(m_port)
        self.mansocket.bind((self.ipv4, self.mport))
        self.peersocket.bind((self.ipv4, self.pport))
        self.registerd = True

        

    
    def Leader(self, tuples):
        self.i = 0
        self.right = 1
        self.n = len(tuples)
        self.dhtinfo = tuples
        nextip = self.dhtinfo[self.right][1]
        port =  self.dhtinfo[self.right][2]

        nextaddr = (nextip,port)

        state = f'set-id {self.right} {self.n}'

        self.peersocket.sendto(state.encode(), nextaddr)

        self.peersocket.sendto(json.dumps(self.dhtinfo).encode(), nextaddr)
        






    def set_id(self, i, n, ntuple):
        self.i = i
        self.n = n
        self.left = i-1
        if i+1 < n:
            self.right = i+1
        else:
            self.right = 0

        self.dhtinfo = ntuple
        #print(ntuple)
        #print(self.dhtinfo)
        #print(self.right)

        if self.right != 0:
            nextip = self.dhtinfo[self.right][1]
            port =  self.dhtinfo[self.right][2]

            nextaddr = (nextip,port)

            state = f'set-id {self.right} {self.n}'

            self.peersocket.sendto(state.encode(), nextaddr)

            self.peersocket.sendto(json.dumps(self.dhtinfo).encode(), nextaddr)
        
        else:
            nextip = self.dhtinfo[self.right][1]
            port =  self.dhtinfo[self.right][2]

            nextaddr = (nextip,port)
            state = f'Ring complete'
            self.peersocket.sendto(state.encode(), nextaddr)
        


    def hash_table_start(self):
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
                    cmnd = f'Store {self.id} {self.pos}'
                    #print(cmnd)
                    self.peersocket.sendto(cmnd.encode(), nextaddr)
                    recrd = json.dumps(hashrecord).encode()
                    self.peersocket.sendto(recrd, nextaddr)

                x +=1
                #print(f'x= {x}')

        self.ringprintrecords()
        finish = "dht-complete"
        self.mansocket.sendto(finish.encode(), addr)
        
        


    def store(self,id, pos, hashrcd):
        self.id = id
        self.pos = pos
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
                
                
                






if len(sys.argv) != 3:
    print("usage: python .'\'peers.py <Managerip> <port>")
    

else:
    
    managerIP = sys.argv[1]
    managerPORT = int(sys.argv[2])

    register_peer = False

    peerprocess = peer()

    managersocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


        
    while not peerprocess.registerd:

        message = input()

        addr = (managerIP,managerPORT)
        managersocket.sendto(message.encode(), addr)



        reciept, addr = managersocket.recvfrom(1024)
        print(reciept.decode())

        handle = message.split(' ')

        if reciept.decode() == 'SUCCESS' and handle[0] == 'Register':
            peerprocess.register(handle[1], handle[2], handle[3], handle[4])



    t1 = threading.Thread(target=recieve, args=(peerprocess,))

    t1.start()


    #managersocket.sendto(b'OKAY THIS WORKS', ('172.27.125.154', 32006))

        
    while peerprocess.registerd:

        message = input()

        addr = (managerIP,managerPORT)
        peerprocess.mansocket.sendto(message.encode(), addr)



        reciept, addr = peerprocess.mansocket.recvfrom(1024)
        print(reciept.decode())
 
        handle = message.split(' ')
        

        if reciept.decode() == 'SUCCESS' and handle[0] == 'setup-dht':
            reciept, addr = peerprocess.mansocket.recvfrom(1024)
            dhtpeerlist = json.loads(reciept.decode())
            print(dhtpeerlist)
            peerprocess.year = handle[3]
            peerprocess.Leader(dhtpeerlist)
            
            

        



    

