#to be implemented
import sys
import socket
import random
import string
import json
from colorama import Fore, Back, Style

#manager class to create required functions
class dht_manager:


    HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
    PORT = -1

    s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM) #udp we use SOCK_DGRAM

    #information of host in the ring network
    _states = ("Free", "Leader", "InDHT")  #() is a tuple which cannot be changed
    _peersocketarray = []    #prob useless
    _peersocketinfo = []
    

    #information about dth
    dhtset = False
    leader_index = -1    #based on peersocketinfo index
    _peerdhtlist = []
    _dthpeerinfo = []

    waitDht = False    #helper functions for dht requirements
    dhtcompleted = False #boolean value to manage contorl flow
    teardowncompleted = False #boolean value for teardown

    #------------------------

    leavepeer = {}
    year = ""
    

    
    

    def register_peer(self, peer_name , ipv4, mport, pport):
        #to be implemented

        #to implement failure block

        for x in self._peersocketinfo:   #[{x1},{x2},{x3},{x4}]    _peerdhtlist = []
            if x["name"] == peer_name:  #{ "name": "127.0.0.1, pport = "4000",..}
                return "FAILURE"

            if x["ipv4addr"] == ipv4 and (x["mport"] == mport or x["pport"] == pport):
                return "FAILURE"

        #the following implements if we have unique details 
            
        #the below comment block is not needed in this function    
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as peersock:
            peersock.bind(ipv4,pport)
            self._peersocketarray.append(peersock)

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as managerport:
            managerport.bind(ipv4,mport)
                                        """
        
        registerpeer = {
            "name" : peer_name,
            "ipv4addr" : ipv4,
            "mport" : mport,
            "pport" : pport,
            "state" : self._states[0]

        }

        self._peersocketinfo.append(registerpeer)  #for first run its like [{x1}, {x2}]

        return "SUCCESS"
    
#-----------------Next Function-------------------------------------------------

    def set_up_dht(self, peer_name, n , year, sendaddr):
        #to implement failure conditions
        if n < 3 or len(self._peersocketinfo) < n or self.dhtset == True:
            self.s.sendto(b'FAILURE', sendaddr)
            return "FAILURE"
        
        
        #which index in the list of peers _peersocketinfo = []
        namedoesnotexist = True
        leadindex = 0
        for x in self._peersocketinfo:
            if x["name"] == peer_name:
                namedoesnotexist = False
                self.leader_index = leadindex
                break
            leadindex +=1
        
        #print(self._peersocketinfo[self.leader_index]["ipv4addr"])
        #print(sendaddr[1])

        #print(self._peersocketinfo[self.leader_index]["mport"] != sendaddr[1])

        if namedoesnotexist or self._peersocketinfo[self.leader_index]["mport"] != sendaddr[1]: 
            self.s.sendto(B'FAILURE', sendaddr)
            return "FAILURE"



        
        #implment successful block
        self._peersocketinfo[self.leader_index]["state"] = self._states[1]
        self._peerdhtlist.append(self._peersocketinfo[self.leader_index])

        

        i = 0

        while i < n-1:
            x = random.randint(0, len(self._peersocketinfo)-1)

            if self._peersocketinfo[x]["state"] == self._states[0]:
                i += 1
                self._peersocketinfo[x]["state"] = self._states[2]
                self._peerdhtlist.append(self._peersocketinfo[x])

        
        self.s.sendto(b'SUCCESS', sendaddr)

        i = 0

        for peer in self._peerdhtlist:
            ipv4 = peer["ipv4addr"]
            pport = peer["pport"]
            print(f"peer{i} {ipv4} {pport}" )
            self._dthpeerinfo.append([peer["name"], ipv4, pport])
            i +=1
            

        
        #print((self._dthpeerinfo))
        self.s.sendto(json.dumps(self._dthpeerinfo).encode(), sendaddr)

       
        while True:
            message, cmdaddr = self.s.recvfrom(1024)
        
            if cmdaddr == sendaddr and message.decode() == 'dht-complete':
                self.dhtcompleted = True
                #print("here")
                print('dht-completed')
                return
            else:
                #print(sendaddr)
                #print(cmdaddr)
                self.s.sendto(b'FAILURE', cmdaddr)


        
        #to impement creating DHT
        
            




        
    
    def manager_start(self, PORT):
        self.PORT = PORT    #update from parameter

        self.s.bind((self.HOST, PORT))
        while True:
            # x = input("hey write your command")
            #if x=="z":
            #   exit()
            #print("here")
            message, cmdaddr = self.s.recvfrom(1024)   #returns a 2-tuple (meg, address)  address -is also to tuple = (msg, (ip4,port))
            print( Fore.GREEN + "[Receiving]... " + Fore.YELLOW + message.decode())  #whatever you in socket.py when u send from peer
            #print(addr)
                  
            command = message.decode()
            spltcmnd = command.split(' ')

            if spltcmnd[0] == 'Register' or spltcmnd[0] == 'register':
                p_name = spltcmnd[1]
                p_addrv4 = spltcmnd[2]
                m_port = int(spltcmnd[3])
                p_port = int(spltcmnd[4])
                msg = self.register_peer(p_name, p_addrv4, m_port, p_port)
                self.s.sendto(msg.encode(), cmdaddr)
                #print(self._peersocketinfo)


                
            if spltcmnd[0] == 'setup-dht':
                lead_name = spltcmnd[1]
                n = int(spltcmnd[2])
                year = spltcmnd[3]
                self.year = year
                msg = self.set_up_dht(lead_name,n,year, cmdaddr )



            if spltcmnd[0] == 'dht-complete':
                #print(cmdaddr)
                if len(spltcmnd) > 1:
                    peer_name = spltcmnd[1] 
                    self.dht_complete(peer_name, cmdaddr)
                

            if spltcmnd[0] == 'query-dht':    #query-dht leader
                peer_name = spltcmnd[1]
                self.query_dht(peer_name, cmdaddr)

            if spltcmnd[0] == 'teardown-dht':
                peer_name = spltcmnd[1]
                self.dht_teardown(peer_name, cmdaddr)

            if spltcmnd[0] == 'teardown-complete':
                peer_name = spltcmnd[1]
                self.teardown_complete(peer_name, cmdaddr)

            
            if spltcmnd[0] == 'join-dht' :
                peer_name = spltcmnd[1]
                self.join_dht(peer_name, cmdaddr)

            if spltcmnd[0] == 'leave-dht' :
                peer_name = spltcmnd[1]
                self.leave_dht(peer_name, cmdaddr)

            if spltcmnd[0] == 'deregister':
                p_name = spltcmnd[1]
                self.deregister(p_name, cmdaddr)


    

    def dht_complete(self, peer_name, sendaddr):
        
        index = 0
        peer_index = -1
        for x in self._peersocketinfo:
            if x["name"] == peer_name:
                namedoesnotexist = False
                peer_index = index
                break
            index +=1
        
        if peer_index != -1:
            sendaddr = (self._peersocketinfo[peer_index]["ipv4addr"], self._peersocketinfo[peer_index]["mport"])

        #print("there")
        #print(sendaddr)
        if self.dhtcompleted and peer_index != -1:
             self.s.sendto(b'SUCCESS', sendaddr) 
        else:
             self.s.sendto(b'FAILURE', sendaddr)


    #use fuser -k [PORT]/tcp  to kill processs on a port if u cant reuse it

    def query_dht(self, peer_name, sendaddr):


        peerindex= -1
        index = 0
        for x in self._peersocketinfo:
            if x["name"] == peer_name:
                peerindex = index
                break
            index +=1

        if peerindex == -1 or self._peersocketinfo[peerindex]["state"] != self._states[0] or not self.dhtcompleted:
            self.s.sendto(b'FAILURE', sendaddr)
            return
        
        dhtpeerind = random.randint(0,len(self._dthpeerinfo) -1)

        self.s.sendto(b'SUCCESS', sendaddr)
        self.s.sendto(json.dumps(self._dthpeerinfo[dhtpeerind]).encode(), sendaddr)




    def leave_dht(self, peer_name, cmdaddr):
        
        index = 0
        peerind = -1
        for x in self._peersocketinfo:
            if x['name'] == peer_name:
                peerind = index
                break
            index +=1
        
        if peerind == -1 or self._peersocketinfo[peerind]['state'] == self._states[0]:
            self.s.sendto(b'FAILURE', cmdaddr)
            return

        leavepeer = self._peersocketinfo[peerind]

        self.s.sendto(b'SUCCESS', cmdaddr)
        self.s.sendto(self.year.encode(), cmdaddr)

        #wait for dht rebuilt complete reciept

        while True:
            message, sendaddr = self.s.recvfrom(1024)
            reciept = message.decode().split()
        
            if cmdaddr == sendaddr and reciept[0] == 'dht-rebuilt':
                newdhtinfo, sendaddr = self.s.recvfrom(1024)
                self._dthpeerinfo = json.loads(newdhtinfo.decode())

                self.dhtcompleted = True
                freeind = next((i for i, item in enumerate(self._peersocketinfo) if item["name"] == reciept[1]), None)
                self._peersocketinfo[freeind]['state'] = self._states[0]
                if freeind != self.leader_index:
                    self._peersocketinfo[self.leader_index]['state'] = self._states[2]
                self.leader_index = next((i for i, item in enumerate(self._peersocketinfo) if item["name"] == reciept[2]), None)
                self._peersocketinfo[self.leader_index]['state'] = self._states[1]
                #print("here")
                print( Fore.GREEN + "[Receiving]... " + Fore.YELLOW + message.decode())
                print( Fore.GREEN + "[Receiving]... " + Fore.YELLOW + str(self._dthpeerinfo))
                return
            else:
                #print(sendaddr)
                #print(cmdaddr)
                self.s.sendto(b'FAILURE', cmdaddr)
        

        






    def join_dht(self, peer_name, cmdaddr):

        index = 0
        peerind = -1
        for x in self._peersocketinfo:
            if x['name'] == peer_name:
                peerind = index
                break
            index +=1

        #print(self._peersocketinfo[peerind])
        
        if peerind == -1 or self._peersocketinfo[peerind]['state'] != self._states[0]:
            self.s.sendto(b'FAILURE', cmdaddr)
            return
        
        joinpeer = self._peersocketinfo[peerind]

        self.s.sendto(b'SUCCESS', cmdaddr)
        self.s.sendto(self.year.encode(), cmdaddr)
        self.s.sendto(json.dumps(self._dthpeerinfo).encode(), cmdaddr)

        
        while True:
            message, sendaddr = self.s.recvfrom(1024)
            reciept = message.decode().split()
        
            if cmdaddr == sendaddr and reciept[0] == 'dht-rebuilt':
                newdhtinfo, sendaddr = self.s.recvfrom(1024)
                self._dthpeerinfo = json.loads(newdhtinfo.decode())
                joinind = next((i for i, item in enumerate(self._peersocketinfo) if item["name"] == reciept[1]), None)
                self._peersocketinfo[joinind]['state'] = self._states[2]

                self.dhtcompleted = True
                print( Fore.GREEN + "[Receiving]... " + Fore.YELLOW + message.decode())
                print( Fore.GREEN + "[Receiving]... " + Fore.YELLOW +  str(self._dthpeerinfo))
                return


            else:
                #print(sendaddr)
                #print(cmdaddr)
                self.s.sendto(b'FAILURE', cmdaddr)   
        


        









    # def dht_rebuilt(self, peer_name, new_leader):


    def dht_teardown(self, peer_name, sendaddr):

        # check if peer is leader
        if peer_name != self._peersocketinfo[self.leader_index]["name"]:
            self.s.sendto(b'FAILURE', sendaddr)
            return
        else:
            self.s.sendto(b'SUCCESS', sendaddr)


        self.teardown_complete(peer_name, sendaddr)
    

        







    def teardown_complete(self, peer_name, sendaddr):



        # wait for teardown-complete
        while True:
            message, cmdaddr = self.s.recvfrom(1024)

            if cmdaddr == sendaddr and message.decode() == f'teardown-complete {peer_name}':
                self.teardowncompleted = True
                self.dhtset = False
                self.dhtcompleted = False
                self._dthpeerinfo = []
                self._peerdhtlist = []
                for x in self._peersocketinfo:
                    x["state"] = self._states[0]
                    self._peerdhtlist = []
                    
                print('teardown-complete')
                return
            else:
                self.s.sendto(b'FAILURE', cmdaddr)
       


    def deregister(self, peer_name, sendaddr):

        deregister_success = False

        for x in self._peersocketinfo:
            if x["name"] == peer_name:
                if x["state"] == self._states[0]:
                    self._peersocketinfo.remove(x)
                    deregister_success = True
                    self.s.sendto(b'SUCCESS', sendaddr)
                    # message = "exit"
                    # self.s.sendto(message.encode(), sendaddr)
                    # deregister_success = True



                if x["state"] == self._states[2] or x["state"] == self._states[1]:
                    self.s.sendto(b'FAILURE', sendaddr) # returns FAILURE if peer state is set to inDHT
                    return


        if not deregister_success:
            self.s.sendto(b'FAILURE', sendaddr) # returns FAILURE if peer state is set to inDHT
            return














#main function like c++

if len(sys.argv) != 2:
    print("usage: python3 socket.py <port>")

else:
    PORT = int(sys.argv[1])   #port number parameter when open sockets.py
    manager = dht_manager()
    manager.manager_start(PORT) #manager_start(port) in class def
     


        
            




            





         


         
        

        




            





    
