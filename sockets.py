#to be implemented
import sys
import socket
import random
import string
import json

#manager class to create required functions
class dht_manager:


    HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
    PORT = -1

    s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

    #information of host in the ring network
    _states = ("Free", "Leader", "InDHT")
    _peersocketarray = []
    _peersocketinfo = []
    

    #information about dth
    dhtset = False
    leader_index = -1
    _peerdhtlist = []
    _dthpeerinfo = []

    waitDht = False
    dhtcompleted = False
    

    
    

    def register_peer(self, peer_name , ipv4, mport, pport):
        #to be implemented

        #to implement failure block

        for x in self._peersocketinfo:
            if x["name"] == peer_name:
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

        self._peersocketinfo.append(registerpeer)

        return "SUCCESS"
    
#-----------------Next Function-------------------------------------------------

    def set_up_dht(self, peer_name, n , year, sendaddr):
        #to implement failure conditions
        if n < 3 or len(self._peersocketinfo) < n or self.dhtset == True:
            self.s.sendto(b'FAILURE', sendaddr)
            return "FAILURE"
        
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
            self._dthpeerinfo.append([i, ipv4, pport])
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
        self.PORT = PORT

        self.s.bind((self.HOST, PORT))
        while True:
            # x = input("hey write your command")
            #if x=="z":
            #   exit()

            message, cmdaddr = self.s.recvfrom(1024)
            print(message.decode())
            #print(addr)
                    
            command = message.decode()
            spltcmnd = command.split(' ')

            if spltcmnd[0] == 'Register':
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
                msg = self.set_up_dht(lead_name,n,year, cmdaddr )



            if spltcmnd[0] == 'dht-complete':
                #print(cmdaddr)
                peer_name = spltcmnd[1] 
                self.dht_complete(peer_name)

    

    def dht_complete(self, peer_name):
        
        index = 0
        peer_index = -1
        for x in self._peersocketinfo:
            if x["name"] == peer_name:
                namedoesnotexist = False
                peer_index = index
                break
            index +=1
        
        sendaddr = ('0',0)
        if peer_index != -1:
            sendaddr = (self._peersocketinfo[peer_index]["ipv4addr"], self._peersocketinfo[peer_index]["mport"])

        #print("there")
        #print(sendaddr)
        if self.dhtcompleted and peer_index != -1:
             self.s.sendto(b'SUCCESS', sendaddr) 
        else:
             self.s.sendto(b'FAILURE', sendaddr)


    #use fuser -k [PORT]/tcp  to kill processs on a port if u cant reuse it




if len(sys.argv) != 2:
    print("usage: python .'\'socket.py <port>")

else:
    PORT = int(sys.argv[1])
    manager = dht_manager()
    manager.manager_start(PORT)
        


        
            




            





         


         
        

        




            





    
