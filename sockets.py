#to be implemented
import socket
import random


#manager class to create required functions
class dht_manager:

    #information of host in the ring network
    _states = ("Free", "Leader", "InDHT")
    _peersocketarray = []
    _peersocketinfo = []
    

    #information about dth
    dhtset = False
    leader_index = -1
    _peerdhtlist = []

    
    

    def register_peer(self, peer_name , ipv4, mport, pport):
        #to be implemented

        #to implement failure block

        for x in self._peersocketarray:
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

    def set_up_dht(self, peer_name, n , year):
        #to implement failure conditions
        if n < 3 or len(self._peersocketinfo < n) or self.dhtset == True:
            return "FAILURE"
        
        namedoesnotexist = True
        leadindex = 0
        for x in self._peersocketinfo:
            if x["name"] == peer_name:
                namedoesnotexist = False
                self.leader_index = leadindex
                break
            leadindex +=1
        
        if namedoesnotexist:
            return "FAILURE"
        
        #implment successful block
        self._peersocketinfo[self.leader_index]["state"] = self._states[1]
        self._peerdhtlist.append(self._peersocketinfo[self.leader_index])

        i = 0

        while i < n:
            x = random(len(self._peersocketinfo))

            if self._peersocketinfo[x]["state"] == self._states[0]:
                i -= 1
                self._peersocketinfo[x]["state"] = self._states[1]
                self._peerdhtlist.append(self._peersocketinfo[x])

        
        print("SUCCESS")

        i = 0

        for peer in self._peerdhtlist:
            print("peer{0} {1} {2}\n", i, peer["ipv4addr"], peer["pport"] )

        
        #to impement creating DHT
        
    
    def manager_start(self):
        HOST = "172.27.125.154"  # Standard loopback interface address (localhost)
        PORT = 4000  # Port to listen on (non-privileged ports are > 1023)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:    
            s.bind((HOST, PORT))
            while True:
                s.listen()
                conn, addr = s.accept()
                with conn:
                    print(f"Connected by {addr}")
                    while True:
                        data = conn.recv(1024)
                        print(f"{str(data, encoding='utf-8')}")
                        if not data:
                            break
                        conn.sendall(data)

    #use fuser -k [PORT]/tcp  to kill processs on a port if u cant reuse it



manager = dht_manager()

manager.manager_start()
        


        
            




            





         


         
        

        




            





    
