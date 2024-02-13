#to be implemented
import socket


#manager class to create required functions
class dht_manager:

    #information of host in the ring network
    states = ("Free", "Leader", "Indth")
    peersocketarray = []
    peersocketinfo = []
    
    

    def register_peer(self, peer_name , ipv4, mport, pport):
        #to be implemented

        #to implement failure block



        #the following implements if we have unique details 

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as peersock:
            peersock.bind(ipv4,pport)
            self.peersocketarray.append(peersock)

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as managerport:
            managerport.bind(ipv4,mport)

        
        registerpeer = {
            "name" : peer_name,
            "ipv4addr" : ipv4,
            "mport" : mport,
            "pport" : pport,
            "state" : "Free"

        }

        self.peersocketinfo.append(registerpeer)

        return "SUCCESS"
            





    
