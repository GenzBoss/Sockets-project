import sys
import socket
import random
from random import randint
import string

def get_random_string(length):
# choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str
    #print("Random string of length", length, "is:", result_str)


if len(sys.argv) != 3:
    print("usage: python .'\'peers.py <Managerip> <port>")
    

else:
    
    managerIP = sys.argv[1]
    managerPORT = int(sys.argv[2])

    namelen = randint(1,15)

    peer_name = get_random_string(namelen)

   

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        
      #  while True:
        message = f'Register {peer_name} {managerIP} {managerPORT}'
        addr = (managerIP,managerPORT)
        s.sendto(message.encode(), addr)
        print(s.recvfrom(1024))



