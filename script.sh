#!/bin/sh 

# list the TCP process bound to port PORT
fuser -k 32000/udp
# Example: list the TCP process bound to port 8080
fuser -k 32005/udp

# list the UDP process bound to port PORT
fuser -k 32002/udp
# Example: list the UDP process bound to port 8080
fuser -k 32003/udp

fuser -k 32006/udp
# Example: list the TCP process bound to port 8080
fuser -k 32004/udp

# list the UDP process bound to port PORT
fuser -k 32010/udp   

fuser -k 32011/udp 

fuser -k 32012/udp   

fuser -k 32013/udp 