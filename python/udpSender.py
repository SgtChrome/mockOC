
import socket
import time

serverAddressPort   = ("object_controller", 20001)
bufferSize          = 1024

# Create a UDP socket at client side
UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
# Send to server using created UDP socket

while True:
    text = 'pups'
    bytesToSend = str.encode(text)
    UDPClientSocket.sendto(bytesToSend, serverAddressPort)

    print("Sent:", text)
    time.sleep(3)