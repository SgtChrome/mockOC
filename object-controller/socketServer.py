import socketserver
import socket
import logging
from datetime import datetime
from time import sleep
import os

RASTAIP, RASTAPORT = "sender_rasta", 20002
HOST, PORT = "object_controller", 20001
#RASTAIP, RASTAPORT = "localhost", 20002
#HOST, PORT = "localhost", 20001

SWITCHING_TIME = 2
ownid = None

UDPSOCKET = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

def sendMessage(rastaID, message):
    temp = ";".join(["0", ownid, str(int(rastaID, 16)), message])
    UDPSOCKET.sendto(str.encode(temp), (RASTAIP, RASTAPORT))
    print("Sent message", temp)


class MyUDPHandler(socketserver.BaseRequestHandler):
    """
    This class works similar to the TCP handler class, except that
    self.request consists of a pair of data and client socket, and since
    there is no connection the client address must be given explicitly
    when sending data back via sendto().
    """

    def handle(self):
        data = self.request[0].strip()
        socket = self.request[1]
        logging.info(datetime.now().strftime("%H:%M:%S-%f") + ' - ' + self.client_address[0] + ':' + data.decode("utf-8"))
        print(datetime.now().strftime("%H:%M:%S-%f") + ' - ' + self.client_address[0] + ':' + data.decode("utf-8"))

        # 0/1 Message/Internal; RastaID_sender; RastaID_Receiver; message
        data = data.decode("utf-8").split(";")

        if data[0] == '1':
            orderID, message = data[3].split('-')
            match message:
                case 'left':
                    sendMessage(data[1], orderID + '-Received')
                    sleep(SWITCHING_TIME)
                    sendMessage(data[1], orderID + '-Position:left')
                case 'right':
                    sendMessage(data[1], orderID + '-Received')
                    sleep(SWITCHING_TIME)
                    sendMessage(data[1], orderID + '-Position:right')
                case 'startup':
                    sendMessage(data[1], 'startup-startup confirmed')


if __name__ == "__main__":
    if not os.path.exists('id.cfg'):
        print("No config file found")
        exit()
    with open("id.cfg") as configFile:
        ownid = configFile.read()
        print(ownid)

    logging.basicConfig(filename='log.log', encoding='utf-8', level=logging.DEBUG)
    with socketserver.UDPServer((HOST, PORT), MyUDPHandler) as server:
        logging.info("'ObjectController' started!")
        print("'ObjectController' started!")
        server.serve_forever()