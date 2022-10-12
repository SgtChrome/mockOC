import socketserver
import socket
import logging
from datetime import datetime
from time import sleep, time
import os

RASTAIP, RASTAPORT = "sender_rasta", 20002
HOST, PORT = "object_controller", 20001
#RASTAIP, RASTAPORT = "localhost", 20002
#HOST, PORT = "localhost", 20001

SWITCHING_TIME = 2
ownid = None

UDPSOCKET = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

def getTimestamp():
    return str(round(time(), 3)).replace(".", "")

def sendMessage(rastaID, message):
    temp = ";".join(["0", ownid, str(int(rastaID, 16)), message])
    UDPSOCKET.sendto(str.encode(temp), (RASTAIP, RASTAPORT))
    logging.info(f'Epoch:{getTimestamp()} - [Client_SENT] {temp}')


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
        #logging.debug(datetime.now().strftime("%H:%M:%S-%f") + ' - ' + self.client_address[0] + ':' + data.decode("utf-8"))
        #print(datetime.now().strftime("%H:%M:%S-%f") + ' - ' + self.client_address[0] + ':' + data.decode("utf-8"))

        # 0/1 Message/Internal; RastaID_sender; RastaID_Receiver; message
        dataArray = data.decode("utf-8").split(";")

        if dataArray[0] == '0':
            orderID, message = dataArray[3].split('-')
            logging.info(f'Epoch:{getTimestamp()} - [Client_RECEIVED] {data.decode("utf-8")}')
            match message:
                case 'left':
                    sendMessage(dataArray[1], orderID + '-Answer_Received')
                    #sleep(SWITCHING_TIME)
                    #sendMessage(dataArray[1], orderID + '-Answer_Position:left')
                case 'right':
                    sendMessage(dataArray[1], orderID + '-Answer_Received')
                    #sleep(SWITCHING_TIME)
                    #sendMessage(dataArray[1], orderID + '-Answer_Position:right')
                case 'startup':
                    sendMessage(dataArray[1], orderID + '-Answer_startup confirmed')


if __name__ == "__main__":
    if not os.path.exists('id.cfg'):
        print("No config file found")
        exit()
    with open("id.cfg") as configFile:
        ownid = configFile.read()
        #print(ownid)

    logging.basicConfig(handlers=[
            logging.FileHandler("logs/OClog.log"),
            logging.StreamHandler()
        ], encoding='utf-8', level=logging.DEBUG)
    with socketserver.UDPServer((HOST, PORT), MyUDPHandler) as server:
        print("'ObjectController' started!")
        server.serve_forever()