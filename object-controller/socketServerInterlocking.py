import socketserver
import threading
import socket

import os
import logging
from datetime import datetime
import time
from time import sleep
import yaml

import shortuuid

RASTAIP, RASTAPORT = "sender_rasta", 20002
HOST, RECEIVERPORT = "object_controller", 20001
#RASTAIP, RASTAPORT = "localhost", 20002
#HOST, RECEIVERPORT = "localhost", 20001

class RASTA_CODES:
    RASTA_CONNECTION_CLOSED = '0'
    RASTA_CONNECTION_DOWN = '1'
    RASTA_CONNECTION_START = '2'
    RASTA_CONNECTION_UP = '3'
    RASTA_CONNECTION_RETRREQ = '4'
    RASTA_CONNECTION_RETRRUN = '5'

REQUEST_RECONNECT = '0'
REQUEST_CONNECTIONLIST = '1'

inst = None


class RaSTA:
    def __init__(self, name, ID, ip) -> None:
        self.name = name
        self.rastaID = ID
        self.ip = ip
        self.clients = []

    def exportClientIPs(self):
        return '{' + ';'.join(['"' + x.ip + '"' for x in self.clients]) + '}'
    def exportClientIDs(self):
        return '{' + ';'.join(['"' + str(x.rastaID) + '"' for x in self.clients]) + '}'
    def getHexID(self):
        return '#' + f'{self.rastaID:0>8x}'

class Interlocking():
    def __init__(self, clients) -> None:
        self.clients = {k['name']:OC(k) for k in clients}


class OC(RaSTA):
    def __init__(self, dicti) -> None:
        super().__init__(dicti['name'], dicti['rastaID'], dicti['ip'])
        self.connection = False
        self.state = None


def getTimestamp():
    return str(round(time.time(), 3)).replace(".", "")


def getOrderID():
    return shortuuid.uuid()

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
        print(datetime.now().strftime("%H:%M:%S-%f") + ' - ' + self.client_address[0] + ': ' + data.decode("utf-8"))
        #sleep(3)
        data = data.decode("utf-8")
        received = data.split(';')

        cur_thread = threading.current_thread()

        """ if received[0] == '1':
            match received[2]:
                case RASTA_CODES.RASTA_CONNECTION_UP:
                    inst.clients[received[1]].connection = True
                case RASTA_CODES.RASTA_CONNECTION_DOWN | RASTA_CODES.RASTA_CONNECTION_CLOSED:
                    inst.clients[received[1]].connection = False
                case REQUEST_CONNECTIONLIST:
                    for client in received[3].split('-')[:-1]:
                        data = client.split(',')
                        inst.clients[data[0]].connection = bool(data[1])
                        print(client) """

        if received[0] == '0':
            print(received[3])
            orderID, message = received[3].split('-')
            if 'startup confirmed' in message:
                loop_thread = threading.Thread(target=loopMessages)
                loop_thread.daemon = True
                loop_thread.start()
            logging.info(f'Epoch:{getTimestamp()} - [Interlocking_RECEIVED]:{data}')


        # socket.sendto(str.encode(message), (RASTAIP, RASTAPORT))
        # 0/1 Message/Internal; RastaID_Sender; RastaID_Receiver; orderId - message


class ThreadedUDPServer(socketserver.ThreadingMixIn, MyUDPHandler):
    pass

def sendOrder(udpSocket, order):
    try:
        udpSocket.sendto(str.encode(order), (RASTAIP, RASTAPORT))
        logging.info(f'Epoch:{getTimestamp()} - [Interlocking_SENT]:{order}')
        print(f'Epoch:{getTimestamp()} - [Interlocking_SENT]:{order}')
    except socket.gaierror as err:
        if err.errno == -2:
            print("Rasta socket is down")
        else:
            print(err)

def loopMessages():
    while(True):
        sendOrder(UDPClientSocket, "0;%s;%s;%s-left" % (inst.clients['interlocking'].rastaID, inst.clients['switch1'].rastaID, getOrderID()))
        sleep(5)
        sendOrder(UDPClientSocket, "0;%s;%s;%s-right" % (inst.clients['interlocking'].rastaID, inst.clients['switch1'].rastaID, getOrderID()))
        sleep(5)

if __name__ == "__main__":
    print(getTimestamp())

    if not os.path.exists('internalConfig.yaml'):
        print("No config file found")
        exit()

    with open("internalConfig.yaml") as configFile:
        try:
            config = yaml.safe_load(configFile)
        except yaml.YAMLError as exc:
            print(exc)

    inst = Interlocking(config)
    logging.basicConfig(handlers=[
            logging.FileHandler("logs/OClog.log"),
            logging.StreamHandler()
        ], encoding='utf-8', level=logging.DEBUG)

    receiver = socketserver.UDPServer((HOST, RECEIVERPORT), ThreadedUDPServer)
    server_thread = threading.Thread(target=receiver.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    print("Object controller up!")

    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    server_thread.join()

    """ while(True):
        order = input('->')
        orderInput = order.split(' ')
        match orderInput[0]:
            case 'test':
                print('testi')
            case 'ls':
                sendOrder(UDPClientSocket, "1;0;0;" + REQUEST_CONNECTIONLIST)
                for name, client in inst.clients.items():
                    print('\n', name, client.connection)
            case 'left':
                if len(orderInput) == 1:
                    print("no name")
                    continue
                if orderInput[1] not in inst.clients:
                    print("name not in clients")
                    continue
                print('left')
                print(inst.clients[orderInput[1]].rastaID)
                sendOrder(UDPClientSocket, "0;%s;%s;left" % (inst.clients['interlocking0'].rastaID, inst.clients[orderInput[1]].rastaID))

            case 'right':
                if len(orderInput) == 1:
                    print("no name")
                    break
                print('right')
                sendOrder(UDPClientSocket, "0;%s;%s;right" % (inst.clients['interlocking0'].rastaID, inst.clients[orderInput[1]].rastaID))
            case 'reconnect':
                if orderInput[1] in inst.clients.keys():
                    sendOrder(UDPClientSocket, "1;" + orderInput[1] + ";" + REQUEST_RECONNECT)
                else:
                    print(orderInput[1] + " not in connections") """
