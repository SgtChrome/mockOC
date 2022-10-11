import socketserver
import threading
import socket
import sys

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

repetitions = 100
expName = ''
waittime = 1

inst = None
loopThread = threading.Thread()
countReceived = 0
countSent = 0


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
    global countSent, expName
    string = f"{expName}Exp{str(countSent)}+{shortuuid.uuid()[10:]}"
    countSent += 1
    return string
    #return shortuuid.uuid()

class MyUDPHandler(socketserver.BaseRequestHandler):

    def handle(self):
        data = self.request[0].strip()
        #socket = self.request[1]
        print(datetime.now().strftime("%H:%M:%S-%f") + ' - ' + self.client_address[0] + ': ' + data.decode("utf-8"))
        #sleep(3)
        data = data.decode("utf-8")
        received = data.split(';')

        cur_thread = threading.current_thread()

        global countReceived, countSent, repetitions, expName, waittime, loopThread

        if received[0] == '0':
            #print(received[3])
            orderID, message = received[3].split('-')
            if 'startup confirmed' in message:
                print('startup confirmed')
                return

            logging.info(f'Epoch:{getTimestamp()} - [Interlocking_RECEIVED] {data}')
            countReceived += 1

        elif received[0] == '3':
            print('Received experiment start', received)
            countReceived = 0
            countSent = 0
            repetitions = int(received[1])
            waittime = float(received[2])
            expName = str(received[3])
            if loopThread:
                if loopThread.is_alive():
                    loopThread.join()
            loopThread = threading.Thread(target=loopMessages)
            loopThread.daemon = True
            loopThread.start()

        elif received[0] == '2':
            exit()

        if countReceived == repetitions:
            print('Finished experiment with', repetitions, 'repetitions')

        # socket.sendto(str.encode(message), (RASTAIP, RASTAPORT))
        # 0/1 Message/Internal; RastaID_Sender; RastaID_Receiver; orderId - message


class ThreadedUDPServer(socketserver.ThreadingMixIn, MyUDPHandler):
    pass

def sendOrder(udpSocket, order):
    try:
        udpSocket.sendto(str.encode(order), (RASTAIP, RASTAPORT))
        logging.info(f'Epoch:{getTimestamp()} - [Interlocking_SENT] {order}')
        #print(f'Epoch:{getTimestamp()} - [Interlocking_SENT]:{order}')
    except socket.gaierror as err:
        if err.errno == -2:
            print("Rasta socket is down")
        else:
            print(err)

def loopMessages():
    print("Repetitions:", repetitions)
    while(countSent < repetitions):
        sendOrder(UDPClientSocket, "0;%s;%s;%s-left" % (inst.clients['interlocking'].rastaID, inst.clients['switch1'].rastaID, getOrderID()))
        sleep(waittime)
        sendOrder(UDPClientSocket, "0;%s;%s;%s-right" % (inst.clients['interlocking'].rastaID, inst.clients['switch1'].rastaID, getOrderID()))
        sleep(waittime)

if __name__ == "__main__":
    """ if len(sys.argv) < 2:
        pass
    else:
        #repetitions = int(sys.argv[1])
        #print(f"Repetitions: {str(repetitions)}")
        expName = sys.argv[1]
        print(f"ExpName String: {expName}") """


    """ expName = os.environ.get('EXP_NAME')
    repetitions = int(os.environ.get('REPETITIONS')) if os.environ.get('REPETITIONS') else 10 """

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

    #countLock = threading.Lock()
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    receiver = socketserver.UDPServer((HOST, RECEIVERPORT), ThreadedUDPServer)
    server_thread = threading.Thread(target=receiver.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    print("Object controller up!")

    server_thread.join()
