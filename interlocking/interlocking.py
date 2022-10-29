import socketserver
import threading
import socket

import os
import logging
from datetime import datetime
import time
from time import sleep
from typing import Dict
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
interlockingMutex = threading.Lock()
interlockingEvent = threading.Event()
loopThread = threading.Thread()
countReceived = 0
routesSet = 0


class Telegram:
    translate = {
        # Stellbefehle
        'LEFT': 1,
        'RIGHT': 0,
        # Message types SCI_P
        'MOVE_POINT': '0x0001',
        'POINT_POSITION': '0x000B',
        # Message types SCI_LS
        'INDICATE_SIGNAL_ASPECT': '0x0001',
    }
    def __init__(self, kind, receiver, sender, data) -> None:
        self.protocoltype = int('0x40', 16)
        self.messagetype = int(Telegram.translate[kind], 16)
        self.receiver = receiver
        self.sender = sender
        # data is always 1 byte for both SCI_P and SCI_LS
        self.data = data
        self.orderID = getOrderID()

    def getOrderIDBytes():
        expNameBytes = getOrderID()
        #expNameLength = len(expNameBytes).to_bytes(2, byteorder='little')
        return expNameBytes

    def toBytes(self) -> None:
        protocoltype = self.protocoltype
        messagetype = self.messagetype .to_bytes(2, byteorder='little')
        receiver = (self.receiver + 48).to_bytes(20, byteorder='little')
        sender = (self.sender + 48).to_bytes(20, byteorder='little')
        # data is always 1 byte for both SCI_P and SCI_LS
        data = self.data.to_bytes(1, byteorder='little')
        pufferbyte = int('0x00', 16).to_bytes(1, byteorder='little')
        orderID = self.orderID.encode('utf-8')
        return bytearray([protocoltype]) + messagetype + receiver + sender + data + pufferbyte + orderID

class TelegramParser:
    def __init__(self, telegram):
        self.telegram = telegram
        # rust sends +48 too much right now
        self.protocoltype = telegram[0] - 48
        self.messagetype = int.from_bytes(telegram[1:3], byteorder='little')
        self.receiver = int.from_bytes(telegram[3:23], byteorder='little') - 48
        self.sender = int.from_bytes(telegram[23:43], byteorder='little') - 48
        self.orderID = None

    def parsePayload(self):
        pass


class SCI_P(TelegramParser):
    def __init__(self, telegram):
        super().__init__(telegram)
        self.reportedPointPosition = None
        self.degradedPointPosition = None
        self.parsePayload()

    def parsePayload(self):
        if self.messagetype == 0x000B:
            self.reportedPointPosition = self.telegram[43]
            self.degradedPointPosition = self.telegram[44]
            #self.orderIDLenght = int.from_bytes(self.telegram[45:47], byteorder='little')
            self.orderID = self.telegram[45:len(self.telegram)-1].decode('utf-8')

class SCI_LS(TelegramParser):
    def __init__(self, telegram):
        super().__init__(telegram)
        self.signalAspect = None
        self.parsePayload()

    def parsePayload(self):
        if self.messagetype == 0x0001:
            self.signalAspect = self.telegram[43]
            #self.orderIDLenght = int.from_bytes(self.telegram[44:46], byteorder='little')
            self.orderID = self.telegram[44:len(self.telegram)-1].decode('utf-8')


class Interlocking():
    def __init__(self, clients, routes) -> None:
        self.interlocking = list(filter(lambda client: client['kind'] == "interlocking", clients))[0]
        clients.remove(self.interlocking)
        self.interlocking = RaSTA(self.interlocking)
        self.clients: dict[str, OC] = {k['name']:OC(k) for k in clients}
        self.routes = {list(k.keys())[0]:list(k.values())[0] for k in routes}
        self.currentRoute = None

    def setRoute(self, route):
        self.currentRoute = route
        orderType = {
            'switch': 'MOVE_POINT',
            'signal': 'INDICATE_SIGNAL_ASPECT',
        }
        for elem, val in self.routes[route].items():
            # so elems can either be switch or signal
            # if elem is a signal that needs to be set to green (4) we have to wait
            if self.clients[elem].kind == "signal" and val == 4:
                continue
            sendOrder(Telegram(orderType[self.clients[elem].kind], self.clients[elem].rastaID, self.interlocking.rastaID, val), route + str(routesSet))
            logging.debug(f"Sent order to {elem} to set to {val}")

    def receiveTelegram(self, telegram):
        # receive the telegram and set the state of the client
        elem = next((x for x in self.clients.values() if x.rastaID == telegram.sender), None)

        if type(telegram) == SCI_P:
            self.clients[elem.name].state = telegram.reportedPointPosition
        elif type(telegram) == SCI_LS:
            self.clients[elem.name].state = telegram.signalAspect
            # CONFIG: this is the loop
            # if the route has been completed, finish the experiment or start the next route
            if all(self.clients[elem].state == val for elem, val in self.routes[self.currentRoute].items()):
                logging.debug(f"Route {self.currentRoute} completed")
                for signal in filter(lambda x: self.checkIfGreen(x[0]), self.clients.items()):
                    sendOrder(Telegram('INDICATE_SIGNAL_ASPECT', signal.rastaID, self.interlocking.rastaID, 1), 'reset')
                # if the signal is green we can set the other route
                routesSet += 1
                if routesSet < repetitions:
                    newRoute = filter(lambda x: x != self.currentRoute, self.routes.keys())[0]
                    logging.debug(f"Finished route {routesSet}, starting next route {newRoute}")
                    self.setRoute(newRoute)
                else:
                    # notify master about finished experiment
                    print("Finished experiment")
                return
        # check if all elements for the current route are set
        if self.checkIfRouteIsSet():
            logging.debug(f"Route {self.currentRoute} is set, setting signals to green")
            for signal in filter(lambda x: self.checkIfGreen(x[0]), self.routes[self.currentRoute].items()):
                sendOrder(Telegram('INDICATE_SIGNAL_ASPECT', signal.rastaID, self.interlocking.rastaID, 4), self.currentRoute + str(routesSet))

    def checkIfRouteIsSet(self):
        # check if all elements for the current route are set
        # except for the signals that are supposed to be green
        if any(self.clients[elem].state != val for elem, val in filter(lambda x: not self.checkIfGreen(x[0]), self.routes[self.currentRoute].items())):
            return False
        else:
            return True

    def checkIfGreen(self, name):
        return self.clients[name].kind == "signal" and self.clients[name].state == 4

    def loop(self):
        logging.debug("Interlocking waiting for start of experiment")
        while True:
            interlockingEvent.wait()
            self.setRoute(list(self.routes)[0])
            interlockingEvent.clear()
            logging.debug("Interlocking waiting for start of experiment")


""" def loop(self):
    while routesSet < repetitions:
        self.setRoute(self.currentRoute)
        time.sleep(waittime)
        self.setRoute(self.currentRoute)
        time.sleep(waittime) """


class RaSTA:
    """This is a rasta component"""
    def __init__(self, elemConfig) -> None:
        self.name = elemConfig['name']
        self.rastaID = elemConfig['rastaID']
        self.blueIP = elemConfig['blueIP']
        self.greyIP = elemConfig['greyIP']
        self.kind = elemConfig['kind']
        self.clients = []


class OC(RaSTA):
    def __init__(self, elemConfig) -> None:
        super().__init__(elemConfig)
        self.connection = False
        self.state = None


def getTimestamp():
    return str(round(time.time(), 3)).replace(".", "")


def getOrderID():
    global routesSet, expName
    string = f"{expName}+{str(routesSet)}+{shortuuid.uuid()}"
    routesSet += 1
    return string
    #return shortuuid.uuid()


class MyUDPHandler(socketserver.BaseRequestHandler):

    def handle(self):
        data = self.request[0].strip()
        #print(datetime.now().strftime("%H:%M:%S-%f") + ' - ' + self.client_address[0] + ': ' + str(data))
        if data[0] == 0x30:
            logging.debug(f"Received SCI_LS from {self.client_address[0]}")
            telegram = SCI_LS(data)
            logging.info(f'Epoch:{getTimestamp()} - [Interlocking_RECEIVED][{telegram.messagetype}][{telegram.signalAspect}]{telegram.orderID}')
            interlockingMutex.acquire()
            inst.receiveTelegram(telegram)
            interlockingMutex.release()
        elif data[0] == 0x40:
            logging.debug(f"Received SCI_P from {self.client_address[0]}")
            telegram = SCI_P(data)
            logging.info(f'Epoch:{getTimestamp()} - [Interlocking_RECEIVED][{telegram.messagetype}][{telegram.reportedPointPosition}]{telegram.orderID}')
            interlockingMutex.acquire()
            inst.receiveTelegram(telegram)
            interlockingMutex.release()

        elif data[0] == 0x03:
            global routesSet, repetitions, expName, waittime, interlockingEvent
            logging.debug("Starting experiment")
            routesSet = 0
            repetitions = int.from_bytes(data[1:3], byteorder='little')
            # get waittime in milliseconds
            waittime = int.from_bytes(data[3:5], byteorder='little') / 1000
            expName = bytes(data[5:]).decode('utf-8')

            interlockingEvent.set()

        elif data[0] == 0x04:
            exit()


class ThreadedUDPServer(socketserver.ThreadingMixIn, MyUDPHandler):
    pass

def sendOrder(telegram: Telegram, routesID:str):
    try:
        UDPClientSocket.sendto(telegram.toBytes() + '+'+ routesID.encode('utf-8'), (RASTAIP, RASTAPORT))
        logging.info(f'Epoch:{getTimestamp()}-[INTERLOCKING_SENT][{telegram.messagetype}][{telegram.data}]{telegram.orderID}+{routesID}')
        print(f'Epoch:{getTimestamp()} - [Interlocking_SENT]:{telegram.toBytes()+routesID}')
    except socket.gaierror as err:
        if err.errno == -2:
            print("Rasta socket is down")
        else:
            print(err)

def loopMessages():
    print("Repetitions:", repetitions)
    while(routesSet < repetitions):
        # 0/1 Message/Internal; RastaID_Sender; RastaID_Receiver; orderId - message
        sendOrder("0;%s;%s;%s-left" % (inst.clients['interlocking'].rastaID, inst.clients['switch1'].rastaID, getOrderID()))
        sleep(waittime)
        sendOrder("0;%s;%s;%s-right" % (inst.clients['interlocking'].rastaID, inst.clients['switch1'].rastaID, getOrderID()))
        sleep(waittime)

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    if not os.path.exists('internalConfig.yaml'):
        print("No config file found")
        exit()

    with open("internalConfig.yaml") as configFile:
        try:
            config = yaml.safe_load(configFile)
        except yaml.YAMLError as exc:
            print(exc)
            exit(1)

    if os.path.isfile("routes.yaml"):
        with open("routes.yaml") as configFile:
            try:
                routes = yaml.safe_load(configFile)
            except yaml.YAMLError as exc:
                print(exc)
                exit(1)
    else:
        print("No routes file found")
        exit(1)

    inst = Interlocking(config, routes)
    interlockingThread = threading.Thread(target=inst.loop)
    interlockingThread.daemon = True
    interlockingThread.start()

    logging.root.handlers = []

    logging.basicConfig(
        handlers=[
            #logging.FileHandler("logs/OClog.log"),
            logging.StreamHandler()
        ],
        encoding='utf-8',
        level=logging.DEBUG)
    logging.debug("Logging setup properly")

    #countLock = threading.Lock()
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    receiver = socketserver.UDPServer((HOST, RECEIVERPORT), ThreadedUDPServer)
    server_thread = threading.Thread(target=receiver.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    print("Object controller up!")

    # TEST
    """ sleep(2)
    data = b"\x032\x00,\x010*0*0*300"
    routesSet = 0
    repetitions = int.from_bytes(data[1:3], byteorder='little')
    # get waittime in milliseconds
    waittime = int.from_bytes(data[3:5], byteorder='little') / 1000
    expName = bytes(data[5:]).decode('utf-8')

    interlockingEvent.set() """

    server_thread.join()
