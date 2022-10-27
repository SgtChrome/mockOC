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
    def __init__(self) -> None:
        pass

    def getOrderIDBytes(self):
        expNameBytes = getOrderID().encode('utf-8')
        expNameLength = len(expNameBytes).to_bytes(2, byteorder='little')
        return expNameLength + expNameBytes

    @staticmethod
    def getTelegram(kind, receiver, sender, data) -> None:
        protocoltype = int('0x40', 16)
        messagetype = int(Telegram.translate[kind], 16).to_bytes(2, byteorder='little')
        receiver = receiver.to_bytes(20, byteorder='little')
        sender = sender.to_bytes(20, byteorder='little')
        # data is always 1 byte for both SCI_P and SCI_LS
        data = data.to_bytes(1, byteorder='little')
        orderID = Telegram.getOrderIDBytes()
        return bytearray([protocoltype]) + messagetype + receiver + sender + data + orderID

class TelegramParser:
    def __init__(self, telegram):
        self.telegram = telegram
        self.protocoltype = hex(telegram[0])
        self.messagetype = hex(int.from_bytes(telegram[1:3], byteorder='little'))
        self.receiver = int.from_bytes(telegram[3:23], byteorder='little')
        self.sender = int.from_bytes(telegram[23:43], byteorder='little')
        self.orderID = None

        self.parsePayload()

    def parsePayload(self):
        pass


class SCI_P(TelegramParser):
    def __init__(self, telegram):
        super().__init__(telegram)
        self.reportedPointPosition = None
        self.degradedPointPosition = None

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

    def parsePayload(self):
        if self.messagetype == 0x0001:
            self.signalAspect = self.telegram[43]
            #self.orderIDLenght = int.from_bytes(self.telegram[44:46], byteorder='little')
            self.orderID = self.telegram[44:len(self.telegram)-1].decode('utf-8')


class Interlocking():
    def __init__(self, clients, routes) -> None:
        self.interlocking = list(filter(lambda client: client['kind'] == "interlocking", clients))[0]
        clients.remove(self.interlocking)
        self.clients = {k['name']:OC(k) for k in clients}
        self.routes = routes
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
            if elem.kind == "signal" and val == 4:
                continue
            sendOrder(Telegram.getTelegram(orderType[elem.kind], self.clients[elem].rastaID, self.interlocking.rastaID, val))

    def receiveTelegram(self, telegram):
        # receive the telegram and set the state of the client
        name = next((x for x in self.clients.values() if x.rastaID == telegram.sender), None)
        if type(telegram) == SCI_P:
            self.clients[name].state = telegram.reportedPointPosition
        elif type(telegram) == SCI_LS:
            self.clients[name].state = telegram.signalAspect
            # CONFIG: this is the loop
            # if the route has been completed, finish the experiment or start the next route
            if any(self.clients[elem].state != val for elem, val in self.routes[self.currentRoute].items()):
                # if the signal is green we can set the other route
                routesSet += 1
                if routesSet < repetitions:
                    self.setRoute(self.routes.keys().filter(lambda x: x != self.currentRoute)[0])
                else:
                    # notify master about finished experiment
                    pass
                return
        # check if all elements for the current route are set
        if self.checkIfRouteIsSet():
            for signal in self.routes[self.currentRoute].items().filter(lambda x: x.kind == "signal" and x.state == 4):
                sendOrder(Telegram.getTelegram('INDICATE_SIGNAL_ASPECT', signal.rastaID, self.interlocking.rastaID, 4))

    def checkIfRouteIsSet(self):
        # check if all elements for the current route are set
        # except for the signals that are supposed to be green
        if any(self.clients[elem].state != val for elem, val in self.routes[self.currentRoute].items().filter(lambda elem: not (elem[1].kind == "signal" and elem[0].state == 4))):
            return False
        else:
            return True

    def loop(self):
        while True:
            interlockingEvent.wait()
            self.setRoute(self.routes.values()[0])
            interlockingEvent.clear()


def loop(self):
    while routesSet < repetitions:
        self.setRoute(self.currentRoute)
        time.sleep(waittime)
        self.setRoute(self.currentRoute)
        time.sleep(waittime)


class RaSTA:
    """This is a rasta component"""
    def __init__(self, name, ID, blueIP, greyIP) -> None:
        self.name = name
        self.rastaID = ID
        self.blueIP = blueIP
        self.greyIP = greyIP
        self.clients = []


class OC(RaSTA):
    def __init__(self, dicti) -> None:
        super().__init__(dicti['name'], dicti['rastaID'], dicti['blueIP'], dicti['greyIP'])
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


def handleTelegram(telegram):
    logging.info(f'Epoch:{getTimestamp()} - [Interlocking_RECEIVED]{telegram.orderID}')
    interlockingMutex.acquire()
    inst.receiveTelegram(telegram)
    interlockingMutex.release()


class MyUDPHandler(socketserver.BaseRequestHandler):

    def handle(self):
        data = self.request[0].strip()
        print(datetime.now().strftime("%H:%M:%S-%f") + ' - ' + self.client_address[0] + ': ' + data)
        if data[0] == 0x30:
            self.handleTelegram(SCI_LS(data))
        elif data[0] == 0x40:
            self.handleTelegram(SCI_P(data))

        elif data[0] == 0x03:
            global routesSet, repetitions, expName, waittime, interlockingEvent
            print('Received experiment start', )
            routesSet = 0
            repetitions = int.from_bytes(data[1:3], byteorder='little')
            waittime = int.from_bytes(data[3:5], byteorder='little') / 1000
            expName = bytes(data[5:]).decode('utf-8')

            interlockingEvent.set()

        elif data[0] == 0x04:
            exit()


class ThreadedUDPServer(socketserver.ThreadingMixIn, MyUDPHandler):
    pass

def sendOrder(order):
    try:
        UDPClientSocket.sendto(str.encode(order), (RASTAIP, RASTAPORT))
        logging.info(f'Epoch:{getTimestamp()}-[STEP1]{order}')
        #print(f'Epoch:{getTimestamp()} - [Interlocking_SENT]:{order}')
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
    print(os.path.dirname(os.path.abspath(__file__)))
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

    logging.basicConfig(handlers=[
            #logging.FileHandler("logs/OClog.log"),
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
