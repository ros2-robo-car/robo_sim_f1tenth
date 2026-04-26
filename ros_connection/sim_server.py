import socket
import select
import struct

LIDAR_COUNT = 1080
LIDAR_RATE = 40

class sim_server:
    host = ''
    port = ''
    server = None
    client = None
    send_size = 4 * LIDAR_COUNT
    receive_size = 1024
    onReceive = lambda x, y: None
    getLiDARData = lambda: [0.] * LIDAR_COUNT

    def __init__(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def bind(self, host, port):
        self.host = host
        self.port = port
        self.server.bind((host, port))

    def serve(self, close_event=None):

        def sendToClient(msg):
            nonlocal observeSockets
            sendsize = 0
            try:
                sendsize = self.client.send(msg)
            except ConnectionError as e:
                print(f"Connection from {self.client.getsockname()} closed: {e}")
                self.client = None
                observeSockets.pop()
            except Exception as e:
                print(e)
            return sendsize
        
        def receiveFromClient(recvSize):
            nonlocal observeSockets
            msg = None
            try:
                msg = self.client.recv(recvSize)
                self.client.setblocking(False)
                while self.client.recv(recvSize): pass
            except BlockingIOError:
                pass
            except ConnectionError as e:
                print(f"Connection from {self.client.getsockname()} closed: {e}")
                self.client = None
                observeSockets.pop()
            except Exception as e:
                print(e)
            return msg

        self.server.listen()
        observeSockets = [self.server]

        while True:
            if close_event != None and close_event.is_set():
                break

            readers, writers, _ = select.select(observeSockets, observeSockets, [], 5.0)

            for reader in readers:
                if reader == self.server:
                    if self.client == None:
                        self.client, _ = self.server.accept()
                        observeSockets.append(self.client)

                        msg = struct.pack('If', *[self.send_size, LIDAR_RATE])
                        sendToClient(msg)

                        print(f"connection from {self.client.getsockname()} accepted: ", end='')
                        print(f"{self.send_size} bytes with {LIDAR_RATE}hz")

                    else:
                        client, _ = self.server.accept()

                        msg = struct.pack('If', *[0, 0])
                        client.send(msg)
                        client.close()

                        print(f"connection from ${client.getsockname()} refused")
                else:
                    msg = receiveFromClient(self.receive_size)
                    try:
                        steer, speed = struct.unpack('2f', msg[0:8])
                        speed = max(speed, 0.)
                        self.onReceive(steer, speed)
                    except Exception as e:
                        print(f"{e}, msg: {msg}")

            for writer in writers:
                if writer == self.client:
                    msg = struct.pack('1080f', *self.getLiDARData())
                    sendToClient(msg)
        
        if self.client != None:
            self.client.close()
        self.server.close()

if __name__ == '__main__':
    HOST = '127.0.0.1'
    PORT = 22200
    server = sim_server()
    server.bind(HOST, PORT)
    server.onReceive = lambda x: print(x)
    server.serve()