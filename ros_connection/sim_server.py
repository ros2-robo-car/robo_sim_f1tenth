import socket
import select
import struct

class sim_server:
    host = ''
    port = ''
    server = None
    client = None
    send_size = 4320
    receive_size = 1024
    onReceive = lambda x: None

    def __init__(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def bind(self, host, port):
        self.host = host
        self.port = port
        self.server.bind((host, port))

    def serve(self):

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

        self.server.listen()
        observeSockets = [self.server]

        while True:
            readers, writers, _ = select.select(observeSockets, observeSockets, [], 5.0)

            for reader in readers:
                if reader == self.server:
                    if self.client == None:
                        self.client, _ = self.server.accept()
                        observeSockets.append(self.client)

                        msg = struct.pack('If', *[self.send_size, 40])
                        sendsize = sendToClient(msg)

                        print(f"connection from {self.client.getsockname()} accepted: ", end='')
                        print(f"{self.send_size} bytes with {40}hz")

                    else:
                        client, _ = self.server.accept()

                        msg = struct.pack('If', *[0, 0])
                        client.send(msg)
                        client.close()

                        print(f"connection from ${client.getsockname()} refused")

            for writer in writers:
                if writer == self.client:

                    # test msg
                    msg = struct.pack('1080f', *[i for i in range(1080)])
                    sendsize = sendToClient(msg)

if __name__ == '__main__':
    HOST = '127.0.0.1'
    PORT = 22200
    server = sim_server()
    server.bind(HOST, PORT)
    server.serve()