import socket
import struct

HOST = '127.0.0.1'
PORT = 22200

if __name__ == '__main__':
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        msg = s.recv(1024)
        receive_size, receive_rate = struct.unpack('If', msg)
        print(receive_size, receive_rate)

        msg = s.recv(receive_size)
        received = struct.unpack('1080f', msg)
        print(received[0], received[1079])
        s.close()