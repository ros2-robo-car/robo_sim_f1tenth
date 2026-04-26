import socket
import select
import struct
import threading
import keyboard
import time

HOST = '127.0.0.1'
PORT = 22200
INTERVAL = 0.025

keyStateLock = threading.Lock()
keyState = {
    "up": False,
    "down": False,
    "right": False,
    "left": False,
    "q": False
}

def keyEventCallback(e):
    keyStateLock.acquire()
    keyState[e.name] = True if e.event_type == "down" else False
    keyStateLock.release()

def connect():
    steer, speed = 0., 0.
    quitFlag = False

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        msg = s.recv(1024)
        receive_size, receive_rate = struct.unpack('If', msg)
        print(receive_size, receive_rate)

        while not quitFlag:
            curTime = time.time()
            keyStateLock.acquire()
            speed = (keyState["up"] - keyState["down"]) * 5.0
            steer = (keyState["left"] - keyState["right"]) * 1.0
            quitFlag = keyState["q"]
            keyStateLock.release()

            readers, writers, _ = select.select([s], [s], [], 1.0)
            
            for reader in readers:
                msg = reader.recv(receive_size)
                if len(msg) == 4320:
                    received = struct.unpack('1080f', msg)
                    # print(received[0], received[540], received[1079])

            for writer in writers:
                msg = struct.pack('2f', steer, speed)
                writer.send(msg)

            elapsedTime = time.time() - curTime
            if elapsedTime < INTERVAL:
                time.sleep(INTERVAL - elapsedTime)
        s.close()

if __name__ == '__main__':
    keyboard.hook(keyEventCallback)
    t = threading.Thread(target=connect, daemon=True)
    t.start()
    t.join()