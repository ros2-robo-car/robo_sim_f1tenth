import struct
from .constants import *
from .packet_formatter import *
import queue, asyncio, threading

RECEIVE_UNIT = 8192

class sim_server:

    _client_reader = None
    _client_writer = None
    _send_line = queue.Queue()
    _recv_line = queue.Queue()
    _gym_online_event = None
    _terminated_event = None
    _recv_thread = None
    _listening_loop = None
    
    def __init__(self, disconnect_event, terminated_event):
        self._gym_online_event = disconnect_event
        self._terminated_event = terminated_event

    def send(self, msg, block=True):
        self._send_line.put(msg, block)

    def recv(self, block = True, timeout = None):
        try:
            return self._recv_line.get(block, timeout)
        except queue.Empty:
            return None
    
    def flush(self):
        self._send_line.mutex.acquire()
        while self._send_line._qsize() > 0:
            self._send_line._get()
        self._send_line.mutex.release()

        self._recv_line.mutex.acquire()
        while self._recv_line._qsize() > 0:
            self._recv_line._get()
        self._recv_line.mutex.release()

    def serve(self, host, port):
        asyncio.run(self._serve(host, port))

    async def _wait_for_terminate(self):
        while not self._terminated_event.is_set():
            await asyncio.sleep(1)

        self._close_client()
        for task in asyncio.all_tasks():
            task.cancel()
        self._recv_thread.join()

    async def _serve(self, host, port):
        asyncio.create_task(self._wait_for_terminate())
        self._listening_loop = asyncio.get_running_loop()
        server = await asyncio.start_server(self._accept, host, port)
        print(f"Sim Server is started on {host}:{port}")
        print("Listening...")
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            pass
        print("Sim Server closed.")

    async def _accept(self, reader, writer):
        if self._client_writer != None and not self._client_writer.is_closing():
            await self._response_error('Another client is using simulation.', writer)
            return
        self._client_reader = reader
        self._client_writer = writer

        try:
            recv_msg = await self._receive_from_client()
            if len(recv_msg) == 0:
                raise ConnectionError('Disconnect')
            msgtype, _ = unpack(recv_msg)
        except Exception as e:
            await self._response_error(str(e))
            return

        if msgtype != MSGTYPE.REQUEST:
            await self._response_error(f'Expected REQUEST, Received {msgtype.name} ({msgtype})')
            return

        self._recv_line.put(recv_msg)
        asyncio.create_task(self._init_sim())

    async def _init_sim(self):
        init_sim_res_msg = self._send_line.get()
        msgtype, init_sim_res = unpack(init_sim_res_msg)
        if msgtype != MSGTYPE.RESPONSE:
            await self._response_error(f'Expected RESPONSE, Received {msgtype.name} ({msgtype})')
            return
            
        if init_sim_res['status'] == STATUS.ERROR:
            await self._response_error(init_sim_res['msg'])
            return

        if init_sim_res['flags'] & SIMFLAGS.ASYNC:
            self._recv_thread = threading.Thread(
                target = asyncio.run,
                kwargs = {'main': self._serve_async_send_to_client()}
            )
            self._recv_thread.start()
            asyncio.create_task(
                self._serve_async_receive_from_client()
            )
        else:
            self._recv_thread = threading.Thread(
                target = asyncio.run,
                kwargs = {'main': self._serve_sync()}
            )
            self._recv_thread.start()

        send_msg = header_parser.pack(len(init_sim_res_msg)) + init_sim_res_msg
        self._client_writer.write(send_msg)
        await self._client_writer.drain()
        
        print(f"Connection from {self._client_writer.get_extra_info('peername')} accepted")


    async def _response_error(self, msg: str, writer: asyncio.StreamWriter = _client_writer):
        if self._client_writer.is_closing():
            return
        
        print(f"Connection from {self._client_writer.get_extra_info('peername')} refused: {msg}")
        err_response = {
            'status': STATUS.FAILURE,
            'msg': msg,
            'timestep': 0.0,
            'flags': 0,
            'map': ''
        }

        send_msg = pack(MSGTYPE.RESPONSE, err_response)
        send_msg = header_parser.pack(len(send_msg)) + send_msg
        writer.write(send_msg)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def _close_client(self):
        if self._client_writer != None:
            print(f"Connection from {self._client_writer.get_extra_info('peername')} closed by server.")
            self._client_writer.close()

    # thread waitable
    async def _serve_sync(self):
        while not self._client_writer.is_closing():
            msg = self._send_line.get() # blocked in isolated thread 
            asyncio.run_coroutine_threadsafe(self._send_to_client(msg), self._listening_loop)
            future = asyncio.run_coroutine_threadsafe(self._receive_from_client(), self._listening_loop)
            msg = future.result()
            if len(msg) > 0:
                self._recv_line.put(msg, False)

    async def _serve_async_receive_from_client(self):
        while not self._client_writer.is_closing():
            msg = await self._receive_from_client()
            if len(msg) > 0:
                self._recv_line.put(msg, False)

    # thread waitable
    async def _serve_async_send_to_client(self):
        while not self._client_writer.is_closing():
            msg = self._send_line.get() # blocked in isolated thread 
            asyncio.run_coroutine_threadsafe(self._send_to_client(msg), self._listening_loop)

    async def _send_to_client(self, msg):
        try:
            msg = header_parser.pack(len(msg)) + msg
            self._client_writer.write(msg)
            await self._client_writer.drain()
        except Exception as e:
            if self._gym_online_event.is_set():
                print(f"Connection from {self._client_writer.get_extra_info('peername')} closed: {e}")
                self._gym_online_event.clear()
            self._client_writer.close()

    async def _receive_from_client(self):
        try:
            recv = await self._client_reader.read(4)
            if len(recv) == 0:
                raise ConnectionError('Disconnect')
            msglen = header_parser.unpack(recv)[0]
            msg = await self._client_reader.readexactly(msglen)
            return msg
        except asyncio.IncompleteReadError as e:
            print(f"Expected {e.expected}bytes, Recieved {len(e.partial)}bytes.")
            return b''
        except Exception as e:
            if self._gym_online_event.is_set():
                print(f"Connection from {self._client_writer.get_extra_info('peername')} closed: {e}")
                self._gym_online_event.clear()
            self._client_writer.close()
            return b''

if __name__ == '__main__':
    HOST = '127.0.0.1'
    PORT = 22200
    server = sim_server()
    server.bind(HOST, PORT)
    server.on_receive_from_bridge = lambda x: print(x)
    server.serve()