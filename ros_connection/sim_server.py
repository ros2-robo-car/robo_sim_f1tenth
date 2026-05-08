import struct
from .constants import *
from .packet_formatter import pack, unpack
import queue, asyncio, concurrent.futures

RECEIVE_UNIT = 8192
header_parser = struct.Struct('!I')

class sim_server:

    _client_reader = None
    _client_writer = None
    _send_line = queue.Queue()
    _recv_line = queue.Queue()
    _terminated_event = None
    # _thread_waitable_pool = concurrent.futures.ThreadPoolExecutor()

    def send(self, msg, block=True):
        self._send_line.put(msg, block)

    def recv(self, block=True):
        if block:
            return self._recv_line.get()
        else:
            res = None
            try:
                res = self._recv_line.get_nowait()
            except queue.Empty:
                return res

    def serve(self, host, port, terminate_event):
        self._terminated_event = terminate_event
        asyncio.run(self._serve(host, port))
        asyncio.create_task(self._wait_for_terminate())

    async def _wait_for_terminate(self):
        while not self._terminated_event.is_set():
            yield None

        for task in asyncio.all_tasks():
            task.cancel()

    async def _serve(self, host, port):
        server = await asyncio.start_server(self._init_sim, host, port)
        print(f"Sim Server is started on {host}:{port}")
        print("Listening...")
        await server.serve_forever()
        print("Sim Server closed.")

    async def _init_sim(self, reader, writer):
        if self._client_writer != None and not self._client_writer.is_closing():
            await self._response_error(writer, 'Another client is using simulation.')
            return
        self._client_reader = reader
        self._client_writer = writer

        print('asdf')

        try:
            recv_msg = await self._receive_from_client()
            if len(recv_msg) == 0:
                raise ConnectionError('Disconnect')
            t, d = unpack(recv_msg)
        except Exception as e:
            await self._response_error(writer, str(e))
            return

        if t != MSGTYPE.REQUEST:
            await self._response_error(writer, 'Sim server expected REQUEST packet.')
            return
        
        print('asdf')

        loop = asyncio.get_running_loop()
        loop.run_in_executor(
            None,
            self._recv_line.put(recv_msg)
        )
        
        if d['flags'] & SIMFLAGS.ASYNC:
            loop.run_in_executor(
                None,
                self._serve_async_send_to_client
            )
            asyncio.create_task(
                self._serve_async_receive_from_client()
            )
        else:
            loop.run_in_executor(
                None,
                asyncio.create_task(self._serve_sync())
            )
        
        response = {
            'status': STATUS.READY,
            'msg': 'Ready',
            'timestep': 0.0,
            'flags': 0,
            'map': ''
        }
        send_msg = pack(MSGTYPE.RESPONSE, response)
        send_msg = header_parser.pack(len(send_msg)) + send_msg
        writer.write(send_msg)
        await writer.drain()
        print(f"Connection from ${writer.get_extra_info('peername')} accepted")

    async def _response_error(self, writer, msg: str):
        
        print(f"Connection from {writer.get_extra_info('peername')} refused: {msg}")
        if writer.is_closing():
            return

        err_response = {
            'status': STATUS.ERROR,
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
        print("Sim is ready (sync)")
        while not self._client_writer.is_closing():
            msg = self._send_line.get() # blocked in isolated thread 
            asyncio.create_task(self._send_to_client(msg))
            msg = await self._receive_from_client()
            if len(msg) > 0:
                self._recv_line.put(msg, False)
        self._client_writer.wait_closed()

    async def _serve_async_receive_from_client(self):
        print("Sim is ready (async)")
        while not self._client_writer.is_closing():
            msg = await self._receive_from_client()
            if len(msg) > 0:
                self._recv_line.put(msg, False)
        self._client_writer.wait_closed()

    # thread waitable
    def _serve_async_send_to_client(self):
        while not self._client_writer.is_closing():
            msg = self._send_line.get() # blocked in isolated thread 
            asyncio.create_task(self._send_to_client(msg))
        self._client_writer.wait_closed()

    async def _send_to_client(self, msg):
        try:
            msg = header_parser.pack(len(msg)) + msg
            self._client_writer.write(msg)
            await self._client_writer.drain()
        except Exception as e:
            print(f"Connection from {self._client_writer.get_extra_info('peername')} closed: {e}")
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
            print(f"Connection from {self._client_writer.get_extra_info('peername')} closed: {e}")
            self._client_writer.close()
            return b''

if __name__ == '__main__':
    HOST = '127.0.0.1'
    PORT = 22200
    server = sim_server()
    server.bind(HOST, PORT)
    server.on_receive_from_bridge = lambda x: print(x)
    server.serve()