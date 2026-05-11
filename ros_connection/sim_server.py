from .constants import *
from .packet_formatter import *
import heapq, queue, asyncio, threading, time

RECEIVE_UNIT = 8192

class sim_server:

    _client_reader = None
    _client_writer = None
    _client_timeout = 30

    _send_line = queue.Queue()
    _latest_send_id = 0
    _recv_line = queue.Queue()
    _latest_recv_id = 0
    _recv_stash = []

    _gym_online_event = None
    _client_online_event = None
    _terminated_event = None

    _recv_thread = None
    _listening_loop = None
    
    def __init__(self, gym_online_event, client_online_event, terminated_event):
        self._gym_online_event = gym_online_event
        self._client_online_event = client_online_event
        self._terminated_event = terminated_event

    def send(self, msg, block=True):
        self._send_line.put(msg, block)

    def recv(self, block = True, timeout = None):
        try:
            msg = self._recv_line.get(block, timeout)
            if get_type(msg) != MSGTYPE.SEND:
                print(f"recv: {get_type(msg).name}")
            return msg
        except queue.Empty:
            return None
    
    def flush(self):
        with self._send_line.mutex:
            while self._send_line._qsize() > 0:
                self._send_line._get()

        with self._recv_line.mutex:
            while self._recv_line._qsize() > 0:
                self._recv_line._get()
        
        self._latest_send_id = 0
        self._latest_recv_id = 0
        self._recv_stash.clear()

    def serve(self, host, port):
        asyncio.run(self._serve(host, port))

    async def _serve(self, host, port):
        asyncio.create_task(self._wait_for_terminate())
        self._listening_loop = asyncio.get_running_loop()
        server = await asyncio.start_server(self._accept_init_sim, host, port)
        print(f"Sim Server is started on {host}:{port}")
        print("Listening...")
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            pass
        print("Sim Server closed.")

    async def _wait_for_terminate(self):
        while not self._terminated_event.is_set():
            await asyncio.sleep(1)

        await self._response_error("Sim server was closed.")
        await self._close_client()
        for task in asyncio.all_tasks():
            task.cancel()
        self._recv_thread.join()

    async def _response_error(self, msg: str, msgtype: MSGTYPE, writer: asyncio.StreamWriter = None):
        if writer == None:
            writer = self._client_writer
        if writer == None or writer.is_closing():
            return
        
        if msgtype == MSGTYPE.INIT_RESPONSE:
            base = {}
        elif msgtype == MSGTYPE.START_RESPONSE:
            base = {'timestep': 0.0, 'flags': 0, 'map': ''}
        else:
            base = {}
        
        print(f"Connection from {writer.get_extra_info('peername')} refused: {msg}")
        err_response = {
            'status': STATUS.FAILURE,
            'msg': msg
        }
        err_response |= base

        send_msg = pack(msgtype, err_response)
        send_msg = header_parser.pack(len(send_msg)) + send_msg
        writer.write(send_msg)
        await writer.drain()

    async def _close_client(self, e = None):
        await self._close_writer(e, self._client_writer)
        self._gym_online_event.clear()
        self._client_online_event.clear()

        self._client_reader = None
        self._client_writer = None
        self.flush()

    async def _close_writer(self, e = None, writer = None):
        if writer == None:
            writer = self._client_writer
        if writer == None:
            return
        
        if e != None:
            print(f"Connection from {writer.get_extra_info('peername')} closed: {e}")
        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass

    # INIT_REQUEST: from client, to sim
    async def _accept_init_sim(self, reader, writer):
        if self._client_writer != None and not self._client_writer.is_closing():
            await self._response_error('Another client is using simulation.', MSGTYPE.INIT_RESPONSE, writer)
            return
        self._client_reader = reader
        self._client_writer = writer

        try:
            init_req_raw = await self._receive_from_client()
            if len(init_req_raw) == 0:
                raise ConnectionError('Disconnect')
            msgtype, init_req = unpack(init_req_raw)

            self._client_timeout = max(init_req['timeout'], 0)
        except Exception as e:
            await self._response_error(str(e), MSGTYPE.INIT_RESPONSE)
            await self._close_client()
            return

        if msgtype != MSGTYPE.INIT_REQUEST:
            await self._response_error(f'Expected INIT_REQUEST, Received {msgtype.name} ({msgtype})', MSGTYPE.INIT_RESPONSE)
            await self._close_client()
            return

        self._client_online_event.set()
        self._recv_line.put(init_req_raw)
        asyncio.create_task(self._response_init_sim())

    # INIT_RESPONSE: from sim, to client
    async def _response_init_sim(self):
        init_res_raw = self._send_line.get()

        send_msg = header_parser.pack(len(init_res_raw)) + init_res_raw
        self._client_writer.write(send_msg)
        await self._client_writer.drain()
        
        print(f"Connection from {self._client_writer.get_extra_info('peername')} accepted")
        asyncio.create_task(self._accept_start_sim())

    # START_REQUEST: from client, to sim
    async def _accept_start_sim(self):
        start_time = time.time()
        start_req_raw = None

        while time.time() - start_time < self._client_timeout:
            start_req_raw = None
            try:
                print(f"accepting {time.time()}")
                start_req_raw = await asyncio.wait_for(
                    self._receive_from_client(),
                    self._client_timeout - (time.time() - start_time)
                )
                print(start_req_raw)

                if start_req_raw == None:
                    continue

                msgtype = get_type(start_req_raw)
                if msgtype != MSGTYPE.START_REQUEST:
                    continue

            except asyncio.TimeoutError:
                continue
            else:
                break
        
        if start_req_raw == None:
            await self._response_error(f'Time out while waiting for START_REQUEST', MSGTYPE.START_RESPONSE)
            await self._close_client()
            return

        self._recv_line.put(start_req_raw)
        asyncio.create_task(self._response_start_sim())

    # START_RESPONSE: from sim, to client
    async def _response_start_sim(self):
        start_res_raw = self._send_line.get()

        msgtype, start_res = unpack(start_res_raw)
        if msgtype != MSGTYPE.START_RESPONSE:
            await self._response_error(f'Expected START_RESPONSE, Received {msgtype.name} ({msgtype})', MSGTYPE.START_RESPONSE)
            await self._close_client()
            return

        if start_res['status'] == STATUS.FAILURE:
            await self._response_error(start_res['msg'], MSGTYPE.START_RESPONSE)
            await self._close_client()
            return

        if start_res['status'] == STATUS.ERROR:
            await self._response_error(start_res['msg'], MSGTYPE.START_RESPONSE)

        if start_res['flags'] & SIMFLAGS.ASYNC:
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
        
        send_msg = header_parser.pack(len(start_res_raw)) + start_res_raw
        self._client_writer.write(send_msg)
        await self._client_writer.drain()

    # thread waitable
    async def _serve_sync(self):
        while self._gym_online_event.is_set():
            try:
                msg = self._send_line.get(True, 1.0) # blocked in isolated thread 
            except queue.Empty:
                continue
            asyncio.run_coroutine_threadsafe(self._send_to_client(msg), self._listening_loop)

            future = asyncio.run_coroutine_threadsafe(self._receive_from_client(), self._listening_loop)
            msg = future.result()
            if len(msg) > 0:
                self._recv_line.put(msg, False)
        asyncio.run_coroutine_threadsafe(self._accept_start_sim(), self._listening_loop)

    async def _serve_async_receive_from_client(self):
        while self._gym_online_event.is_set():
            msg = await self._receive_from_client()
            if len(msg) > 0:
                self._recv_line.put(msg, False)
        asyncio.create_task(self._accept_start_sim())

    # thread waitable
    async def _serve_async_send_to_client(self):
        while self._gym_online_event.is_set():
            try:
                msg = self._send_line.get(True, 1.0) # blocked in isolated thread 
            except queue.Empty:
                continue
            asyncio.run_coroutine_threadsafe(self._send_to_client(msg), self._listening_loop)

    async def _send_to_client(self, msg):
        try:
            msg = header_parser.pack(len(msg), self._latest_send_id) + msg
            self._client_writer.write(msg)
            await self._client_writer.drain()
            self._latest_send_id = (self._latest_send_id + 1) % (UINT_MAX + 1)
        except Exception as e:
            await self._close_client(e)

    async def _receive_from_client(self):
        try:
            # assure sequence
            while True:
                if self._recv_stash[0][0] == self._latest_recv_id:
                    return heapq.heappop(self._recv_stash)[1]
                
                recv = await self._client_reader.read(8)
                if len(recv) == 0:
                    raise ConnectionError('Disconnect')
                
                msglen, msgid = header_parser.unpack(recv)
                msg = await self._client_reader.readexactly(msglen)
                if msgid == self._latest_recv_id:
                    self._latest_recv_id = (self._latest_recv_id + 1) % (UINT_MAX + 1)
                    return msg
                else:
                    heapq.heappush(self._recv_stash, (msgid, msg))

        except asyncio.IncompleteReadError as e:
            print(f"Expected {e.expected}bytes, Recieved {len(e.partial)}bytes.")
            return b''
        except Exception as e:
            await self._close_client(e)
            return b''