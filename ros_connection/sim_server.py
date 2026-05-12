from .constants import *
from .packet_formatter import *
import heapq, queue, asyncio, threading, time

RECEIVE_UNIT = 8192
DEFAULT_TIMEOUT = 30

class sim_server:

    _client_reader = None
    _client_writer = None
    _client_timeout = 30

    _from_client_line = {
        MSGTYPE.INIT_REQUEST: queue.Queue(),
        MSGTYPE.START_REQUEST: queue.Queue(),
        MSGTYPE.SEND: queue.Queue()
    }
    _to_server_line = {
        MSGTYPE.INIT_REQUEST: queue.Queue(),
        MSGTYPE.START_REQUEST: queue.Queue(),
        MSGTYPE.SEND: queue.Queue()
    }

    _from_server_line = {
        MSGTYPE.INIT_RESPONSE: queue.Queue(),
        MSGTYPE.START_RESPONSE: queue.Queue(),
        MSGTYPE.RECV: queue.Queue()
    }
    _err_line = queue.Queue()

    _latest_send_id = 0
    _latest_recv_id = 0
    _recv_pending_heap = []

    _gym_offline_event = None
    _client_offline_event = None
    _terminated_event = None

    _recv_thread = None
    _listening_loop = None
    
    def __init__(self, gym_online_event, client_online_event, terminated_event):
        self._gym_offline_event = gym_online_event
        self._client_offline_event = client_online_event
        self._terminated_event = terminated_event

    def send(self, msg, msgtype: MSGTYPE, block=True):
        self._from_server_line[msgtype].put(msg, block)

    def recv(self, msgtype: MSGTYPE, block = True, timeout = None):
        try:
            msg = self._to_server_line[msgtype].get(block, timeout)
            return msg
        except queue.Empty:
            return None
    
    def flush(self):
        for line in self._from_client_line.values():
            self._flush_line(line)
        for line in self._to_server_line.values():
            self._flush_line(line)
        for line in self._from_server_line.values():
            self._flush_line(line)
        self._flush_line(self._err_line)

        self._latest_send_id = 0
        self._latest_recv_id = 0
        self._recv_pending_heap.clear()

    def _flush_line(self, line: queue.Queue):
        with line.mutex:
            while line._qsize() > 0:
                line._get()

    # Run in worker thread
    def _dispatch_coroutine(self, coroutine):
        return asyncio.run_coroutine_threadsafe(
            coroutine,
            self._listening_loop
        )

    def serve(self, host, port):
        asyncio.run(self._serve(host, port))

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

    async def _recv_loop(self):
        while not self._client_offline_event.is_set():
            try:
                msg_raw = await self._receive_from_client()
                if len(msg_raw) == 0:
                    continue
                msgtype, msg = unpack(msg_raw)
                self._from_client_line[msgtype].put(msg)
            except asyncio.IncompleteReadError as e:
                print(f"Expected {e.expected}bytes, Recieved {len(e.partial)}bytes.")
            except ConnectionError as e:
                await self._close_client(e)
            except Exception as e:
                print(f"Exception in recv_loop: {str(e)}")

    async def _wait_for_terminate(self):
        await asyncio.to_thread(self._terminated_event.wait)
        await self._close_client()
        for task in asyncio.all_tasks():
            task.cancel()
        if self._recv_thread != None:
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
        await self._send_to_client(send_msg)

    async def _close_client(self, e = None):
        await self._close_writer(e, self._client_writer)
        self._gym_offline_event.set()
        self._client_offline_event.set()

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

    async def _accept(self, reader, writer):
        if self._client_writer != None and not self._client_writer.is_closing():
            await self._response_error('Another client is using simulation.', MSGTYPE.INIT_RESPONSE, writer)
            return
        self._client_reader = reader
        self._client_writer = writer
        self._client_offline_event.clear()
        asyncio.create_task(self._recv_loop())

        # accept INIT_REQUESET
        status = await asyncio.to_thread(self._accept_init_sim)
        if not await self._response_status(status, MSGTYPE.INIT_RESPONSE):
            return
        print(f"Init Request from {self._client_writer.get_extra_info('peername')} arrived.")

        # response INIT_RESPONSE
        status = await asyncio.to_thread(self._response_init_sim)
        if not await self._response_status(status, MSGTYPE.INIT_RESPONSE):
            return
        print(f"Init Request from {self._client_writer.get_extra_info('peername')} accepted.")

        while not self._client_offline_event.is_set():

            # accept START_REQUESET
            status = await asyncio.to_thread(self._accept_start_sim)
            if not await self._response_status(status, MSGTYPE.START_RESPONSE):
                return
            print(f"Start Request from {self._client_writer.get_extra_info('peername')} arrived.")

            # response START_RESPONSE
            status = await asyncio.to_thread(self._response_start_sim)
            if not await self._response_status(status, MSGTYPE.START_RESPONSE):
                return
            print(f"Start Request from {self._client_writer.get_extra_info('peername')} accepted.")

            await asyncio.to_thread(self._gym_offline_event.wait)

    async def _response_status(self, status: STATUS, msgtype: MSGTYPE):
        if status == STATUS.FAILURE:
            err_msg = self._err_line.get()
            await self._response_error(str(err_msg), msgtype)
            await self._close_client()
            return False
        elif status == STATUS.ERROR:
            err_msg = self._err_line.get()
            await self._response_error(str(err_msg), msgtype)
            return True
        else:
            return True

    # INIT_REQUEST: from client, to sim
    # Run in worker thread
    def _accept_init_sim(self):
        try:
            init_req = self._from_client_line[MSGTYPE.INIT_REQUEST].get(timeout=DEFAULT_TIMEOUT)
            self._client_timeout = max(init_req['timeout'], 0)
        except queue.Empty:
            self._err_line.put(ConnectionError('Timeout'))
            return STATUS.FAILURE
        except Exception as e:
            self._err_line.put(e)
            return STATUS.FAILURE
        else:
            self._to_server_line[MSGTYPE.INIT_REQUEST].put(init_req)
            return STATUS.DONE

    # INIT_RESPONSE: from sim, to client
    # Run in worker thread
    def _response_init_sim(self):
        try:
            init_res = self._from_server_line[MSGTYPE.INIT_RESPONSE].get(timeout=self._client_timeout)
            init_res_raw = pack(MSGTYPE.INIT_RESPONSE, init_res)
            self._dispatch_coroutine(self._send_to_client(init_res_raw)).result()
        except queue.Empty:
            self._err_line.put(ConnectionError('Timeout'))
            return STATUS.FAILURE
        except Exception as e:
            self._err_line.put(e)
            return STATUS.FAILURE
        else:
            return STATUS.DONE

    # START_REQUEST: from client, to sim
    # Run in worker thread
    def _accept_start_sim(self):
        try:
            start_req = self._from_client_line[MSGTYPE.START_REQUEST].get(timeout=self._client_timeout)
        except queue.Empty:
            self._err_line.put(ConnectionError('Timeout'))
            return STATUS.FAILURE
        except Exception as e:
            self._err_line.put(e)
            return STATUS.FAILURE
        else:
            self._to_server_line[MSGTYPE.START_REQUEST].put(start_req)
            return STATUS.DONE

    # START_RESPONSE: from sim, to client
    # Run in worker thread
    def _response_start_sim(self):
        try:
            start_res = self._from_server_line[MSGTYPE.START_RESPONSE].get(timeout=self._client_timeout)
            if start_res['status'] == STATUS.FAILURE:
                self._err_line.put(e)
                return STATUS.FAILURE
            if start_res['status'] == STATUS.ERROR:
                self._err_line.put(e)
                return STATUS.ERROR

            if start_res['flags'] & SIMFLAGS.ASYNC:
                self._dispatch_coroutine(asyncio.to_thread(self._serve_async_send_to_client))
                self._dispatch_coroutine(asyncio.to_thread(self._serve_async_receive_from_client))
            else:
                self._dispatch_coroutine(asyncio.to_thread(self._serve_sync))

            start_res_raw = pack(MSGTYPE.START_RESPONSE, start_res)
            self._dispatch_coroutine(self._send_to_client(start_res_raw)).result()
        except queue.Empty:
            self._err_line.put(ConnectionError('Timeout'))
            return STATUS.FAILURE
        except Exception as e:
            self._err_line.put(e)
            return STATUS.FAILURE
        else:
            return STATUS.DONE

    # thread waitable
    # Run in worker thread
    def _serve_sync(self):
        while not self._gym_offline_event.is_set():
            try:
                msg = self._from_client_line[MSGTYPE.SEND].get(True, self._client_timeout)
                if len(msg) > 0:
                    self._to_server_line[MSGTYPE.SEND].put(msg, False)
            except queue.Empty:
                return
            
            try:
                msg = self._from_server_line[MSGTYPE.RECV].get(True, 1.0)
                msg_raw = pack(MSGTYPE.RECV, msg)
            except queue.Empty:
                continue
            self._dispatch_coroutine(self._send_to_client(msg_raw))

    # thread waitable
    # Run in worker thread
    def _serve_async_receive_from_client(self):
        while not self._gym_offline_event.is_set():
            try:
                msg = self._from_client_line[MSGTYPE.SEND].get(True, self._client_timeout)
                if len(msg) > 0:
                    self._to_server_line[MSGTYPE.SEND].put(msg, False)
            except queue.Empty:
                return

    # thread waitable
    # Run in worker thread
    def _serve_async_send_to_client(self):
        while not self._gym_offline_event.is_set():
            try:
                msg = self._from_server_line[MSGTYPE.RECV].get(True, 1.0)
                msg_raw = pack(MSGTYPE.RECV, msg)
            except queue.Empty:
                continue
            self._dispatch_coroutine(self._send_to_client(msg_raw))

    async def _send_to_client(self, msg):
        try:
            msg = header_parser.pack(len(msg), self._latest_send_id) + msg
            self._client_writer.write(msg)
            await self._client_writer.drain()
            self._latest_send_id = (self._latest_send_id + 1) % (UINT_MAX + 1)
        except ConnectionError as e:
            await self._close_client(e)
        except Exception as e:
            print(f"Exception in _send_to_client: {str(e)}")

    async def _receive_from_client(self):
        if self._client_writer.is_closing():
            return b''
        
        # assure sequence
        while True:
            if len(self._recv_pending_heap) > 0 and self._recv_pending_heap[0][0] == self._latest_recv_id:
                return heapq.heappop(self._recv_pending_heap)[1]
            
            recv = await self._client_reader.read(8)
            if len(recv) == 0:
                raise ConnectionError('Disconnect')
            
            msglen, msgid = header_parser.unpack(recv)
            msg = await self._client_reader.readexactly(msglen)
            if msgid == self._latest_recv_id:
                self._latest_recv_id = (self._latest_recv_id + 1) % (UINT_MAX + 1)
                return msg
            else:
                heapq.heappush(self._recv_pending_heap, (msgid, msg))