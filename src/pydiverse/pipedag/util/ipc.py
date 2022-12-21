from __future__ import annotations

import ipaddress
import os
import struct
import threading
import uuid
from functools import cached_property
from typing import Any

import msgpack
import pynng
import structlog

from pydiverse.pipedag.errors import IPCError


def human_thread_id(thread_ident):
    """Return human processable number for thread ID."""
    return hash(thread_ident) % 100


class IPCServer(threading.Thread):
    """Server for inter process communication

    FORMAT:
        REQUEST: [nonce, payload]
        RESPONSE: [payload]
    """

    def __init__(
        self,
        listen="tcp://127.0.0.1:0",
        msg_default=None,
        msg_ext_hook=None,
    ):
        super().__init__(name="IPCServer", daemon=True)

        # Reduce recv_timeout when running pytest for better performance
        is_running_pytest = "PYDIVERSE_PIPEDAG_PYTEST" in os.environ
        recv_timeout = 200 if not is_running_pytest else 10

        self.socket = pynng.Rep0(listen=listen, recv_timeout=recv_timeout)
        self.nonces = set()

        self.__stop_flag = False
        self.__thread = None
        self.logger = structlog.get_logger(cls=type(self).__name__)

        self.msg_default = msg_default
        self.msg_ext_hook = msg_ext_hook

    def _get_id(self, ctx_id=0):
        return dict(
            address=self.address,
            thread=human_thread_id(threading.get_ident()),
            ctx_id=ctx_id,
        )

    def run(self):
        self.logger.info("start IPCServer", **self._get_id())
        self.run_loop()

    def stop(self):
        self.__stop_flag = True
        self.logger.debug("request IPCServer to stop", **self._get_id())

    def __enter__(self):
        self.__thread = self
        self.__thread.start()
        self.logger.debug(
            "started IPCServer thread", thread=human_thread_id(self.__thread.ident)
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        if self.__thread is not None:
            self.logger.debug(
                "stop IPCServer thread", thread=human_thread_id(self.__thread.ident)
            )
            self.__thread.join()

    def run_loop(self):
        """
        Run main loop for responding to client requests to this server process.

        We rely on a fixed request-response pattern of pynng library (Req0/Rep0)
        which requires us to open a context for every request-response pair.
        We create threads for determining and sending responses and we keep 10
        such threads in-flight. At the end we join all 10 threads even though
        some threads might actually already have completed and may even share the
        same ident number. We log thread IDs as hash(thread.ident) % 100 for
        receiving human processable numbers.
        """
        max_threads_in_flight = 10  # keep up to 10 threads in flight

        socket = None
        ctx_id = 0
        threads = []
        try:
            socket = self.socket.new_context()
            ctx_id += 1
            self.logger.debug("new socket context", **self._get_id(ctx_id))
            while not self.__stop_flag:
                # noinspection PyBroadException
                try:
                    data = socket.recv()
                    unpacker = msgpack.Unpacker(
                        use_list=False, ext_hook=self.msg_ext_hook
                    )
                    unpacker.feed(data)

                    # Expected: (NONCE, PAYLOAD)
                    if unpacker.read_array_header() != 2:
                        raise IPCError(
                            "Expected request to contain exactly two fields:"
                            " nonce and payload."
                        )
                    nonce = unpacker.unpack()
                    nonce_hex = nonce.hex()

                    if nonce in self.nonces:
                        # Already processing this request
                        self.logger.warning(
                            "Already processing request",
                            nonce=nonce_hex,
                            **self._get_id(ctx_id),
                        )
                        socket = self.socket.new_context()
                        ctx_id += 1
                        self.logger.debug("next socket context", **self._get_id(ctx_id))
                        continue

                    self.nonces.add(nonce)

                    if len(threads) >= max_threads_in_flight:
                        self.logger.debug(
                            "joining thread", thread=human_thread_id(threads[0].ident)
                        )
                        threads[0].join()
                        del threads[0]
                    thread_logger = structlog.get_logger()
                    thread = threading.Thread(
                        name="IPC Worker",
                        target=self._serve,
                        args=[thread_logger, socket, unpacker, nonce_hex, ctx_id],
                        daemon=True,
                    )
                    socket = self.socket.new_context()
                    ctx_id += 1
                    self.logger.debug("next socket context", **self._get_id(ctx_id))
                    threads.append(thread)
                    thread.start()
                except pynng.Timeout:
                    pass
                except Exception:
                    self.logger.exception("exception occurred in run_loop")
        finally:
            for thread in threads:
                self.logger.debug(
                    "joining thread", thread=human_thread_id(thread.ident)
                )
                thread.join()
            self.logger.debug("closing socket", **self._get_id())
            if socket is not None:
                socket.close()  # close the open request-response context
            self.socket.close()
            self.logger.info("stopped IPCServer", **self._get_id())

    def _serve(self, thread_logger, socket, unpacker, nonce_hex, ctx_id):
        msg = unpacker.unpack()
        thread_logger.debug(
            "IPCServer Received",
            message=msg,
            nonce=nonce_hex,
            ctx_id=ctx_id,
            thread=human_thread_id(threading.get_ident()),
        )
        reply = self.handle_request(msg)
        thread_logger.debug(
            "IPCServer Reply",
            reply=reply,
            nonce=nonce_hex,
            ctx_id=ctx_id,
            thread=human_thread_id(threading.get_ident()),
        )
        socket.send(msgpack.packb(reply, default=self.msg_default))

    def handle_request(self, request: dict[str, Any]):
        return None

    @cached_property
    def address(self) -> str:
        address = self.socket.listeners[0].local_address
        addr = address.addr
        port = address.port

        # Convert from big endian to native
        if isinstance(addr, int):
            addr = struct.unpack("=L", addr.to_bytes(4, byteorder="big"))[0]
        port = struct.unpack("=H", port.to_bytes(2, byteorder="big"))[0]

        # Create address string
        ip = ipaddress.ip_address(addr)
        if ip.version == 4:
            return f"tcp://{ip.exploded}:{port}"
        else:
            return f"tcp://[{ip}]:{port}"

    def get_client(self) -> IPCClient:
        return IPCClient(
            addr=self.address,
            msg_default=self.msg_default,
            msg_ext_hook=self.msg_ext_hook,
        )


class IPCClient:
    def __init__(self, addr: str, msg_default=None, msg_ext_hook=None):
        self.logger = structlog.get_logger(
            thread=human_thread_id(threading.get_ident())
        )
        self.addr = addr
        self.socket = self._connect()

        self.msg_default = msg_default
        self.msg_ext_hook = msg_ext_hook

    def _connect(self):
        self.logger.debug("opening client connection", addr=self.addr)
        return pynng.Req0(dial=self.addr, resend_time=30_000)

    def request(self, payload: Any) -> Any:
        with self.socket.new_context() as socket:
            self.logger.debug("client request")
            nonce = uuid.uuid4().bytes[:8]
            msg = msgpack.packb((nonce, payload), default=self.msg_default)
            socket.send(msg)

            response_msg = socket.recv()
            self.logger.debug("client got response")
            response = msgpack.unpackb(
                response_msg, use_list=False, ext_hook=self.msg_ext_hook
            )
            return response

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["socket"]
        del state["logger"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = structlog.get_logger(threading.get_ident())
        self.socket = self._connect()


if __name__ == "__main__":
    with IPCServer() as serv:
        client = serv.get_client()
        client.request(["This is a test"])
