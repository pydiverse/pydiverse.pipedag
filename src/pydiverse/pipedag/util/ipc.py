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
from cryptography.fernet import Fernet

from pydiverse.pipedag.errors import IPCError


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

        self._fernet = Fernet(Fernet.generate_key())

        self.__stop_flag = False
        self.logger = structlog.get_logger(
            logger_name=type(self).__name__, address=self.address
        )

        self.msg_default = msg_default
        self.msg_ext_hook = msg_ext_hook

    def run(self):
        self.logger.info("Starting IPCServer")
        self.run_loop()

    def stop(self):
        self.__stop_flag = True
        self.logger.debug("Request IPCServer to stop")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self.join()

    def run_loop(self):
        context = None
        context_id = 0

        threads = set()
        active_threads = 0

        try:
            context = self.socket.new_context()
            context_id += 1

            while not self.__stop_flag:
                try:
                    data = context.recv()
                    data = self._fernet.decrypt(data)
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
                        )

                        context = self.socket.new_context()
                        context_id += 1
                        continue

                    thread_logger = structlog.get_logger(
                        logger_name="IPC Worker", nonce=nonce_hex, id=context_id
                    )
                    thread = threading.Thread(
                        name="IPC Worker",
                        target=self._serve,
                        args=[thread_logger, context, unpacker],
                        daemon=True,
                    )

                    thread.start()
                    threads.add(thread)

                    context = self.socket.new_context()
                    context_id += 1
                except pynng.Timeout:
                    pass
                except Exception:
                    self.logger.exception("Exception occurred in run_loop")

                # Clear list of active threads
                if len(threads) >= active_threads + 25:
                    active_threads = 0
                    for thread in list(threads):
                        if thread.is_alive():
                            active_threads += 1
                        else:
                            self.logger.debug("Joining thread", thread=thread.ident)
                            thread.join()
                            threads.remove(thread)

        finally:
            if context is not None:
                context.close()  # close the open request-response context

            for thread in threads:
                self.logger.debug("Joining thread", thread=thread.ident)
                thread.join()

            self.socket.close()
            self.logger.info("Stopped IPCServer")

    def _serve(self, thread_logger, socket: pynng.Socket, unpacker):
        try:
            msg = unpacker.unpack()
            thread_logger.debug(
                "IPCServer Received",
                message=msg,
                thread=threading.get_ident(),
            )
            reply = self.handle_request(msg)
            thread_logger.debug(
                "IPCServer Reply",
                reply=reply,
                thread=threading.get_ident(),
            )
            reply = msgpack.packb(reply, default=self.msg_default)
            reply = self._fernet.encrypt(reply)
        except Exception as e:
            thread_logger.critical("Uncaught exception in _serve", exc_info=e)
            reply = b""

        try:
            socket.send(reply)
            socket.close()
        except Exception:
            thread_logger.exception("Failed to send reply")

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
            fernet=self._fernet,
            msg_default=self.msg_default,
            msg_ext_hook=self.msg_ext_hook,
        )


class IPCClient:
    def __init__(self, addr: str, fernet: Fernet, msg_default=None, msg_ext_hook=None):
        self.logger = structlog.get_logger(logger_name=type(self).__name__)
        self.addr = addr
        self._fernet = fernet
        self.msg_default = msg_default
        self.msg_ext_hook = msg_ext_hook

        self.socket = self._connect()

    def _connect(self):
        self.logger.debug("Opening client connection", addr=self.addr)
        return pynng.Req0(dial=self.addr, resend_time=30_000)

    def request(self, payload: Any) -> Any:
        with self.socket.new_context() as socket:
            self.logger.debug("Client request", payload=payload)
            nonce = uuid.uuid4().bytes[:16]
            msg = msgpack.packb((nonce, payload), default=self.msg_default)
            msg = self._fernet.encrypt(msg)
            socket.send(msg)

            response = socket.recv()
            response = self._fernet.decrypt(response)
            response = msgpack.unpackb(
                response, use_list=False, ext_hook=self.msg_ext_hook
            )
            self.logger.debug("Client got response", response=response)
            return response

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["socket"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = self.logger.bind(
            thread=threading.get_ident(),
        )
        self.socket = self._connect()


if __name__ == "__main__":
    with IPCServer() as serv:
        client = serv.get_client()
        client.request(["This is a test"])
