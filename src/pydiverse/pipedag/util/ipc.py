from __future__ import annotations

import ipaddress
import struct
import threading
import uuid
from functools import cached_property
from typing import Any

import msgpack
import pynng
import structlog

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

        self.socket = pynng.Rep0(listen=listen, recv_timeout=1000)
        self.nonces = set()

        self.__stop_flag = False
        self.logger = structlog.get_logger()

        self.msg_default = msg_default
        self.msg_ext_hook = msg_ext_hook

    def run(self):
        self.logger.info("Starting IPCServer", address=self.address)
        self.run_loop()

    def stop(self):
        self.__stop_flag = True
        self.logger.info("Did stop IPCServer", address=self.address)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def run_loop(self):
        socket = self.socket.new_context()
        while not self.__stop_flag:
            try:
                data = socket.recv()
                unpacker = msgpack.Unpacker(use_list=False, ext_hook=self.msg_ext_hook)
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
                    self.logger.debug("Already processing request", nonce=nonce_hex)
                    return

                self.nonces.add(nonce)
                thread = threading.Thread(
                    name="IPC Worker",
                    target=self._serve,
                    args=[socket, unpacker, nonce_hex],
                    daemon=True,
                )
                thread.start()
            except pynng.Timeout:
                pass
            except Exception as e:
                self.logger.exception(e)
            else:
                socket = self.socket.new_context()

        socket.close()
        self.socket.close()

    def _serve(self, socket, unpacker, nonce_hex):
        msg = unpacker.unpack()
        self.logger.debug("IPCServer Received", message=msg, nonce=nonce_hex)
        reply = self.handle_request(msg)
        self.logger.debug("IPCServer Reply", reply=reply, nonce=nonce_hex)
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
        self.addr = addr
        self.socket = self._connect()

        self.msg_default = msg_default
        self.msg_ext_hook = msg_ext_hook

    def _connect(self):
        return pynng.Req0(dial=self.addr, resend_time=30_000)

    def request(self, payload: Any) -> Any:
        with self.socket.new_context() as socket:
            nonce = uuid.uuid4().bytes[:8]
            msg = msgpack.packb((nonce, payload), default=self.msg_default)
            socket.send(msg)

            response_msg = socket.recv()
            response = msgpack.unpackb(
                response_msg, use_list=False, ext_hook=self.msg_ext_hook
            )
            return response

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["socket"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.socket = self._connect()


if __name__ == "__main__":
    with IPCServer() as serv:
        client = serv.get_client()
        client.request(["This is a test"])
