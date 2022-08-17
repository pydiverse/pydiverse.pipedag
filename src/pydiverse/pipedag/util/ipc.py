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


class IPCServer:
    def __init__(self, msg_default=None, msg_ext_hook=None):
        self.socket = pynng.Rep0(listen="tcp://localhost:0", recv_timeout=1000)
        self.main_thread = None
        self.worker_threads: dict[Any, threading.Thread] = {}

        self.kill_sig = threading.Event()
        self.logger = structlog.get_logger()

        self.msg_default = msg_default
        self.msg_ext_hook = msg_ext_hook

    def start(self):
        assert self.main_thread is None

        self.logger.info("Starting IPCServer", address=self.address)
        self.main_thread = threading.Thread(
            name="IPC Server", target=self.run_loop, daemon=True
        )
        self.main_thread.start()

    def stop(self):
        assert self.main_thread is not None

        self.kill_sig.set()
        self.main_thread.join()

        for thread in self.worker_threads.values():
            thread.join()

        self.kill_sig.clear()
        self.main_thread = None
        self.logger.info("Did stop IPCServer", address=self.address)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def run_loop(self):
        socket = self.socket.new_context()
        while not self.kill_sig.is_set():
            try:
                data = socket.recv()
                unpacker = msgpack.Unpacker(use_list=False, ext_hook=self.msg_ext_hook)
                unpacker.feed(data)

                # Expected: (NONCE, PAYLOAD)
                assert unpacker.read_array_header() == 2
                nonce = unpacker.unpack()
                nonce_hex = nonce.hex()

                if thread := self.worker_threads.get(nonce):
                    if thread.is_alive():
                        # Already processing this request
                        self.logger.debug("Already processing request", nonce=nonce_hex)
                        return

                thread = threading.Thread(
                    name="IPC Worker",
                    target=self._serve,
                    args=[socket, unpacker, nonce_hex],
                    daemon=True,
                )
                thread.start()
                self.worker_threads[nonce] = thread
            except pynng.Timeout:
                pass
            else:
                socket = self.socket.new_context()

    def _serve(self, socket, unpacker, nonce_hex):
        msg = unpacker.unpack()
        self.logger.debug("IPCServer Received", message=msg, nonce=nonce_hex)
        reply = self.handle_request(msg)
        self.logger.debug("IPCServer Reply", reply=reply, nonce=nonce_hex)
        socket.send(msgpack.packb(reply, default=self.msg_default))

    def handle_request(self, request: dict[str, Any]) -> dict[str, Any]:
        reply = {
            "status": 0,
            "message": "SUCCESS",
        }
        return reply

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
            nonce = uuid.uuid4().bytes
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
