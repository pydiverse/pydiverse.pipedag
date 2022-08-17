from __future__ import annotations

import ipaddress
import struct
import threading
from functools import cached_property
from typing import Any

import msgpack
import pynng
import structlog


class IPCServer:
    def __init__(self):
        self.socket = pynng.Rep0(listen="tcp://localhost:0", recv_timeout=1000)
        self.threads = []
        self.kill_sig = threading.Event()
        self.running = False
        self.logger = structlog.get_logger()

    def start(self):
        assert not self.running
        self.running = True

        self.logger.info("Starting IPCServer", address=self.address)

        # Start the main thread that handles requests
        thread = threading.Thread(name="IPC Server", target=self._run, daemon=True)
        thread.start()
        self.threads.append(thread)

    def stop(self):
        assert self.running
        self.kill_sig.set()
        for thread in self.threads:
            thread.join()

        self.kill_sig.clear()
        self.running = False

        self.logger.info("Did stop IPCServer", address=self.address)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def _run(self):
        socket = self.socket.new_context()
        while not self.kill_sig.is_set():
            try:
                data = socket.recv()
                thread = threading.Thread(
                    name="IPC Worker",
                    target=self._serve,
                    args=[socket, data],
                    daemon=True,
                )
                thread.start()
                self.threads.append(thread)
            except pynng.Timeout:
                pass
            else:
                socket = self.socket.new_context()

    def _serve(self, socket, data):
        msg = msgpack.unpackb(data, use_list=False)
        self.logger.debug("IPCServer Received", message=msg)
        reply = self.handle_request(msg)
        self.logger.debug("IPCServer Reply", reply=reply)
        socket.send(msgpack.packb(reply))

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
        return IPCClient(addr=self.address)


class IPCClient:
    def __init__(self, addr: str):
        self.addr = addr
        self.socket = self._connect()

    def _connect(self):
        return pynng.Req0(dial=self.addr)

    def request(self, payload: Any) -> Any:
        with self.socket.new_context() as socket:
            msg = msgpack.packb(payload)
            socket.send(msg)

            response_msg = socket.recv()
            response = msgpack.unpackb(response_msg, use_list=False)
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
