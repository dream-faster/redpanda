"""A TCP proxy that holds every chunk it forwards for a fixed delay.

The acceptance brokers share one host's loopback, where a replication round
trip takes a fraction of a millisecond. Pointing the brokers' advertised RPC
addresses at one of these per node gives raft traffic a round trip of twice
the delay -- what a network hop plus an fsync costs in production -- without
kernel support (netem) the test host may not have. Order is preserved within
each direction of a connection.
"""

from __future__ import annotations

import asyncio
import threading

# What one direction of a connection may hold before the proxy stops reading
# from its sender.
_MAX_QUEUED_BYTES = 64 << 20


class DelayProxy:
    def __init__(self, listen_port: int, target_port: int, delay_s: float):
        self.listen_port = listen_port
        self.target_port = target_port
        self.delay_s = delay_s
        self._loop = asyncio.new_event_loop()
        self._server: asyncio.base_events.Server | None = None
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)

    def start(self) -> None:
        self._thread.start()
        asyncio.run_coroutine_threadsafe(self._listen(), self._loop).result(10)

    def stop(self) -> None:
        async def close():
            if self._server is not None:
                self._server.close()
            for task in asyncio.all_tasks():
                if task is not asyncio.current_task():
                    task.cancel()

        asyncio.run_coroutine_threadsafe(close(), self._loop).result(10)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(10)

    async def _listen(self) -> None:
        self._server = await asyncio.start_server(
            self._handle, "127.0.0.1", self.listen_port, reuse_address=True
        )

    async def _handle(self, client_r, client_w) -> None:
        try:
            upstream_r, upstream_w = await asyncio.open_connection(
                "127.0.0.1", self.target_port
            )
        except OSError:
            client_w.close()
            return
        await asyncio.gather(
            self._pipe(client_r, upstream_w),
            self._pipe(upstream_r, client_w),
            return_exceptions=True,
        )

    async def _pipe(self, reader, writer) -> None:
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue()
        queued = 0
        drained = asyncio.Event()

        async def send() -> None:
            nonlocal queued
            try:
                while True:
                    due, data = await queue.get()
                    if data is None:
                        break
                    wait = due - loop.time()
                    if wait > 0:
                        await asyncio.sleep(wait)
                    writer.write(data)
                    await writer.drain()
                    queued -= len(data)
                    drained.set()
            finally:
                writer.close()

        sender = asyncio.create_task(send())
        try:
            while not sender.done() and (data := await reader.read(1 << 16)):
                queue.put_nowait((loop.time() + self.delay_s, data))
                queued += len(data)
                # Stop reading while the receiver is that far behind, so it
                # pushes back on the sender as a direct connection would.
                while queued > _MAX_QUEUED_BYTES and not sender.done():
                    drained.clear()
                    waiter = asyncio.ensure_future(drained.wait())
                    await asyncio.wait(
                        {waiter, sender}, return_when=asyncio.FIRST_COMPLETED
                    )
                    waiter.cancel()
        except (ConnectionError, OSError):
            pass
        finally:
            queue.put_nowait((0.0, None))
            await asyncio.gather(sender, return_exceptions=True)
