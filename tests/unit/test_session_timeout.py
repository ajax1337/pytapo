"""Regression test: media session handshake must not hang forever when the
camera accepts the TCP connection but never answers (sleeping battery cam)."""

import asyncio
import time

from pytapo.const import EncryptionMethod
from pytapo.media_stream.session import HttpMediaSession


class TestHandshakeTimeout:
    def test_start_times_out_against_silent_server(self):
        async def scenario():
            async def silent_handler(reader, writer):
                # Accept the connection, read forever, never respond.
                try:
                    await reader.read(-1)
                except Exception:
                    pass

            server = await asyncio.start_server(
                silent_handler, "127.0.0.1", 0
            )
            port = server.sockets[0].getsockname()[1]
            session = HttpMediaSession(
                "127.0.0.1",
                "cloudpass",
                "supersecret",
                EncryptionMethod.MD5,
                port=port,
            )
            started = time.monotonic()
            try:
                await session.start()
                raise AssertionError("start() unexpectedly succeeded")
            except asyncio.TimeoutError:
                pass
            finally:
                elapsed = time.monotonic() - started
                server.close()
                await server.wait_closed()
            return elapsed

        elapsed = asyncio.run(scenario())
        # CONNECTION_TIMEOUT bounds the handshake; far below a hang,
        # generous enough not to flake on slow CI.
        assert elapsed < 60
