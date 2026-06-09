"""
The MIT License (MIT)

Copyright (c) 2015-present Rapptz

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
"""

from __future__ import annotations

import asyncio
import time
import logging
from typing import TYPE_CHECKING, Generator, Optional, Type, TypeVar


if TYPE_CHECKING:
    from .abc import Messageable, MessageableChannel

    from types import TracebackType

    BE = TypeVar('BE', bound=BaseException)

# fmt: off
__all__ = (
    'Typing',
)
# fmt: on

_log = logging.getLogger(__name__)


def _typing_done_callback(fut: asyncio.Future) -> None:
    # just retrieve any exception and call it a day
    try:
        fut.exception()
    except (asyncio.CancelledError, Exception):
        pass


class Typing:
    def __init__(self, messageable: Messageable) -> None:
        self.loop: asyncio.AbstractEventLoop = messageable._state.loop
        self.messageable: Messageable = messageable
        self.channel: Optional[MessageableChannel] = None
        self.typing_deadline: float = 0

    async def _get_channel(self) -> MessageableChannel:
        if self.channel:
            return self.channel

        self.channel = channel = await self.messageable._get_channel()
        return channel

    async def wrapped_typer(self) -> None:
        channel = await self._get_channel()
        await channel._state.http.send_typing(channel.id)

    def __await__(self) -> Generator[None, None, None]:
        return self.wrapped_typer().__await__()

    async def do_typing(self, channel: MessageableChannel) -> None:
        self.typing_deadline = time.time() + (60 * 3)

        while True:
            if time.time() > self.typing_deadline:
                return _log.warning(
                    "Typing keepalive for channel {!r} (guild={} id={}) exceeded {}s deadline — task exiting",
                    getattr(channel, "name", "unknown"),
                    getattr(getattr(channel, "guild", None), "id", "DM"),
                    channel.id,
                    int(time.time() - (self.typing_deadline - 60 * 3)),
                )
                
            await channel._state.http.send_typing(channel.id)
            await asyncio.sleep(4.2)

    async def __aenter__(self) -> Typing:
        channel = await self._get_channel()
        await channel._state.http.send_typing(channel.id)
        self.task: asyncio.Task[None] = asyncio.create_task(self.do_typing(channel))
        self.task.add_done_callback(_typing_done_callback)
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BE]],
        exc: Optional[BE],
        traceback: Optional[TracebackType],
    ) -> None:
        if self.task:
            self.task.cancel()
            asyncio.ensure_future(asyncio.gather(self.task, return_exceptions=True))