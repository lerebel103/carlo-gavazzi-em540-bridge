"""Serial-only frame-latching for downstream Modbus slave bridges.

Downstream Modbus-over-SERIAL clients can poll the bridge faster than the
upstream EM540/EM530 master refreshes, so they receive DUPLICATE frames. This
module latches a serial client's effective read rate to the upstream refresh
rate: when a serial read arrives and no NEW frame has been published into the
downstream registers since that serial line was last served, the read BLOCKS
until the next fresh frame lands, then releases and responds IMMEDIATELY.

The mechanism is structural serial-only scoping: only the serial server's
``context`` is swapped for :class:`SerialPacingContext`; the TCP and
RTU-over-TCP servers keep the plain shared ``SimCore`` and are byte-for-byte
unaffected.

Design basis (verified against pymodbus 3.15.0):
  - ``ReadHoldingRegistersRequest.datastore_update`` awaits
    ``context.async_getValues(...)`` and maps an ``ExcCodes`` return straight to
    an ``ExceptionResponse`` (identical on the FC4 read-input path). So awaiting
    inside the proxy defers the serial response, and returning ``ExcCodes`` is a
    clean fail-closed path.
  - The only ``context`` methods on the server request path are
    ``device_ids()``, ``async_getValues(...)`` and ``async_setValues(...)``; the
    proxy forwards exactly those.

The latch sits BEFORE the datastore read and DEFERS to the tier-1 staleness
breaker (``PduHelper.circuit_open``): a doomed request never blocks, it fails
closed with ``DEVICE_BUSY`` immediately. The whole staleness ladder is
preserved.
"""

from __future__ import annotations

import asyncio

from pymodbus.constants import ExcCodes

from app.utils.pdu_helper import PduHelper


class FrameGate:
    """Cross-thread wake primitive for one serial server's frame generation.

    The upstream listener thread bumps a per-bridge monotonic generation counter
    on each successful write and schedules :meth:`publish` onto the dedicated
    server loop via ``call_soon_threadsafe``. The serial read coroutine (running
    on that same server loop) awaits :meth:`wait_for_generation_after` for a
    newer generation.

    The ``asyncio.Condition`` is created LAZILY on first use so it binds to the
    running server loop (an asyncio primitive binds to the loop at first await,
    not construction; creating/awaiting on the wrong loop raises
    "got Future attached to a different loop"). Both :meth:`publish` and
    :meth:`wait_for_generation_after` are only ever invoked on the server loop.
    """

    def __init__(self) -> None:
        self.current_generation: int = 0
        self._condition: asyncio.Condition | None = None

    def _get_condition(self) -> asyncio.Condition:
        if self._condition is None:
            # Created on the running (server) loop on first use.
            self._condition = asyncio.Condition()
        return self._condition

    def publish(self, generation: int) -> None:
        """Store the latest generation and wake any awaiter.

        Runs on the server loop (scheduled via ``call_soon_threadsafe``). Because
        a serial server is single-request/half-duplex there is at most one
        awaiter, so ``notify_all`` vs ``notify(1)`` is immaterial.
        """
        if generation > self.current_generation:
            self.current_generation = generation
        condition = self._get_condition()

        async def _notify() -> None:
            async with condition:
                condition.notify_all()

        # publish() is a sync callback invoked on the server loop; schedule the
        # async notify on that same loop.
        asyncio.ensure_future(_notify())

    async def wait_for_generation_after(self, generation: int) -> None:
        """Await until ``current_generation`` is strictly greater than ``generation``.

        Returns immediately if a newer generation already exists at entry (client
        slower than upstream).
        """
        if self.current_generation > generation:
            return
        condition = self._get_condition()
        async with condition:
            await condition.wait_for(lambda: self.current_generation > generation)


class SerialPacingContext:
    """Serial-only async context proxy wrapping the real ``SimCore``.

    Forwards ``device_ids()`` and ``async_setValues(...)`` as straight
    pass-throughs (writes are NEVER gated) and implements the read latch in
    ``async_getValues(...)``.

    A single per-instance ``_last_served_gen`` cursor models "has this serial
    line been served the current frame yet?" — correct because a serial server
    is single-handler/half-duplex (one awaiter at a time), so no per-connection
    state is needed.

    If ``max_wait_s <= 0`` latching is disabled and ``async_getValues`` is a pure
    pass-through (today's behaviour, duplicate frames possible).
    """

    def __init__(
        self,
        core: object,
        gate: FrameGate,
        pdu_helper: PduHelper,
        max_wait_s: float,
    ) -> None:
        self._core = core
        self._gate = gate
        self._pdu_helper = pdu_helper
        self._max_wait_s = max_wait_s
        self._last_served_gen: int = 0

    def device_ids(self) -> list[int]:
        return self._core.device_ids()

    async def async_setValues(self, device_id, func_code, address, values):
        # Writes are never gated — straight pass-through.
        return await self._core.async_setValues(device_id, func_code, address, values)

    async def async_getValues(self, device_id, func_code, address, count: int = 1):
        # (a) STALE-BREAKER FIRST: if the upstream staleness breaker is open, fail
        # closed immediately with DEVICE_BUSY — never block a doomed request up to
        # max_wait_s.
        if self._pdu_helper.circuit_open:
            return ExcCodes.DEVICE_BUSY

        # (b) DISABLED: latching off — pure pass-through, no wait.
        if self._max_wait_s <= 0:
            return await self._core.async_getValues(device_id, func_code, address, count)

        # (c) NORMAL WAIT: bounded wait for a generation strictly newer than the
        # one this serial line was last served. Returns immediately if a newer
        # generation already exists at entry.
        try:
            await asyncio.wait_for(
                self._gate.wait_for_generation_after(self._last_served_gen),
                timeout=self._max_wait_s,
            )
        except (asyncio.TimeoutError, TimeoutError):
            # (d) JITTER FALLBACK: the bounded wait timed out. If the breaker
            # OPENED during the wait, fail closed (DEVICE_BUSY). Otherwise serve
            # the current frame (one possible duplicate, acceptable) rather than
            # tripping a breaker on mere jitter.
            if self._pdu_helper.circuit_open:
                return ExcCodes.DEVICE_BUSY

        # (e) After a successful wait OR jitter fallthrough: record the generation
        # served, then read the datastore.
        self._last_served_gen = self._gate.current_generation
        return await self._core.async_getValues(device_id, func_code, address, count)
