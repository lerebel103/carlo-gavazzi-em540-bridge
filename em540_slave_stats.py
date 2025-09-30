from typing import Callable


class EM540SlaveStats:
    def __init__(self):
        self.rtu_client_count: int = 0
        self.rtu_client_disconnect_count: int = 0

        self.tcp_client_count: int = 0
        self.tcp_client_disconnect_count: int = 0

        self._listeners: list[Callable[['EM540SlaveStats'], None]] = []

    def changed(self) -> None:
        for listener in self._listeners:
            listener(self)

    def add_listener(self, listener: Callable[['EM540SlaveStats'], None]) -> None:
        self._listeners.append(listener)
