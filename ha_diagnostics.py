from meter_data import MeterData

diag_interval = 5  # seconds

class HADiagnostics:
    def __init__(self):
        self._last_update = 0
        self._data_counter = 0
        self._last_data_counter = 0
        self._read_failed_counter = 0
        self._update_rate = 0

    def new_data(self, data: MeterData):
        # Keep track of how many updates we have received, so we can calculate an update rate
        self._data_counter += 1

        # Calculate update rate
        if data.timestamp - self._last_update > diag_interval:
            update_rate = (self._data_counter - self._last_data_counter) / (data.timestamp - self._last_update)

            print(update_rate)

            self._last_data_counter = self._data_counter
            self._last_update = data.timestamp

    def read_failed(self):
        self._read_failed_counter += 1

    @property
    def update_rate(self):
        return self._update_rate

    @property
    def read_failed_count(self):
        return self._read_failed_counter
