# Carlo Gavazzi EM540 Energy Meter Modbus Bridge

## Overview

Bridges a single Carlo Gavazzi EM540/EM530 energy meter to multiple downstream consumers simultaneously.

The bridge acts as a Modbus master to the physical meter over RS485 (via an RS485-to-IP converter or serial device),
reading all dynamic registers at a target rate of 10 Hz. It then re-serves this data in several formats:

- **Transparent Modbus/TCP proxy:** Direct access to EM540-compatible registers for clients such as Victron GX devices.
- **Fronius TS-65-A emulation:** EM540 data mapped to the Fronius TS-65-A register layout for Fronius inverter compatibility.
- **MQTT sensors for Home Assistant:** Key measurements published as MQTT sensors for monitoring and automation.

Fronius TS-65-A emulation maps EM540 data to the corresponding registers transparently. Optionally, meter values can be
smoothed over a configurable time window so the emulated values represent a rolling average rather than instantaneous
readings (see `config-default.yaml`).

> **Note:** The bridge is strictly a read-only proxy. Most EM540 registers are read and cached; some infrequently-changing
> registers are polled at a lower rate to reduce bus load.

## Features

- **Modbus/RTU to Modbus/TCP proxy:** Reads metering registers from the EM540 via Modbus/RTU and serves them over Modbus/TCP.
- **Fronius TS-65-A emulation:** Maps EM540 data to the Fronius TS-65-A register format for compatibility with Fronius inverters.
- **Home Assistant integration:** Publishes measurements and diagnostics as MQTT sensors.
- **High-rate acquisition:** Targets a 10 Hz (100 ms) polling interval for near real-time updates.
- **Concurrent client support:** Serves multiple downstream clients without increasing load on the EM540 meter.
- **Stale-data protection:** A circuit breaker blocks downstream Modbus responses when upstream data is stale or unavailable, preventing silent delivery of bad values.

## Requirements

- **Hardware:** RS485 to Modbus/RTU physical converter to connect the EM540 meter to your network.
- **Meter configuration:** EM540 must be set to a baud rate of 57600 or higher to support a 100 ms read cycle.
- **Software:** Python 3.13 and all dependencies listed in `requirements.txt`.

## Usage

1. Install Python dependencies: `pip install -r requirements.txt`
2. Connect the EM540 meter via an RS485-to-IP converter.
3. Configure the meter for a baud rate of 57600 or higher; ensure the converter matches (baud rate, data bits, parity, stop bits).
4. Copy `config-default.yaml` and update it with your converter's IP address/port and any other settings.
5. Run the bridge: `python -m app` (or `make up` for Docker).
6. For Home Assistant, point the bridge at the MQTT broker used by HA — see the `mqtt` section in the config file.

## Home Assistant Integration

The following measurement sensors are published to Home Assistant and refreshed at the interval configured in the config file:

| Sensor Name            | Unit  | Device Class   | State Class      | Precision |
|------------------------|-------|----------------|------------------|-----------|
| Frequency              | Hz    | frequency      | measurement      | 2         |
| Mean Voltage L-N       | V     | voltage        | measurement      | 1         |
| Voltage L1-N           | V     | voltage        | measurement      | 1         |
| Voltage L2-N           | V     | voltage        | measurement      | 1         |
| Voltage L3-N           | V     | voltage        | measurement      | 1         |
| Mean Voltage L-L       | V     | voltage        | measurement      | 1         |
| Voltage L1-L2          | V     | voltage        | measurement      | 1         |
| Voltage L2-L3          | V     | voltage        | measurement      | 1         |
| Voltage L3-L1          | V     | voltage        | measurement      | 1         |
| Current                | A     | current        | measurement      | 1         |
| Current L1             | A     | current        | measurement      | 1         |
| Current L2             | A     | current        | measurement      | 1         |
| Current L3             | A     | current        | measurement      | 1         |
| Power                  | W     | power          | measurement      | 0         |
| Power L1               | W     | power          | measurement      | 0         |
| Power L2               | W     | power          | measurement      | 0         |
| Power L3               | W     | power          | measurement      | 0         |
| Reactive Power         | var   | reactive_power | measurement      | 0         |
| Reactive Power L1      | var   | reactive_power | measurement      | 0         |
| Reactive Power L2      | var   | reactive_power | measurement      | 0         |
| Reactive Power L3      | var   | reactive_power | measurement      | 0         |
| Apparent Power         | VA    | apparent_power | measurement      | 0         |
| Apparent Power L1      | VA    | apparent_power | measurement      | 0         |
| Apparent Power L2      | VA    | apparent_power | measurement      | 0         |
| Apparent Power L3      | VA    | apparent_power | measurement      | 0         |
| Mean Power Factor      |       | power_factor   | measurement      | 2         |
| Power Factor L1        |       | power_factor   | measurement      | 2         |
| Power Factor L2        |       | power_factor   | measurement      | 2         |
| Power Factor L3        |       | power_factor   | measurement      | 2         |
| Energy Import          | kWh   | energy         | total_increasing | 2         |
| Energy Export          | kWh   | energy         | total_increasing | 2         |
| Reactive Energy Import | kvarh | reactive_energy| total_increasing | 2         |
| Reactive Energy Export | kvarh | reactive_energy| total_increasing | 2         |
| Apparent Energy        | kWh   | energy         | total_increasing | 2         |
| Run Hours              | h     | duration       | total_increasing | 1         |

The following diagnostic sensors are also published (entity category: `diagnostic`):

| Sensor Name                        | Unit | Device Class | State Class | Precision |
|------------------------------------|------|--------------|-------------|-----------|
| Sys Uptime                         | s    | duration     | measurement | 0         |
| Bridge Uptime                      | s    | duration     | measurement | 0         |
| RS485 Master Read Rate             | Hz   | frequency    | measurement | 2         |
| RS485 Master Read Failures         |      |              | measurement | 0         |
| RS485 Consumer Missed Updates      |      |              | measurement | 0         |
| RS485 Consumer Max Seq Gap         |      |              | measurement | 0         |
| RS485 Master Read Duration         | ms   | duration     | measurement | 2         |
| RS485 Master Read Duration Max     | ms   |              | measurement | 2         |
| RS485 Tick Headroom                | ms   | duration     | measurement | 2         |
| RS485 Tick Headroom Min            | ms   |              | measurement | 2         |
| RS485 Tick Overrun Count           |      |              | measurement | 0         |
| Min Power W                        | W    | power        | measurement | 1         |
| Max Power W                        | W    | power        | measurement | 1         |
| EM540 RTU Client Count             |      |              | measurement | 0         |
| EM540 RTU Client Disconnect Count  |      |              | measurement | 0         |
| EM540 Circuit Breaker Open         |      |              | measurement | 0         |
| EM540 Circuit Breaker Open Count   |      |              | measurement | 0         |
| EM540 Stale Data Age               | ms   | duration     | measurement | 1         |
| EM540 Dropped Stale Requests       |      |              | measurement | 0         |
| TS65A TCP Client Count             |      |              | measurement | 0         |
| TS65A TCP Client Disconnect Count  |      |              | measurement | 0         |
| Overfeed Limit Count               |      |              | measurement | 0         |
| Overfeed Limit Max Duration        | ms   | duration     | measurement | 2         |
| TS65A Circuit Breaker Open         |      |              | measurement | 0         |
| TS65A Circuit Breaker Open Count   |      |              | measurement | 0         |
| TS65A Stale Data Age               | ms   | duration     | measurement | 1         |
| TS65A Dropped Stale Requests       |      |              | measurement | 0         |

<img src="./media/HA%20Diagnostics.png" width=200>

## References

- [Carlo Gavazzi EM540/EM530 Modbus Register Map](https://www.gavazziautomation.com/fileadmin/images/PIM/OTHERSTUFF/COMPRO/EM500_CPP_Mod_V1.3_13022024.pdf)
