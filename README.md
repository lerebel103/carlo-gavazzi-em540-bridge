# Carlo Gavazzi EM540 Energy Meter MODBUS IP Bridge

## Overview

Allows multiple IP clients to independently access real-time data from a single Carlo Gavazzi EM540/EM530 Energy Meter.

To do so, a physical RS485 MODBUS/RTU to IP converter, or RS485 Serial device is first required so the meter data is
made accessible to this bridge application, acting as a MODBUS master to the meter. This application then acts as a Modbus/TCP server, 
to proxy the same data to multiple Modbus/TCP/RTU clients, emulating EM540 registers. Additionally, it can provide a
Fronius TS-65-A emulation server, and MQTT publisher to Home Assistant.

The general idea is to read all dynamic registers from the EM540 meter at a high rate, close to the maximum
polling interval (100ms), and efficiently re-serve this data to multiple consumers in various formats:

- **Transparent Modbus/RTU over sockets:** Direct access to EM540 registers for compatible clients (like Victron Gx devices).
- **Fronius TS-65-A emulation over Modbus/TCP:** EM540/EM530 data is mapped and presented as if it were a Fronius TS-65-A smart meter.
- **MQTT sensors for Home Assistant:** Key measurements are published as MQTT sensors for easy integration and automation.

In the case of the Fronius TS-65-A emulation, the EM540/EM530 data is mapped to the corresponding registers,
enabling seamless integration without additional hardware or complex configurations. It is also possible to smooth
the meter data over a specified time window so the emulated values represent a time period average, rather than 
instantaneous readings, if this is desired (see config file).

> **Note:** Most EM540 registers are read and cached, but some are intentionally excluded. The bridge is strictly a read-only proxy.

## Features

- **Modbus/RTU to Modbus/TCP Proxy:** Reads metering registers from the EM540 meter via Modbus/RTU and serves them over Modbus/TCP.
- **Fronius TS-65-A Emulation:** Maps EM540 data to Fronius TS-65-A register format for compatibility with Fronius inverters.
- **Home Assistant Integration:** Publishes selected measurements as MQTT sensors for monitoring and automation.
- **High-Rate Data Acquisition:** Supports fast polling intervals (100ms) for near real-time updates.
- **Concurrent Client Support:** Efficiently serves multiple clients without overloading the EM540 meter.

## Requirements

- **Hardware:** RS485 to Modbus/RTU physical converter to connect the EM540 meter to your system.
- **Meter Configuration:** EM540 meter must be set to a high baud rate (57600 or above) to support fast data transfer and achieve a 100ms read rate.
- **Software:** Python, pip, and all dependencies listed in `requirements.txt`.

## Usage

1. Install the required Python dependencies using pip.
2. Connect your EM540 meter to any RS485 to Modbus/RTU converter that can serve RS485 over IP.
3. Configure the EM540 meter for a baud rate of 57600 or higher.
4. Ensure the converter has the correct matching baud rate, data bits, parity, and stop bits settings.
5. Set its IP address and port and add to the configuration file of the bridge.
6. Run the bridge application to start reading registers and serving data over Modbus/TCP, Fronius emulation, and MQTT.
7. Integrate with Home Assistant by configuring this bridge to the MQTT broker used by Home Assistant, see `mqtt` section in the configuration file.

## Home Assistant Integration

The following measurements are exposed in Home Assistant and refreshed at which ever interval you set in the configuration file:

| Sensor Name                | Unit   | Device Class      | State Class        | Precision |
|----------------------------|--------|-------------------|--------------------|-----------|
| Frequency                  | Hz     | frequency         | measurement        | 2         |
| Mean Voltage L-N           | V      | voltage           | measurement        | 1         |
| Voltage L1-N               | V      | voltage           | measurement        | 1         |
| Voltage L2-N               | V      | voltage           | measurement        | 1         |
| Voltage L3-N               | V      | voltage           | measurement        | 1         |
| Mean Voltage L-L           | V      | voltage           | measurement        | 1         |
| Voltage L1-L2              | V      | voltage           | measurement        | 1         |
| Voltage L2-L3              | V      | voltage           | measurement        | 1         |
| Voltage L3-L1              | V      | voltage           | measurement        | 1         |
| Current                    | A      | current           | measurement        | 1         |
| Current L1                 | A      | current           | measurement        | 1         |
| Current L2                 | A      | current           | measurement        | 1         |
| Current L3                 | A      | current           | measurement        | 1         |
| Power                      | W      | power             | measurement        | 0         |
| Power L1                   | W      | power             | measurement        | 0         |
| Power L2                   | W      | power             | measurement        | 0         |
| Power L3                   | W      | power             | measurement        | 0         |
| Reactive Power             | var    | reactive_power    | measurement        | 0         |
| Reactive Power L1          | var    | reactive_power    | measurement        | 0         |
| Reactive Power L2          | var    | reactive_power    | measurement        | 0         |
| Reactive Power L3          | var    | reactive_power    | measurement        | 0         |
| Apparent Power             | VA     | apparent_power    | measurement        | 0         |
| Apparent Power L1          | VA     | apparent_power    | measurement        | 0         |
| Apparent Power L2          | VA     | apparent_power    | measurement        | 0         |
| Apparent Power L3          | VA     | apparent_power    | measurement        | 0         |
| Mean Power Factor          |        | power_factor      | measurement        | 2         |
| Power Factor L1            |        | power_factor      | measurement        | 2         |
| Power Factor L2            |        | power_factor      | measurement        | 2         |
| Power Factor L3            |        | power_factor      | measurement        | 2         |
| Energy Import              | kWh    | energy            | total_increasing   | 2         |
| Energy Export              | kWh    | energy            | total_increasing   | 2         |
| Reactive Energy Export     | kvarh  | reactive_energy   | total_increasing   | 2         |
| Reactive Energy Import     | kvarh  | reactive_energy   | total_increasing   | 2         |
| Apparent Energy kvah       | kWh    | energy            | total_increasing   | 2         |
| Run Hours                  | h      | duration          | total_increasing   | 1         |

The following Diagnostic sensors are also available in Home Assistant:

| Sensor Name                          | Unit | Device Class | State Class | Precision | Entity Category |
|---------------------------------------|------|--------------|-------------|-----------|----------------|
| Sys Uptime                           | s    | duration     | measurement | 0         | diagnostic     |
| Bridge Uptime                        | s    | duration     | measurement | 0         | diagnostic     |
| RS485 Master Read Rate                | Hz   | frequency    | measurement | 2         | diagnostic     |
| RS485 Master Read Failures            |      |              | measurement | 0         | diagnostic     |
| Min Power W                          | W    | power        | measurement | 1         | diagnostic     |
| Max Power W                          | W    | power        | measurement | 1         | diagnostic     |
| TS65A TCP Client Count               |      |              | measurement | 0         | diagnostic     |
| TS65A TCP Client Disconnect Count    |      |              | measurement | 0         | diagnostic     |
| Overfeed Limit Count                 |      |              | measurement | 0         | diagnostic     |
| Overfeed Limit Max Duration          | ms   | duration     | measurement | 2         | diagnostic     |
| EM540 RTU Client Count               |      |              | measurement | 0         | diagnostic     |
| EM540 RTU Client Disconnect Count    |      |              | measurement | 0         | diagnostic     |
| EM540 TCP Client Count               |      |              | measurement | 0         | diagnostic     |
| EM540 TCP Client Disconnect Count    |      |              | measurement | 0         | diagnostic     |

<img src="./media/HA%20Diagnostics.png" width=200>


## Documentation

For installation, configuration, and advanced usage details, refer to the documentation and example configuration files included in the repository.

## References
- [Carlo Gavazzi EM540/EM530 Modbus Register Map](https://www.gavazziautomation.com/fileadmin/images/PIM/OTHERSTUFF/COMPRO/EM500_CPP_Mod_V1.3_13022024.pdf)

---

Feel free to open issues or contribute improvements to this project!
