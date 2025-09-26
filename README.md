# EM540 MODBUS Bridge

## Overview

This bridge application allows multiple clients to independently access real-time data from a single Carlo Gavazzi EM540 Energy Meter.

 This bridge is ideal for integrating the EM540 meter into home automation systems, 
Victron GX devices, Fronius inverters, and other energy management solutions—eliminating the need for each system to 
have its own dedicated meter.

The bridge establishes a single high-speed connection to the EM540 meter, reads all relevant registers at a fast 
polling interval (100ms), and efficiently re-serves this data to multiple consumers in various formats:

- **Transparent Modbus/RTU over sockets:** Direct access to EM540 registers for compatible clients.
- **Fronius TS-65-A emulation over Modbus/TCP:** EM540 data is mapped and presented as if it were a Fronius TS-65-A smart meter.
- **MQTT sensors for Home Assistant:** Key measurements are published as MQTT sensors for easy integration and automation.

All dynamic registers from the EM540 meter are read and cached for fast, concurrent access. Data is served directly 
from the cache for EM540 clients, while Fronius emulation maps the relevant registers before serving. 
The bridge operates in read-only mode and does not write back to the EM540 meter.

Each component—EM540 Modbus/RTU client, Modbus/TCP server, Fronius TS-65-A server, and MQTT publisher—runs independently and can be configured individually via the main configuration file.

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

| Sensor Group      | Sensor Name           | Unit  | Device Class     | State Class        | Precision |
|-------------------|----------------------|-------|------------------|--------------------|-----------|
| Frequency         | Frequency             | Hz    | frequency        | measurement        | 2         |
| Voltage           | Mean Voltage L-N      | V     | voltage          | measurement        | 1         |
| Voltage           | Voltage L1-N          | V     | voltage          | measurement        | 1         |
| Voltage           | Voltage L2-N          | V     | voltage          | measurement        | 1         |
| Voltage           | Voltage L3-N          | V     | voltage          | measurement        | 1         |
| Voltage           | Mean Voltage L-L      | V     | voltage          | measurement        | 1         |
| Voltage           | Voltage L1-L2         | V     | voltage          | measurement        | 1         |
| Voltage           | Voltage L2-L3         | V     | voltage          | measurement        | 1         |
| Voltage           | Voltage L3-L1         | V     | voltage          | measurement        | 1         |
| Current           | Current               | A     | current          | measurement        | 1         |
| Current           | Current L1            | A     | current          | measurement        | 1         |
| Current           | Current L2            | A     | current          | measurement        | 1         |
| Current           | Current L3            | A     | current          | measurement        | 1         |
| Power             | Power                 | W     | power            | measurement        | 1         |
| Power             | Power L1              | W     | power            | measurement        | 1         |
| Power             | Power L2              | W     | power            | measurement        | 1         |
| Power             | Power L3              | W     | power            | measurement        | 1         |
| Reactive Power    | Reactive Power        | var   | reactive_power   | measurement        | 1         |
| Reactive Power    | Reactive Power L1     | var   | reactive_power   | measurement        | 1         |
| Reactive Power    | Reactive Power L2     | var   | reactive_power   | measurement        | 1         |
| Reactive Power    | Reactive Power L3     | var   | reactive_power   | measurement        | 1         |
| Apparent Power    | Apparent Power        | VA    | apparent_power   | measurement        | 1         |
| Apparent Power    | Apparent Power L1     | VA    | apparent_power   | measurement        | 1         |
| Apparent Power    | Apparent Power L2     | VA    | apparent_power   | measurement        | 1         |
| Apparent Power    | Apparent Power L3     | VA    | apparent_power   | measurement        | 1         |
| Power Factor      | Mean Power Factor     |       | power_factor     | measurement        | 2         |
| Power Factor      | Power Factor L1       |       | power_factor     | measurement        | 2         |
| Power Factor      | Power Factor L2       |       | power_factor     | measurement        | 2         |
| Power Factor      | Power Factor L3       |       | power_factor     | measurement        | 2         |
| Energy            | Energy Import         | kWh   | energy           | total_increasing   | 2         |
| Energy            | Energy Export         | kWh   | energy           | total_increasing   | 2         |

## Documentation

For installation, configuration, and advanced usage details, refer to the documentation and example configuration files included in the repository.

---

Feel free to open issues or contribute improvements to this project!
