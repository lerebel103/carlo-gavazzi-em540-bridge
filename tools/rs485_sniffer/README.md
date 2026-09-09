# RS485 bus sniffer (temporary diagnostic)

A standalone, **passive** Modbus-RTU listener for debugging the intermittent
downstream dropout between the Fronius inverter (Modbus master) and this host's
TS65A bridge (slave).

It answers a question the bridge's own logging cannot: **is the inverter still
transmitting during the ~2-minute silences, and if so, are its frames reaching
the wire intact?** The bridge only sees bytes that its OS/pyserial layer
delivers; a frame corrupted or dropped below that layer is invisible to it. A
second, independent adapter tapping the same bus gives a view of the traffic
that is independent of the bridge's own serial stack.

> **Observer limitation.** This is still a UART/driver-level observer, not a
> bit-level capture. The sniffer's own USB-serial adapter and kernel driver can
> themselves drop or mangle bytes on framing/overrun/parity errors before this
> program ever sees them. So a **row-free interval is not proof that the bus was
> silent** — it only means *this adapter* received nothing decodable. Treat a
> silent window as strong-but-not-conclusive evidence. When you need bit-level
> certainty about whether the inverter was driving the bus, use a logic analyzer
> or oscilloscope on the A/B pair.

> This is a throwaway diagnostic. It is not part of the main bridge, ships as its
> own container, and should be removed once the root cause is found.

## How it works

RS485 is a shared multi-drop bus, so one listener sees **both** directions
(master requests and slave responses) as a single interleaved stream. There is
no hardware tx/rx split for a passive tap — direction is *inferred* from the
Modbus frame structure and recorded in the parsed log. The **raw** log captures
every byte regardless, including partial, bad-CRC, or garbage data, so nothing
is lost even when framing fails.

Frames are delimited by the Modbus RTU idle gap (>= 3.5 character times, derived
from the baud rate, with a small floor to absorb USB-serial latency jitter).

## Wiring

Connect your spare RS485 adapter's A/B to the **same** A/B pair as the existing
inverter<->bridge bus (parallel, read-only — do not enable any driver/TX on it).
It becomes an additional silent node. Keep termination correct for the bus as a
whole; a passive tap should not add a second terminator unless it sits at a
physical end of the line.

## Run it

Adjust `SNIFFER_PORT`, the `devices:` mapping, and the baud settings in
`docker-compose.sniffer.yaml` to your spare adapter and the bus you are tapping
(the TS65A serial link), then:

```sh
docker compose -f tools/rs485_sniffer/docker-compose.sniffer.yaml up -d --build
```

CSV logs are written to `tools/rs485_sniffer/sniffer-logs/` on the host.

### Output-directory permissions

The container runs as a non-root user, and the CSVs are written to the
bind-mounted `sniffer-logs/`. On Linux a bind mount keeps the **host**
directory's ownership, so if that directory is not writable by the container's
user the process fails to open the CSVs and restart-loops. To avoid this, the
compose file runs the container as your host UID/GID and adds the `dialout`
group for serial access:

```sh
mkdir -p tools/rs485_sniffer/sniffer-logs
cd tools/rs485_sniffer
# UID is a readonly variable in Bash, so pass the values via `env` (not a bare
# `UID=... GID=... docker compose`, which Bash rejects).
env UID="$(id -u)" GID="$(id -g)" docker compose -f docker-compose.sniffer.yaml up -d --build
```

Adjust the `dialout` group in the compose file if your serial device is owned by
a different group (`ls -l /dev/ttyUSB*` to check).

## Configuration (environment variables)

| Variable             | Default                  | Meaning                                  |
| -------------------- | ------------------------ | ---------------------------------------- |
| `SNIFFER_PORT`       | (required)               | Serial device, e.g. `/dev/ttyUSB3`       |
| `SNIFFER_BAUDRATE`   | `9600`                   | Bus baud rate                            |
| `SNIFFER_PARITY`     | `N`                      | `N`, `E`, or `O`                         |
| `SNIFFER_BYTESIZE`   | `8`                      | Data bits                                |
| `SNIFFER_STOPBITS`   | `1`                      | Stop bits                                |
| `SNIFFER_RAW_CSV`    | `/data/rs485_raw.csv`    | Raw byte log path (inside container)     |
| `SNIFFER_PARSED_CSV` | `/data/rs485_parsed.csv` | Parsed Modbus log path (inside container)|
| `SNIFFER_TZ` / `TZ`  | system local time        | Olson tz for local timestamps            |

## Output

Both CSVs share `seq`, `wall_local` (ISO-8601 local time, millisecond
precision), `mono` (monotonic seconds), and `delta_ms` (gap since the previous
record — this is where a stall shows up as one large value).

**`rs485_raw.csv`** — one row per frame boundary, nothing dropped:

```
seq,wall_local,mono,delta_ms,n_bytes,hex
```

**`rs485_parsed.csv`** — one row per frame, best-effort decoded:

```
seq,wall_local,mono,delta_ms,direction,dev_id,fc,addr,count,bytecount,exception,crc_ok,n_bytes,summary,hex
```

- `direction` — inferred `request` / `response` / `request/response`, or
  `unparsed` when the frame did not decode / failed CRC.
- `crc_ok` — `1`/`0`; a run of `0`s is direct evidence of line corruption.
- `summary` — human-readable decode, e.g. `read fc=3 addr=40071 count=58`.

## Reading a dropout

During a healthy period you should see the master's periodic reads and the
bridge's responses alternating with sub-second `delta_ms`. When the dropout
occurs, the parsed/raw logs distinguish the two competing hypotheses directly:

- **No rows during the silence** — the bus *appears* idle from this adapter.
  Strong evidence the inverter stopped transmitting, but not conclusive (see the
  observer-limitation note above: this adapter's own driver could have dropped
  the bytes). Confirm with a logic analyzer if certainty is required.
- **Rows present with `crc_ok=0` / `direction=unparsed`** — bytes/noise reached
  this adapter but did not form valid frames. This is evidence of corruption on
  the segment and is the case the bridge itself cannot see. Note it does **not**
  identify *who* transmitted: a passive tap sees both endpoints, and an unparsed
  frame has no reliable direction. Attribute traffic to the inverter only when a
  valid, request-shaped frame (`direction=request`, `crc_ok=1`) supports it.
