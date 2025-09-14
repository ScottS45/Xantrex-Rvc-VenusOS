# Xantrex RV-C → Venus OS D-Bus Driver

**Date Updated:** Sep 14, 2025  
**Scott Sheen**

This is my first python project.  I created it so I could have my Xantrex inverter/charger talk to my Pi running venus os.

It is a Python service that reads Xantrex Freedom Pro RV-C (CAN) frames and publishes decoded values to Victron Venus OS D-Bus as inverter and charger services.

As of September 14, 2025, this is still under development with issues on the charger side.  The State for the inverter has an issue.

**NOTE:** Other inverter/charger devices that support RV-C may also work.  I see no reason why they wouldn't as long as they support the RV-C standard.  Would just need to tweak the product name, and the source.

- Inverter service: com.victronenergy.inverter.can_xantrex
- Charger service:  com.victronenergy.charger.can_xantrex

**Target:** Venus OS - I have tested with v3.15+ (Python 3.8 on Raspberry Pi). 

-------------------------------------------------------------------------------

## FEATURES

- Decodes key RV-C DGNs and maps them to Venus D-Bus paths (AC-in/out, DC bus, states, temperatures, flags).
- Creates two services (inverter + charger) for a single physical unit with shared identity/management.
- Heartbeat updates, /State path, and derived power calculations (e.g., P = V × I).
- Structured frame logging with --debug / --verbose (logs under /data/xantrex-monitor/logs/).
- Sends PGN requests on startup (Address Claimed, Product ID, Model Info, AC metrics, state, etc.).
- Default Can is Can10

-------------------------------------------------------------------------------

## REQUIREMENTS

- Venus OS device with Python 3.8 and D-Bus/GLib available.
- SocketCAN interface configured (default: can10, 250 kbps).
- Permission to access CAN sockets (root or appropriate capabilities).
- RV-C wiring to the Xantrex Freedom Pro.

-------------------------------------------------------------------------------

## INSTALL (VENUS OS)

**1) Place files on the device**
```text
/data/xantrex-monitor/
├─ xantrex_service.py    # this script
└─ velib_python/         # optional vendored copy; I still have to confirm this.  Do not recall at the moment
```

**2) Create logs directory**
```bash
mkdir -p /data/xantrex-monitor/logs
```

**3) Bring up the CAN interface (example; adapt to your setup)**
```bash
ip link set can10 up type can bitrate 250000
```

**4) Run the service**
```bash
python3 /data/xantrex-monitor/xantrex_service.py
```

**Optional Program parameters**
```text
--can IFACE    SocketCAN interface (default: can10)
--debug        Enable debug logging
--verbose      Very verbose logging
```

**Examples**
```bash
# Use any combination of flags. During initial testing, enable --debug (and optionally --verbose).
python3 /data/xantrex-monitor/xantrex_service.py --can can1
python3 /data/xantrex-monitor/xantrex_service.py --debug
python3 /data/xantrex-monitor/xantrex_service.py --debug --verbose
```

-------------------------------------------------------------------------------

## ARCHITECTURE NOTES

**Main loop:**
- GLib main loop drives CAN receive and D-Bus exports.

**D-Bus services:**
- Two services are registered—inverter and charger—exported via private SystemBus connections to avoid conflicts when multiple root objects are present.

**Source selection:**
- Xantrex default source is 0x42 (inverter/charger) and and though I did not see documentation it also seems use 0xD0 (inverter). Filtering ensures the correct DGN values update the intended service.

**Startup handshake:**
- On start, sends a burst of PGN requests to solicit identity and status.  For me it does not seem to respond to most.  (e.g., Address Claimed EE00, Product Identification FEEB, Model Info FFDE, AC metrics FFD7, state FFD4/FFD5, Software ID FEEF, segmented info 0EBFF). Responses populate /Info/*, /Firmware/*, /Mgmt/*, /State, and AC/DC paths.

**Mapping/decoding:**
- DGNs are decoded from frame payloads and mapped to canonical Venus paths. Derived values are computed where useful (e.g., power).

**Logging:**
- Structured logging supports --debug and --verbose. Frame counts and source IDs aid traceability. Logs default to /data/xantrex-monitor/logs/xantrex.log.

**Clean shutdown:**
- Signal handling exits the main loop, unregisters D-Bus objects, and closes CAN sockets to avoid stale bus names on restart.

-------------------------------------------------------------------------------

## CONTRIBUTIONS / FEEDBACK

- You are welcome to modify and adapt this code for your own setup.
- Please send improvements back (pull requests or patches) so others can benefit.
- Open an issue for bugs, new DGN decoders, or Venus OS path mapping suggestions.
- Unless otherwise noted, contributions are accepted under the same license (MIT).

-------------------------------------------------------------------------------

## LICENSE

MIT  See LICENSE.
