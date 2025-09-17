#!/usr/bin/env python3
# Version: 2.1.735.2025.09.16
# Date: 2025-09-16

# Xantrex Freedom Pro RV-C D-Bus Driver
#
# This script reads raw RV-C (CAN) data from a Xantrex Freedom Pro inverter/charger
# and publishes meaningful decoded values to the Venus OS D-Bus using com.victronenergy.inverter.can_xantrex.
# Other RV-C  supporting inverters chargers may work as well.
# 
# Author: Scott  Sheen
# website: sheenconsulting.com
#
# Compatible with: Venus OS (standard installations with dbus, GLib, and python3)
# Requirements: Connected CAN interface (default: can10) and appropriate udev/socket permissions
#
# Features:
# - Decodes known Xantrex RV-C DGNs and maps them to Victron D-Bus paths
# - Auto-registers standard D-Bus monitoring paths for inverter/chargers under /Mgmt/xantrex-rvc
# - Supports logging flags (--debug,  --verbose)
# - Includes /Status reporting, derived power calculations, and GLib heartbeat timers
#
# Based on Venus OS community conventions (e.g., GuiMods, dbus-serialbattery)
#
# Tip: Add udev rule for CAN permissions (e.g., /etc/udev/rules.d/99-can.rules):
# ACTION=="add", SUBSYSTEM=="net", KERNEL=="ncan10", RUN+="/sbin/ip link set up can10 type can bitrate 250000"


# --- Venus OS Compatibility Notes ---
# This service was developed on Venus OS v3.5 and forward-compatible with v3.6+
#
# IMPORTANT API Differences in VeDbusServiceWithMeta:
# - In v3.5:
#     • add_path() supports `description` only
#     • `unit` is NOT accepted → will raise: unexpected keyword argument 'unit'
# - In v3.6 and newer:
#     • add_path() supports both `description` and `unit`
#     • Recommended to call `set_register(False)` before path setup, then `registertemper
#
# Upgrading to Venus OS 3.6+ will unlock full D-Bus metadata support.



# --- Import standard and system libraries ---
import sys
import socket
import struct
import os
import errno
import logging
import signal
import time
import re
import dbus
import dbus.mainloop.glib

from collections   import defaultdict
from typing        import Any, Optional, Set, Dict, List
from gi.repository import GLib
import argparse



# ─── Load our locally vendored Velib Python library ───
# We have manually placed the velib_python package under
# /data/xantrex-monitor/velib_python so that:
#  • We control the exact Vedbus implementation (tested for no root-“/” binding)
#  • It lives in /data and survives both reboots and firmware updates
#  • We can safely instantiate two VeDbusServiceWithMeta services on one bus
#
# Prepending this path ensures all “import vedbus” calls use our vendored copy first.
sys.path.insert(0, '/data/xantrex-monitor/velib_python')


import vedbus


# --- Constants defining service identity and CAN parameters ---
DEVICE_INSTANCE     = 252       # just radomly picked, can not clash with one already on the system.
PRODUCT_ID          = 0xA045
FIRMWARE_VERSION    = '2.14'    # hard coded, matches mine.  Can be picked up by DGN though it is not transmitted when requested.
PRODUCT_NAME        = 'Freedom XC Pro'
SCRIPT_VERSION      = '2.1.705.2025.09.08'
MAX_UNMAPPED_DGNS   = 100

# ManufacturerCode = 119

# map raw RV-C codes → Venus OS GUI /State enum
RVC_INV_STATE = {
    0: 0,  # Not Available → Off
    1: 1,  # Stand-by → AES mode
    2: 9,  # Active → Inverting
    3: 0,  # Disabled → Off
    4: 4,  # Start-Inhibit → Absorption (best fit)
    5: 2,  # Overload → Fault
    6: 2,  # Short-Circuit → Fault
    7: 2,  # Over-Temperature → Fault
    8: 2,  # Under-Voltage → Fault
    9: 2,  # Over-Voltage → Fault
    10: 2,  # Internal-Fault → Fault
    11: 2,  # Comm-Lost → Fault
}

RVC_CHG_STATE = {
    0: 0,   # 0 NA/init       → Off
    1: 3,   # 1 Bulk          → Bulk
    2: 4,   # 2 Absorption    → Absorption
    3: 5,   # 3 Float         → Float
    4: 7,   # 4 Equalise      → Equalize
    5: 6,   # 5 Storage       → Storage
    6: 2,   # 6 Fault         → Fault
    # RV-C never sends 7–15, but Venus supports 8–11:
    8: 8,   #   — passthru     → Passthru
    9: 9,   #   — inverting    → Inverting
    10: 10, #   — assisting    → Assisting
    11: 11, #   — psu          → Power supply
}


# === CLI Argument Parsing ===
parser = argparse.ArgumentParser()
parser.add_argument('--debug', action='store_true', help='Enable debug logging')
parser.add_argument('--verbose', action='store_true', help='Full verbose logging')
parser.add_argument('--can',     default='can10',     metavar='IFACE',  help='SocketCAN interface to listen on (default: can10)')
args = parser.parse_args()

CAN_INTERFACE = args.can   # default is can10, that is what I used.
RCVBUF_BYTES  = 1 * 1024 * 1024          # we want 1 MiB payload room


# === Configure Logging ===
# make the dir if it is not there
# remove the existing log to start fresh.  

LOG_DIR = '/data/xantrex-monitor/logs'
os.makedirs(LOG_DIR, exist_ok=True)

log_file_path = f'{LOG_DIR}/xantrex.log'
if os.path.exists(log_file_path):
    os.remove(log_file_path)

logger = logging.getLogger("xantrex")
logger.handlers.clear()        # remove any inherited handlers
logger.propagate = False       # don’t duplicate to root logger

if args.debug or args.verbose:
    # choose level
    level = logging.DEBUG if args.debug else logging.INFO

    logger.setLevel(level)

    handlers = [
        logging.FileHandler(f'{LOG_DIR}/xantrex.log'),
        logging.StreamHandler(sys.stdout)      # explicit to stdout
    ]
    for h in handlers:
        h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(h)
else:
    logger.setLevel(logging.ERROR)
    fh = logging.FileHandler(f'{LOG_DIR}/xantrex.log')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)

# ─── SET UP D-BUS TO USE THE GLIB MAIN LOOP ───
# We need D-Bus events (method calls, signals, introspection requests)
# to be dispatched via GLib so they integrate with our CAN I/O loop.
# Calling DBusGMainLoop(set_as_default=True) here ensures that *any*
# subsequent BusConnection (including private ones) will use GLib.
# This must appear *before* creating any BusConnection, hence its
# placement at the top of the module.

dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
    
# Create a shared GLib-backed loop instance for all private connections
glib_loop = dbus.mainloop.glib.DBusGMainLoop()
   
def new_system_bus():
    # Return a private SystemBus connection that dispatches on our shared GLib loop, so multiple VeDbusService instances don’t collide on '/'.
    
    return dbus.SystemBus( mainloop = glib_loop, private = True )
    
    
    
# === Supported DGNs used by Venus OS ===
# ------------------------------------------------------------------------------
# Decoder functions for the DGN_MAPs that safely extract numeric values from CAN data.
# These functions automatically return None if the raw value is masked or invalid
# according to RV-C and J1939 standards:
#   - 0xFF       for 8-bit fields
#   - 0x7F       for signed 8-bit fields
#   - 0xFFFF     for 16-bit fields
#   - 0x7FFF/-32768 for signed 16-bit fields
#   - 0xFFFFFFFF for 32-bit fields
# This ensures masked data is skipped during D-Bus updates instead of being logged
# as large incorrect values (e.g., 6553.5 A or 4294967295 W).
# ------------------------------------------------------------------------------


# ── Pre-compiled struct formats ────────────────────────────────────────────
# A Struct caches parsing metadata in C, so calling the bound .unpack_from()
# avoids reparsing the format string on *every* frame.
# Each returns a tuple ⇒ we unpack with the trailing “,”.
_UNPACK_U8  = struct.Struct('<B').unpack_from
_UNPACK_S8  = struct.Struct('<b').unpack_from
_UNPACK_U16 = struct.Struct('<H').unpack_from
_UNPACK_S16 = struct.Struct('<h').unpack_from
_UNPACK_U32 = struct.Struct('<I').unpack_from
_UNPACK_S32 = struct.Struct('<i').unpack_from

# Big-endian versions
_UNPACK_U16_BE = struct.Struct('>H').unpack_from
_UNPACK_S16_BE = struct.Struct('>h').unpack_from

# UNSIGNED DECODERS
#   Return None when the spec defines the value as "not available"

def safe_u8(data: bytes | memoryview, offset: int, scale: float = 1.0) -> Optional[float]:
    # Unsigned 8-bit. 0xFF ⇒ NA per RV-C.
    if len(data) <= offset: return None
        
    value = data[offset]
    if value == 0xFF: return None
    result = value * scale
    return round(result, 3) if scale != 1.0 else result

def safe_u16(data: bytes | memoryview, offset: int, scale: float = 1.0, byteorder: str = "little") -> Optional[float]:
    # Unsigned 16-bit LE and BE. 0xFFFF ⇒ NA per RV-C.
    if len(data) < offset + 2:
        return None
        
    if byteorder == "little":
        raw, = _UNPACK_U16(data, offset)
    else:
        raw, = _UNPACK_U16_BE(data, offset)
    
    if raw == 0xFFFF:  # NA sentinel for u16
        return None
    if scale == 1.0:
        return raw  # Return integer directly  
        

    return round(raw * scale, 3)  # Return scaled and rounded float    
    

def safe_u32(data: bytes | memoryview, offset: int, scale: float = 1.0) -> Optional[float]:
    # Unsigned 32-bit LE. 0xFFFFFFFF ⇒ NA per RV-C.
    if len(data) < offset + 4: return None
        
    raw, = _UNPACK_U32(data, offset)
    
    if raw == 0xFFFFFFFF:  # NA sentinel for u32
        return None
    if scale == 1.0:
        return raw  # Return integer directly

    return round(raw * scale, 3)  # Return scaled and rounded float
        


# SIGNED DECODERS
#   Return None when the spec defines the value as "not available"
def safe_s8(data: bytes | memoryview, offset: int, scale: float = 1.0) -> Optional[float]:
    # Signed 8-bit. 0x7F ⇒ NA per RV-C.
    if len(data) <= offset: return None
        
    value = data[offset]
    if value == 0x7F: return None
        
    signed = value - 256 if (value & 0x80) else value
    result = signed * scale
    
    return round(result, 3) if scale != 1.0 else result

def safe_s16(data: bytes | memoryview, offset: int, scale: float = 1.0, byteorder: str = "little") -> Optional[float]:
    # Signed 16-bit LE and BE. 0x7FFF ⇒ NA per RV-C.
    if len(data) < offset + 2: return None
        
    if byteorder == "little":
        raw, = _UNPACK_S16(data, offset)
    else:
        raw, = _UNPACK_S16_BE(data, offset)
    
    return None if raw == 0x7FFF else round(raw * scale, 3)

def safe_s32(data: bytes | memoryview, offset: int, scale: float = 1.0) -> Optional[float]:
    # Signed 32-bit LE. 0x7FFFFFFF ⇒ NA per RV-C.
    if len(data) < offset + 4: return None
        
    raw, = _UNPACK_S32(data, offset)
    return None if raw == 0x7FFFFFFF else round(raw * scale, 3)


# OTHER DECODERS
    

def safe_bit(byte_val: int, mask: int) -> Optional[bool]:
    # Return True if the bitmask is set, False if clear, or None if the whole byte is marked Not-Available. 
    
    if byte_val == 0xFF:            # RV-C “not available”
        return None
        
    return bool(byte_val & mask)
    
    
def safe_ascii(data_slice):
    if all(b == 0xFF for b in data_slice):
        return None
    try:
        cleaned = bytes(data_slice).rstrip(b'\xFF')  # <-- convert then rstrip
        return cleaned.decode('ascii').strip()
    except UnicodeDecodeError:
        return None       

    
def fahrenheit_to_c(val):
    return None if val is None else round((val - 32) * 5/9, 1)



# === DGN Map: Decoders from RV-C DGNs to D-Bus paths ===
# Format: DGN : [(dbus_path, decode_function), ...]
# Each DGN (Diagnostic Group Number) corresponds to a specific RV-C data packet
# The lambda decoders extract meaningful values (voltage, current, state, etc.) from the binary payload
# Units and scaling factors are defined by the RV-C specification or device-specific implementation

# ─────────────────────────────────────────────────────────────────────────────
#  INVERTER_DGN_MAP  
# ─────────────────────────────────────────────────────────────────────────────
INVERTER_DGN_MAP = {
    0x1FFD4: [  # INVERTER_STATUS              This is charger if the address is the primary *(0x42 default) or inverter if not that.
        ('/State',                                  lambda d: RVC_INV_STATE.get((int(safe_u8(d, 0) or 0)) & 0x0F, 0),  '',  'Inverter operational state (0=Off,1=Standby,2=InvOnly,3=Bypass,4=Inv+Chg)'),
        ('/Ac/Out/LoadPercent',                     lambda d: safe_u8(d, 1),        '%',     'Percent of rated output'),
        # Bytes 3–7 are NA
    ],
    0x1EE00: [  # ADDRESS_CLAIMED / NAME
        ('/Mgmt/ManufacturerCode',     lambda d: ((int.from_bytes(d[:8], 'little') >> 21) & 0x7FF) if len(d) >= 8 else None,  '', 'RV-C manufacturer code (119 = Xantrex)'),
        ('/Mgmt/Function',             lambda d: ((int.from_bytes(d[:8], 'little') >> 40) & 0xFF)  if len(d) >= 8 else None,  '', 'RV-C function (129 = Inverter/Charger)'),
    ],    
    0x1FEEF: [  # SOFTWARE_IDENTIFICATION  Ignored by Charger
        ('/Firmware/PartNumber',       lambda d: safe_ascii(d[0:16]),            '',      'FW part-number (optional)'),
        ('/Firmware/BuildDate',        lambda d: safe_ascii(d[16:24]),           '',      'FW build date YYYYMMDD'),
    ],
    0x1FFDE: [  # INVERTER_MODEL_INFO
        ('/Info/Model',                lambda d: safe_ascii(d[0:8]),             '',      'ASCII model string'),
        ('/Info/Serial',               lambda d: safe_ascii(d[8:20]),            '',      'Serial number'),
        ('/Info/HardwareVersion',      lambda d: safe_ascii(d[20:28]) if len(d) >= 28 else '',  '', 'Hardware revision'),
    ],
    0x1FFDC: [  # INVERTER_AC_STATUS_2
        ('/Ac/In/V',                   lambda d: safe_u16(d, 0, 0.05),           'V',     'AC Input Voltage'),
        ('/Ac/In/F',                   lambda d: safe_u16(d, 2, 0.01),           'Hz',    'AC Input Frequency'),
        ('/Ac/In/L1/V',                lambda d: safe_u16(d, 0, 0.05),           'V',     'AC Input L1 Voltage (alias)'),
        ('/Ac/In/L1/F',                lambda d: safe_u16(d, 2, 0.01),           'Hz',    'AC Input L1 Frequency (alias)'),
        ('/Ac/In/L1/I',                lambda d: safe_u16(d, 4, 0.1),            'A',     'AC Input L1 Current'),
        ('/Ac/Grid/L1/V',              lambda d: safe_u16(d, 0, 0.05),           'V',     'AC Input L1 Voltage (Grid)'),
        ('/Ac/Grid/L1/I',              lambda d: safe_u16(d, 4, 0.1),            'A',     'AC Input L1 Current (Grid)'),
        ('/Ac/Grid/L1/ApparentPower',  lambda d: (None if safe_u16(d, 0, 0.01) is None or safe_u16(d, 4, 0.01) is None else round(safe_u16(d, 0, 0.01) * safe_u16(d, 4, 0.01), 1)), 'VA', 'AC Input L1 Apparent Power (Grid)'),
    ],
    0x1FFD7: [  # INVERTER_AC_STATUS_1
        ('/Ac/Out/L1/V',               lambda d: safe_u16(d, 1, 0.05),           'V',     'AC Output L1 Voltage'),
        ('/Ac/Out/L1/I',               lambda d: safe_u8(d,  3, 0.1),            'A',     'AC Output L1 Current'),
        ('/Ac/Out/L1/F',               lambda d: safe_u16(d, 5, 2.0, 'big'),     'Hz',    'AC Output Frequency'),
        ('/Ac/Out/V',                  lambda d: safe_u16(d, 1, 0.05),           'V',     'AC Output L1 Voltage'),
        ('/Ac/Out/I',                  lambda d: safe_u8(d,  3, 0.1),            'A',     'AC Output L1 Current'),
        ('/Ac/Out/F',                  lambda d: safe_u16(d, 5, 2.0, 'big'),     'Hz',    'AC Output Frequency'),
    ],
     0x1FEA2: [  # INVERTER_STATUS_2 (DC Input Voltage & Current)
        ('/Dc/0/Voltage',              lambda d: safe_u16(d, 2, 0.05),           'V',     'DC Input Voltage'),
        ('/Dc/0/Current',              lambda d: safe_u16(d, 4, 0.01),           'A',     'DC Input Current'),
    ],
    0x1FFCD: [  # INVERTER_APS_STATUS
        ('/Ac/Out/L1/F',               lambda d: safe_u16(d, 5, 2.0, 'big'),     'Hz',    'AC Output L1 Frequency'),
        ('/Ac/Out/L1/S',               lambda d: safe_u16(d, 2),                 'VA',    'AC Output L1 Apparent Power'),
        ('/Ac/Out/L1/P',               lambda d: safe_u16(d, 4),                 'W',     'AC Output L1 Active Power'),
        ('/Ac/Out/L1/Q',               lambda d: safe_u16(d, 6),                 'VAR',   'AC Output L1 Reactive Power'),
    ],
    0x1FFCC: [  # INVERTER_DCBUS_STATUS
        ('/Dc/0/Voltage',              lambda d: safe_u16(d, 0, 0.1),            'V',     'DC Voltage'),
        ('/Dc/0/Current',              lambda d: safe_u16(d, 2, 0.1),            'A',     'DC Current'),
    ],
    0x1FFCB: [  # INVERTER_OPS_STATUS
        ('/State',                     lambda d: safe_u8(d, 0),                  '',      'Inverter State'),
        ('/Error',                     lambda d: safe_u8(d, 1),                  '',      'Inverter Error Code'),
    ],
    0x1FFD5: [  # INVERTER_AC_STATUS_3
        ('/Ac/Out/L1/Flags',          lambda d: safe_u8(d,  1),                  '',      'Waveform & phase flags'),
        ('/Ac/Out/L1/P',              lambda d: safe_u16(d, 2),                  'W',     'Real power'),
        ('/Ac/Out/L1/Q',              lambda d: safe_u16(d, 4),                  'VAR',   'Reactive power'),
        # bytes 6–7 reserved/not available
    ],
    0x1FFD6: [  # INVERTER_AC_STATUS_2
        ('/Ac/Out/V',                  lambda d: safe_u16(d, 0, 0.05),           'V',     'AC Voltage'),
        ('/Ac/Out/F',                  lambda d: safe_u16(d, 2, 0.01),           'Hz',    'AC Frequency'),
    ],
    0x1FF8F: [  # INVERTER_AC_STATUS_4
        ('/Ac/Out/Instance',           lambda d: safe_u8(d, 0),                  '',      'AC-point instance'),
        ('/Ac/Out/VoltageFaultCode',   lambda d: safe_u8(d, 1),                  '',      'Voltage-fault enumeration'),
        ('/Ac/Out/Fault/Surge',        lambda d: safe_bit(d[2], 0x03),           '',      'Surge-protection fault'),
        ('/Ac/Out/Fault/FreqHigh',     lambda d: safe_bit(d[2], 0x0C),           '',      'High-frequency fault'),
        ('/Ac/Out/Fault/FreqLow',      lambda d: safe_bit(d[2], 0x30),           '',      'Low-frequency fault'),
        ('/Ac/Out/BypassActive',       lambda d: safe_bit(d[2], 0x40),           '',      'Bypass mode active'),
        ('/Ac/Out/QualificationStatus',lambda d: (int(safe_u8(d, 3)) & 0x0F) if safe_u8(d, 3) is not None else None, '',      'Qualification status (0-4)'),
    ],
    0x1FFDD: [  # INVERTER_POWER_SUMMARY
        ('/Ac/Out/Total/P',            lambda d: safe_s16(d, 0, 1.0),            'W',     'Total Active Power'),
        ('/Ac/Out/Total/Q',            lambda d: safe_s16(d, 2, 1.0),            'VAR',   'Total Reactive Power'),
        ('/Ac/Out/Total/S',            lambda d: safe_u16(d, 4, 1.0),            'VA',    'Total Apparent Power'),
        ('/Ac/Out/Total/PowerFactor',  lambda d: safe_s16(d, 6, 0.001),          '',      'Power Factor'),
    ],
    0x1FEE8: [  # INVERTER_DC_STATUS
        ('/Dc/0/Voltage',              lambda d: safe_u16(d, 0, 0.05, 'big'),    'V',     'DC 0 Voltage'),
        ('/Dc/0/Current',              lambda d: safe_s16(d, 2, 0.02, 'big'),    'A',     'DC 0 Current'),
    ],
    0x1FEBE: [  # INVERTER_LOAD_PRIORITY
        ('/Settings/InputPriority',    lambda d: safe_u8(d, 0),                  '',      'Input Priority'),
        ('/Settings/LoadSheddingMode', lambda d: safe_u8(d, 1),                  '',      'Load Shedding Enabled'),
        ('/Settings/InverterMode',     lambda d: safe_u8(d, 2),                  '',      'Inverter Operating Mode'),
    ],
    0x1FFF1: [  # AC_INPUT_LIMITS
        ('/Ac/In/L1/CurrentLimit',     lambda d: safe_u8(d,0),                   'A',     'AC Input L1 Current Limit'),
    ],
    0x1FFB0: [  # AC_PASS_THROUGH_CONFIG
        ('/Ac/PassThrough/Enabled',    lambda d: safe_u8(d, 0),                  '',      'Pass Through Mode Enabled'),
        ('/Ac/PassThrough/Source',     lambda d: safe_u8(d, 1),                  '',      'Pass Through Source Selection'),
        ('/Ac/PassThrough/Delay',      lambda d: safe_u16(d, 2, 0.1),            's',     'Pass Through Delay Time'),
    ],
    0x0EEFF: [  # INVERTER_ACTIVITY_STATUS (heartbeat)
        ('/Mgmt/ProcessAlive',         lambda d: safe_u8(d, 0),                  '',      'Heartbeat value (non-zero = alive)'),
    ],

    0x1FE80: [  # AC_OUTPUT_STATUS – line-to-line voltage
        ('/Blackhole',                 lambda d: None, '',''),  # Discard without processing
        #('/Ac/Out/L1/Voltage',         lambda d: safe_u8(d, 1),                  'V',     'AC Output L1 Voltage'),
    ],
    0x1FE82: [  # AC_OUTPUT_FREQUENCY
       ('/Blackhole',                  lambda d: None, '',''),  # Discard without processing
       #('/Ac/Out/L1/Frequency',       lambda d: safe_u16(d, 2, 0.5) or 60,     'Hz',    'AC Output frequency'),
    ],
    0x0FECA: [  # Inverter Loss
       ('/Energy/InverterOut',         lambda d: safe_u16(d, 0, 1, 'big'),       'Wh',   'Accumulated inverter output energy'        ),   # bytes 0–1 BE, 1 Wh/bit
    ],
    0x1FECA: [  # priority-1
       ('/Energy/InverterOut',         lambda d: safe_u16(d, 0, 1, 'big'),       'Wh',   'Accumulated inverter output energy'        ),   # bytes 0–1 BE, 1 Wh/bit
    ],    
    0x1FFB7: [  # INVERTER_STATE
        ('/sState',                     lambda d: int(safe_u8(d, 0) or 0),        '',      'Inverter state (Victron enum)'),        # I disabled this path, or have so we just use the other dgn.
    ],
    0x1FEB3: [  # ALARM_EVENT
        ('/Alarms/RvcEvent',           lambda d: safe_u8(d, 0),                  '',      'RV-C alarm/event code'),
    ],
    0x1FFFC: [  # NETWORK_STATUS
        ('/Mgmt/NetworkState',         lambda d: 0 if len(d) == 8 and all(b == 0 for b in d) else 1, '',    'Network state (all zeros = idle)'),
    ],
    0x1FFBE: [  # AC_PASS_THROUGH_STATUS
        ('/Ac/PassThrough/Active',     lambda d: safe_u8(d, 0),                  '',      'Pass Through Active Flag'),
        ('/Ac/PassThrough/LoadShare',  lambda d: safe_u8(d, 1),                  '',      'Load Sharing Mode'),
        ('/Ac/PassThrough/SyncStatus', lambda d: safe_u8(d, 2),                  '',      'Synchronization Status'),
    ]
}

# ─────────────────────────────────────────────────────────────────────────────
#  CHARGER_DGN_MAP
# ─────────────────────────────────────────────────────────────────────────────
CHARGER_DGN_MAP = {
    0x1FDFF: [  # CHARGER_MODE_STATUS
        ('/Mode',                    lambda d: safe_u8(d, 0),                    '',      'Charger mode (standby)'),
    ],
    0x1FFC7: [  # CHARGER_STATUS
        ('/State',                   lambda d: RVC_CHG_STATE.get((int(safe_u8(d, 0) or 0)) & 0x0F, 0),  '',      'Charger State'),
        ('/Dc/0/PowerPercent',       lambda d: safe_u8(d, 2),                    '%',     'Charger Power Percent'),        
        #('/Dc/0/Voltage',            lambda d: safe_u8(d, 0, 0.01),              'V',     'Charger Voltage'),
        #('/Dc/0/Current',            lambda d: safe_u8(d, 1, 0.1),               'A',     'Charger Current'),

    ],
    0x1FEA3: [  # CHARGER_STATUS_2 (Battery Voltage & Current)
        ('/Dc/0/Voltage',            lambda d: safe_u16(d, 2, 0.05),             'V',     'Battery Voltage'),
        ('/Dc/0/Current',            lambda d: safe_u16(d, 4, 0.05),             'A',     'Battery Charge Current'),
        ('/Dc/0/Temperature',        lambda d: safe_s8(d, 6),                    '°C',    'Charger Temperature'),        
    ],
    0x1FFC8: [  # CHARGER_AC_STATUS_3 AC Input L1 
        ('/Ac/In/L1/Flags',          lambda d: safe_u8(d, 1),                    '',    'Waveform & phase flags'),
        ('/Ac/In/L1/Distortion',     lambda d: safe_u8(d, 6),                    '%',   'Harmonic distortion'),
        # byte 7 complementary‑leg instance (NA here)
    ],
    0x1FFC6: [  # CHARGER_CONFIGURATION_STATUS
        ('/Status',                  lambda d: safe_u8(d, 0),                    '',      'Charger Status'),
        ('/TargetVoltage',           lambda d: safe_u16(d, 1, 0.01),             'V',     'Target Voltage'),
        ('/TargetCurrent',           lambda d: safe_u16(d, 3, 0.1),              'A',     'Target Current'),
        ('/MaximumCurrent',          lambda d: safe_u16(d, 5, 0.1),              'A',     'Maximum Current'),
    ],
    0x1FFC5: [  # CHARGER_COMMAND
        ('/Mode',                    lambda d: safe_u8(d, 0),                    '',      'Charger Mode'),
        ('/Enabled',                 lambda d: int(safe_u8(d, 0) in (1, 2)),     '',      'Charger Enabled Flag'),
    ],
    0x1FFC2: [  # CHARGER_APS_STATUS
        ('/Frequency',               lambda d: safe_u8(d, 0, 0.01),              'Hz',    'Charger Frequency'),
        ('/Ac/In/L1/V',              lambda d: safe_u16(d, 2, 0.01),             'V',     'Charger Input L1 Voltage'), 
        ('/Ac/In/L1/I',              lambda d: safe_u16(d, 4, 0.05),             'A',     'Charger Input L1 Current'), 
    ],
    0x1FFC1: [  # CHARGER_DCBUS_STATUS
        ('/Dc/0/Voltage',            lambda d: safe_u8(d, 0, 0.01),              'V',     'DC Bus Voltage 1'),
        ('/Dc/0/Current',            lambda d: safe_u8(d, 1,  0.1),              'A',     'DC Bus Current 1'),
    ],
    0x1FFC0: [  # CHARGER_OPS_STATUS
        ('/State',                   lambda d: safe_u8(d, 0),                    '',      'Charger State'),
        ('/Error',                   lambda d: safe_u8(d, 1),                    '',      'Charger Error'),
    ],
    0x1FF98: [  # CHARGER_EQUALIZATION_CONFIG_STATUS
        ('/BulkTimeLimit',           lambda d: safe_u8(d, 0),                    '',      'Bulk Phase Time Limit'),
        ('/AbsorptionTimeLimit',     lambda d: safe_u8(d, 1),                    '',      'Absorption Phase Time Limit'),
        ('/EqualizationTimeLimit',   lambda d: safe_u8(d, 2),                    '',      'Equalization Phase Time Limit'),
    ],
    0x1FEBF: [  # CHARGER_CONFIG_STATUS_4
        ('/FloatTimeLimit',          lambda d: safe_u8(d, 0),                    '',      'Float Phase Time Limit'),
    ],
    # ─────────────────────────────────────────────────────────────────────
    # 0x1FFC9  —  Dual-use PGN on Xantrex Freedom XC / Pro
    #
    # According to the Xantrex RV-C guide this PGN alternates every second:
    #
    #   even seconds  →  “CHARGER_AC_STATUS_2”
    #                    • L2 line-to-neutral voltage / current / power
    #                    • meant for split-phase systems the Freedom XC
    #                      does not actually support
    #
    #   odd  seconds  →  “CHARGER_APS_STATUS”
    #                    • Auxiliary-Power-Supply (12 V) telemetry
    #                      (instance#, count, V, I, internal °C)
    #
    # Venus OS ignores the L2 metrics on single-phase inverters, but APS
    # data can be useful for diagnostics (Node-RED, MQTT, VRM widgets).
    #
    # Therefore we decode *only* the APS variant below.  If a future
    # model supplies genuine split-phase numbers, add a dynamic decoder
    # that inspects byte 0 and routes to /Ac/In/L2/* paths as needed.
    # ─────────────────────────────────────────────────────────────────────    
    0x1FFC9: [   # CHARGER_APS_STATUS
        # dynamic router: first byte tells which variant we received
        # ------------------------------------------------------------
        # Battery variant (byte 0 == 0x01)
        ('/Battery/Instance',               lambda d: safe_u8(d, 0)        if d[0] == 0x01 else None,  '',    'Battery Instance'),
        ('/Soc',                            lambda d: safe_u8(d, 1) * 0.4  if d[0] == 0x01 else None,  '%',   'State of Charge'),
        ('/Battery/Soh',                    lambda d: safe_u8(d, 2) * 0.4  if d[0] == 0x01 else None,  '%',   'State of Health'),
        ('/Battery/Mode',                   lambda d: safe_u8(d, 3)        if d[0] == 0x01 else None,  '',    'Battery Mode'),
        ('/Battery/Voltage',                lambda d: safe_u16(d, 4, 0.01) if d[0] == 0x01 else None,  'V',   'Battery Voltage'),
        ('/Battery/Current',                lambda d: safe_u16(d, 6, 0.1)  if d[0] == 0x01 else None,  'A',   'Battery Current'),
        ('/Battery/Power',                  lambda d: ( None if d[0] != 0x01 
                                                               or safe_u16(d, 4, 0.01) is None 
                                                               or safe_s16(d, 6, 0.1) is None
                                                             else round(safe_u16(d, 4, 0.01) * safe_s16(d, 6, 0.1), 1)),
                                                                                                       'W',   'Battery Power'),
        # APS variant (byte 0 == 0x02) – keep if you want the 12 V aux data
        ('/Dc/Aux/Instance',                lambda d: safe_u8(d, 0) if d[0] == 0x02 else None,         '',    'APS Instance'),
        ('/Dc/Aux/Voltage',                 lambda d: safe_u16(d, 2, 0.05) if d[0] == 0x02 else None,  'V',   'APS output voltage'),
        ('/Dc/Aux/Current',                 lambda d: safe_u16(d, 4, 0.05) if d[0] == 0x02 else None,  'A',   'APS output current'),
        ('/Dc/Aux/Temperature',             lambda d: safe_s8(d, 6) if d[0] == 0x02 else None,         '°C',  'APS internal temp'),
    ],    
    0x1CA42: [  # CHARGER_STATUS_FLAGS
        ('/StatusFlags',                    lambda d: safe_u8(d, 0),                '',      'Charger Status Flags'),
        ('/Flag/Enabled',                   lambda d: safe_bit(d[0], 0x01),         '',      'Charger Enabled'),
        ('/Flag/Derating',                  lambda d: safe_bit(d[0], 0x02),         '',      'Charger Derating Active'),
        ('/Flag/BattLowVoltage',            lambda d: safe_bit(d[0], 0x04),         '',      'Battery Voltage Too Low'),
        ('/Flag/BattHighVoltage',           lambda d: safe_bit(d[0], 0x08),         '',      'Battery Voltage Too High'),
        ('/Flag/BattHighTemp',              lambda d: safe_bit(d[0], 0x10),         '',      'Battery Temperature Too High'),
        ('/Flag/BattLowTemp',               lambda d: safe_bit(d[0], 0x20),         '',      'Battery Temperature Too Low'),
        ('/Flag/ChargerHighTemp',           lambda d: safe_bit(d[0], 0x40),         '',      'Charger Temperature Too High'),
        ('/Flag/ChargerLowTemp',            lambda d: safe_bit(d[0], 0x80),         '',      'Charger Temperature Too Low'),
    ],
    0x0CA42: [  # CHARGER DECODERS
        ('/FanSpeed',                       lambda d: safe_u8(d, 0),                '',      'Charger Fan Speed'),
        ('/Derating',                       lambda d: safe_u8(d, 1),                '%',     'Charger Derating (%)'),
        ('/InputMode',                      lambda d: safe_u8(d, 2),                '',      'Charger Input Mode'),
        ('/InputSource',                    lambda d: safe_u8(d, 3),                '',      'Charger Input Source'),
    ]
}

# A map of the DGNs that will be sent to both services
COMMON_DGN_MAP = {
    0x0EBFF: [
        # Pulls the U3 firmware version (e.g., "2.14") if the current 0x0EBFF segment contains "U3:"  this is hardcoded at first
        ('/FirmwareVersion',                        lambda d: None, '', 'A multi frame DGN that needs to be assembled'),
    ],
    0x1FEEB: [  # PRODUCT_IDENTIFICATION (duplicated)
        ('/FirmwareVersion',                        lambda d: f"{safe_u8(d, 4)}.{safe_u8(d, 5)}",   '',      'Firmware major.minor'),
        ('/ProductId',                              lambda d: safe_u16(d, 2),                       '',      'Numeric product identifier'),
        ('/Mgmt/ManufacturerCode',                  lambda d: ((d[0] & 0x1F) | ((d[1] & 0xE0) << 3) | ((d[1] & 0x03) << 8)),     '',         '11-bit manufacturer code'),
        # not included so we use the hard coded one.
        #('/DeviceInstance',                         lambda d: safe_u8(d, 6),                        '',      'Device instance (node)'),        
    ],
    0x1FFDF: [  # INVERTER_GRID_DETECTION_STATUS
        ('/Ac/Grid/Status',                         lambda d: safe_u8(d, 0),        '',      'Grid Detection Status Code'),
        ('/Ac/Grid/PhaseAlignment',                 lambda d: safe_u8(d, 1),        '',      'Phase Match Indicator'),
        ('/Ac/Grid/FaultFlags',                     lambda d: safe_u8(d, 2),        '',      'Grid Fault Flags'),
    ],
    0x1FFFD: [  # DC Source Status 1  
        ('/Dc/0/Instance',                          lambda d: safe_u8(d, 0),        '',      'DC Source Instance'),
        ('/Dc/0/DevicePriority',                    lambda d: safe_u8(d, 1),        '',      'DC Source Device Priority'),
        ('/Dc/0/Voltage',                           lambda d: safe_u16(d, 2, 0.05), 'V',     'DC Source Voltage'),
        # DC Source Current  Expected on FEA3
    ],
    0x1FFCA: [  # CHARGER_AC_STATUS_1
        ('/Ac/In/L1/V',              lambda d: safe_u16(d, 1, 0.05),             'V',     'AC Input L1 Voltage'),
        ('/Ac/In/L1/I',              lambda d: safe_u16(d, 4, 0.1),              'A',     'AC Input L1 Current'),
        ('/Ac/In/L1/P',              lambda d: (None
                                               if safe_u16(d, 1, 0.05) is None
                                               or safe_u16(d, 4, 0.1) is None
                                               else round(safe_u16(d, 1, 0.05) * safe_u16(d, 4, 0.1), 1)),
                                                                                 'W',     'AC Input L1 Power'),
        ('/Ac/ActiveIn/L1/V',        lambda d: safe_u16(d, 1, 0.05),             'V',     'Active AC Input L1 Voltage'),
        ('/Ac/ActiveIn/L1/I',        lambda d: safe_u16(d, 4, 0.1),              'A',     'Active AC Input L1 Current'),
        ('/Ac/ActiveIn/L1/P',       lambda d: (None
                                               if safe_u16(d, 1, 0.05) is None
                                               or safe_u16(d, 4, 0.1) is None
                                               else round(safe_u16(d, 1, 0.05) * safe_u16(d, 4, 0.1), 1)),
                                                                                 'W',     'Active AC Input L1 Power'),
    ],    
    0x1FDA0: [  # DC_SOURCE_LOAD_CONTROL
        ('/Dc/Source/LoadControl/Status',           lambda d: safe_u8(d, 0),        '',      'Load Control Status'),
        ('/Dc/Source/LoadControl/Reason',           lambda d: safe_u8(d, 1),        '',      'Load Control Reason'),
        ('/Dc/Source/LoadControl/TimeUntilRestart', lambda d: safe_u16(d, 2),       's',     'Time Until Restart'),
        ('/Dc/Source/LoadControl/RetryCount',       lambda d: safe_u8(d, 4),        '',      'Retry Count'),
        ('/Dc/Source/LoadControl/MaxRetries',       lambda d: safe_u8(d, 5),        '',      'Maximum Retries'),
        ('/Dc/Source/LoadControl/Command',          lambda d: safe_u8(d, 6),        '',      'Load Control Command'),
        ('/Dc/Source/LoadControl/Flags',            lambda d: safe_u8(d, 7),        '',      'Flags'),
    ],
    0x1FEDD: [  # INVERTER_TEMPERATURES
        ('/Temp/Ambient',                           lambda d: safe_s16(d, 0, 1.0),  '°C',    'Ambient Temperature'),
        ('/Temp/Board',                             lambda d: safe_s16(d, 2, 1.0),  '°C',    'Board Temperature'),
        ('/Temp/InputFET',                          lambda d: safe_s16(d, 4, 1.0),  '°C',    'Input FET Temperature'),
        ('/Temp/OutputFET',                         lambda d: safe_s16(d, 6, 1.0),  '°C',    'Output FET Temperature'),
    ],
    0x1FEBD: [  # INVERTER_TEMPERATURE_STATUS  (shared with Charger)
        # does not seem to be send data for this 
        #('/Temp/FET',                              lambda d: safe_s8(d, 1),        '°C',    'FET temperature'),
        ('/Temp/Transformer',                       lambda d: safe_s8(d, 2),        '°C',    'Transformer temperature'),
    ],
    0x0E842: [  # INVERTER_TEMPERATURES  (shared with Charger)
        ('/Temp/Transformer',                       lambda d: safe_s8(d, 1),        '°C',    'Transformer Temperature'),
        ('/Temp/MOSFET',                            lambda d: safe_s8(d, 2),        '°C',    'MOSFET Temperature'),
        ('/Temp/Heatsink',                          lambda d: safe_s8(d, 3),        '°C',    'Heatsink Temperature'),
    ]
}


# DGNs required for derived power calculations
DERIVED_DGNS = {
    0x1FFD6,  # AC-Out RMS (V, F)            – ensures /Ac/Out/V,F are fresh
    0x1FFDD,  # AC-Out power summary         – total P, Q, S, PF
    0x1FFCA,  # AC-In / Active-In L1 (V, I)  – feeds /Ac/In/L1/Power
    0x1FFCC,  # DC-Bus voltage & current     – /Dc/0/Voltage, /Dc/0/Current
    0x1FEE8   # Battery bank voltage/current – /Dc/1/Voltage, /Dc/1/Current
}

    #0x1FFD7,  # AC-Out L1 (V, I)             – drives /Ac/Out/L1/Power  /    FFD5 is returned so we use that instead



# =============================================================================
# EXTENSION: Add `.update()` support to VeDbusItemExport without monkeypatching
# -----------------------------------------------------------------------------
# Goal: Allow our code to call `.update({'unit': ..., 'description': ...})`
#       on each D-Bus path we register.
#
# Problem: The default `vedbus.py` in Venus OS does NOT
#          support `.update()` on the objects returned by add_path().
#
# Solution: We subclass both VeDbusItemExport and VeDbusService to safely
#           introduce `.update()` support — cleanly and locally — without:
#           ❌ modifying system vedbus.py
#           ❌ monkeypatching global class methods
# =============================================================================

#  This class extends the D-Bus path object to support .update(meta)
class VeDbusItemExportWithMeta(vedbus.VeDbusItemExport):
    def __init__(self, path, initial_value=None, writeable=False, onchange=None):
        # This ensures D-Bus internals like _locations are properly initialized
        super().__init__(path, initial_value, writeable, onchange)
        self.value = initial_value  # for decoder logic

    
    def update(self, meta: dict):
        """
        Allows setting additional metadata on the D-Bus path, such as:
        - unit
        - description
        Mimics newer vedbus.py behavior (as seen in 3.6+ or GuiMods projects).
        """
        
        for key, value in meta.items():
            setattr(self, key, value)


#  This class overrides the default add_path() to use our extended object
class VeDbusServiceWithMeta(vedbus.VeDbusService):
    def __init__(self, servicename, bus, object_path=None):
        # Initialize the parent VeDbusService class
        super().__init__(servicename, bus, object_path)
        
        # Explicitly initialize exported_paths as a dictionary
        self.exported_paths = {}    
        
        
    def add_path(self, path, value=None, writeable=False, onchange=None, unit=None, description=None):
        """
        - Calls base method to create and register object
        - Promotes to subclass to add .update() and .value
        - Returns safe, fully initialized object
        """

        # Step 1: Register via base method (handles all D-Bus internals)
        base_item = super().add_path(path, value, writeable, onchange)

        # Step 2: Promote to subclass that supports .update()
        item = VeDbusItemExportWithMeta.__new__(VeDbusItemExportWithMeta)
        item.__dict__ = base_item.__dict__

        # Step 3: Add decoding support
        item.value = value

        # Step 4: Attach optional metadata
        # Prepare metadata dictionary from provided unit/description
        meta = {}
        if unit:
            meta["unit"] = unit
        if description:
            meta["description"] = description

        # If any metadata is provided, apply it via .update()
        if meta:
            item.update(meta)
            
        # Step 5: Store the item in exported_paths
        self.exported_paths[path] = item

        return item


# === D-Bus Service Class ===
class XantrexService:
    def __init__(self, *, debug=False, verbose=False):
        # Flags to control runtime logging behavior
        self.debug = debug
        self.verbose = verbose

        # Runtime counters and internal state
        self.frame_count         = 0                 # Total CAN frames received
        self.error_count         = 0                 # Total decode errors
        self.loop                = None              # Reference to the GLib main loop        
        self.last_heartbeat      = time.time()       # Timestamp of last valid frame received
        self.last_dgn            = 0 
        self.last_src            = 0
        self.unmapped_seen       = set()             # DGNs we've seen but aren't in the DGN_MAPs
        self.unmapped_counts     = {}                # Unmapped DGN => count of how many times it's seen
        self.heartbeat_counter   = 0        
        self._frame_counter      = 0
        
        # Xantrex discovery (EE00)
        # we should not be hard coding these values, but xantrex does not seem to respond to the request
        self.xantrex_sources       = {0x42: 129, 0xD0: 128 }   # = inverter/charger; accept SA 0x42 from boot   /  dict: SA -> function (NAME byte 5)  store what is found from the EE00 addresses claimed request  D0 = inverter?
        self.multiframe_assemblies = {}  # {sa: {"len","exp","got","seq","pgn","buf","deadline"}}
        self.SA_toSkip             = set()
        self.primary_source        = 0x42 # default to this as the primary Source address.  We will confirm this via a request for EE00    - do not seem to get a response to ee00
        

        logger.info(f"Initializing Xantrex Service on {CAN_INTERFACE}")

        # This script creates two D-Bus services for the Xantrex Freedom Pro 3000, a single unit with inverter and charger functions:
        # - com.victronenergy.inverter.can_xantrex: Inverter data (e.g., /Ac/Out/L1/Power)
        # - com.victronenergy.charger.can_xantrex: Charger data (e.g., /Dc/0/Current)
        # Both services share the same device instance (252) to indicate a single physical device.
        self.inverter_bus = new_system_bus()
        self.charger_bus  = new_system_bus()
        
        self._InverterService = VeDbusServiceWithMeta('com.victronenergy.inverter.can_xantrex', self.inverter_bus, '/com/victronenergy/inverter/can_xantrex' )        
        self._ChargerService  = VeDbusServiceWithMeta('com.victronenergy.charger.can_xantrex',  self.charger_bus , '/com/victronenergy/charger/can_xantrex'  )        

        
        self._InverterService.descriptor = 'INVERTER'
        self._ChargerService.descriptor  = 'CHARGER' 
        
        # Build one dispatch table, once at startup:
        #    dgn → (dgn_items, dbus_paths)
       
        self._combined_dgns: dict[int, list[tuple[str, Any, str, str, dict[str, Any], Any]]] = {}
        

        for dgn, items in INVERTER_DGN_MAP.items():
            flat = self._combined_dgns.setdefault(dgn, [])
            for item in items:
                if len(item) == 4: 
                    path, decoder, unit, description = item
                else:               
                    path, decoder = item; unit, description = '', ''
                flat.append((path, decoder, unit, description, self._InverterService.exported_paths, self._InverterService))

        for dgn, items in CHARGER_DGN_MAP.items():
            flat = self._combined_dgns.setdefault(dgn, [])
            for item in items:
                if len(item) == 4: 
                    path, decoder, unit, description = item
                else:
                    path, decoder = item; unit, description = '', ''
                flat.append((path, decoder, unit, description, self._ChargerService.exported_paths,  self._ChargerService))

        for dgn, items in COMMON_DGN_MAP.items():
            flat = self._combined_dgns.setdefault(dgn, [])
            for item in items:
                if len(item) == 4: 
                    path, decoder, unit, description = item
                else:               
                    path, decoder = item; unit, description = '', ''
                flat.append((path, decoder, unit, description, self._InverterService.exported_paths, self._InverterService))
                flat.append((path, decoder, unit, description, self._ChargerService .exported_paths,  self._ChargerService))


        # Precompute 5-digit uppercase hex strings for each DGN key
        self._dgn_name_hints = { dgn: f"{dgn:05X}" for dgn in self._combined_dgns }


        # Validate, Open, and bind CAN socket
        try:
            if not os.path.exists(f'/sys/class/net/{CAN_INTERFACE}'):
                logger.error(f"Interface {CAN_INTERFACE} not found in sysfs")
                sys.exit(1)

            self.socket = socket.socket(socket.AF_CAN, socket.SOCK_RAW, socket.CAN_RAW)
            
            # --------- enlarge receive queue ------------------------------------
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RCVBUF_BYTES)
            
            # feedback for the buffer size change
            effective = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
            logger.info("CAN receive buffer requested=%d, effective=%d (kernel reports doubled value)", RCVBUF_BYTES, effective)
            
            self.socket.setsockopt(socket.SOL_CAN_RAW, socket.CAN_RAW_LOOPBACK, 0)     # off → no echo generated at all
            self.socket.bind((CAN_INTERFACE,))
            logger.info(f"CAN socket bound to {CAN_INTERFACE}")
                
        except OSError as e:
            if e.errno == errno.ENODEV:
                logger.error(f"CAN interface {CAN_INTERFACE} not found (ENODEV)")
            elif e.errno == errno.EPERM:
                logger.error(f"Permission denied: try running as root (EPERM)")
            else:
                logger.error(f"Failed to bind CAN socket on {CAN_INTERFACE}: {e}")

            raise


        # Register all known D-Bus paths defined in the DGN maps (structure only, no data decoding).
        # This ensures all expected paths are visible in D-Bus tools (like dbus-spy) from startup,
        # with correct metadata (unit and description), even before any CAN data is received.
        
        # ── Register DGN-maps paths ─────────────────────────────────────
        for dgn, dgn_items in self._combined_dgns.items():
            for item in dgn_items:  # (path, decoder, unit, description, dbus_paths, service)         
                path, decoder, unit, description, dbus_paths, service = item

                # Register the D-Bus path with placeholder value and metadata (if available)
                self.register_path(service, path, None, writeable=False, unit=unit, description=description)



        # Register /Mgmt/xantrex-can paths on both services
        for svc in (self._InverterService, self._ChargerService):
            self.register_path(svc, '/FirmwareVersion',         FIRMWARE_VERSION,       writeable=False, unit='', description='Firmware version of this driver')
            self.register_path(svc, '/ProductId',               PRODUCT_ID,             writeable=False, unit='', description='Numeric product identifier')
            self.register_path(svc, '/DeviceInstance',          DEVICE_INSTANCE,        writeable=False, unit='', description='Device instance number')
            self.register_path(svc, '/Connected',               1,                      writeable=False, unit='', description='Service availability flag')
            self.register_path(svc, '/Status',                  'initializing',         writeable=False, unit='', description='Driver state')
            self.register_path(svc, '/Error',                   0,                      writeable=False, unit='', description='Error code')            
            self.register_path(svc, '/Mgmt/ProcessAlive',       1,                      writeable=False, unit='', description='Standard heartbeat counter')
            self.register_path(svc, '/Mgmt/LastUpdate',         '',                     writeable=False, unit='', description='Global heartbeat timestamp')
            self.register_path(svc, '/Mgmt/ManufacturerCode',   119,                    writeable=False, unit='', description='Manufacturer code')            
            self.register_path(svc, '/Mgmt/ProcessName',        'xantrex_rvc',          writeable=False, unit='', description='Process name')     
            self.register_path(svc, '/Mgmt/ProcessVersion',     SCRIPT_VERSION,         writeable=False, unit='', description='')                 
            self.register_path(svc, '/Mgmt/ProcessInstance',    0,                      writeable=False, unit='', description='')                             
            self.register_path(svc, '/Mgmt/Connection',         f"CAN@{CAN_INTERFACE}", writeable=False, unit='', description='CAN interface binding')                                         
            self.register_path(svc, '/Mgmt/Type',               'inverter' if svc is self._InverterService else 'charger', writeable=False, unit='', description='Device type')            



        # Name is currently hard code mine.  
        # Here is a list of xantrex series that support rv-c, all are inverter/charger units
        # Freedom SW-RVC
        # Freedom SW 
        # Freedom XC Pro
        # Freedom XC 
        # Register additional derived D-Bus paths
        # These are not mapped directly from DGNs but are calculated at runtime
        # Example: /Dc/0/Power = /Dc/0/Voltage × /Dc/0/Current
        # includes meta data units and description
        # ────────────────────────────────────────────────────────────────
        # Register derived D-Bus paths on inverter service (per line)
        # ────────────────────────────────────────────────────────────────
        self.register_path(self._InverterService, '/Dc/0/Power',           None, writeable = False, unit = 'W', description = 'DC Power Bank 0')
        self.register_path(self._InverterService, '/Ac/In/L1/Power',       None, writeable = False, unit = 'W', description = 'AC Input L1 Power')
        self.register_path(self._InverterService, '/Ac/ActiveIn/L1/P',     None, writeable = False, unit = 'W', description = 'AC Active Input L1 Power')
        self.register_path(self._InverterService, '/Ac/Out/L1/P',          None, writeable = False, unit = 'W', description = 'AC Output L1 Power')
        self.register_path(self._InverterService, '/Ac/In/P',              None, writeable = False, unit = 'W', description = 'Total AC Input Power')
        self.register_path(self._InverterService, '/Ac/In/I',              None, writeable = False, unit = 'A', description = 'Total AC Input Current')
        self.register_path(self._InverterService, '/Ac/Out/P',             None, writeable = False, unit = 'W', description = 'Total AC Output Power')
        self.register_path(self._InverterService, '/Ac/Out/I',             None, writeable = False, unit = 'A', description = 'Total AC Output Current')        
        self.register_path(self._InverterService, '/System/Ac/P',          None, writeable = False, unit = 'W', description = 'System AC Power')
        self.register_path(self._InverterService, '/System/Ac/I',          None, writeable = False, unit='A',   description = 'System AC Current')

        self.register_path(self._InverterService, '/CustomName',     'XC Pro 3000 (Inverter)',            writeable=False, unit='',  description='Display device name in GUI.')
        self.register_path(self._ChargerService,  '/CustomName',     'XC Pro 3000 (Charger)',             writeable=False, unit='',  description='Display device name in GUI.')
      
        # /ProductName is listed as a mandatory path for product services in the D-Bus API (along with /ProductId, /FirmwareVersion, /DeviceInstance, /Connected).
        # This is what shows in the device list.
        self.register_path(self._InverterService, '/ProductName',    'Freedom XC Pro (Inverter)',         writeable=False, unit='',  description='Display device name in GUI.')
        self.register_path(self._ChargerService,  '/ProductName',    'Freedom XC Pro (Charger)',          writeable=False, unit='',  description='Display device name in GUI.')

        self.register_path(self._InverterService, '/Ac/Out/Total/P', 0.0,                                 writeable=False, unit='W', description='Total Active Power')
        self.register_path(self._InverterService, '/Ac/Out/Total/I', 0.0,                                 writeable=False, unit='A', description='Total Current')

        mode_item = self.register_path(self._InverterService, '/Mode',                 4, writeable = False,    unit = '',  description = 'Inverter Mode - Venus OS Switch?')   # “On” (normal/auto, charger + inverter available)
        # Give it a text mapper so GUI shows "On"/"Off" instead of 3/4
        mode_item._gettextcallback = (lambda _path, value: {1:"Charger only", 2:"Inverter only", 3:"Auto/On", 4:"Off"}.get(int(value), str(value)) )

        # Add socket listener to GLib event loop
        self.watch_id = GLib.io_add_watch(self.socket.fileno(), GLib.IO_IN, self.handle_can_frame)

        # Unit may be off now; but let's try
        self.send_pgn_request(0x1EE00,  0xFF)   # Address Claimed
        self.send_pgn_request(0x1FEEB,  0xFF)   # Product Identification
        self.send_pgn_request(0x1FFDE,  0xFF)   # Inverter Model Info

        # the unit maybe off or the primary address may not be the default and these never get sent.  For now I am ok with that.    
        # This block sends request to the Xantrex for various info that the Venus OS GUI may display
        #  Inverter-side operational data
        self.send_pgn_request(0x1FFD7)   # Inverter AC Status 1 - /Ac/Out/L1/V I F, /State, output fault bits
        self.send_pgn_request(0x1FFD4)   # Inverter Status (Mode, pass-through, load-sense)
        self.send_pgn_request(0x1FFD5)   # Inverter Status 2 (overload / temp / warning flags)


        #  Charger-side operational data  (decoded now, split service later)
        self.send_pgn_request(0x1FFCA)   # Charger AC Status 1 → AC-in / Active-in L1 V & I
        self.send_pgn_request(0x1FFC7)   # Charger Status Flags → charger state, enable flag, inverter-active bit

        #  Identity / product information    - these do not seem to be sent back
        self.send_pgn_request(0x1FFDE)   # Inverter Model Info     - /Info/Model, /Info/Serial, /Info/HardwareVersion
        self.send_pgn_request(0x1FEEF)   # Software Identification - might be able to drop if no reply
        self.send_pgn_request(0x0EBFF)   # firmware is in here and more.

        # these do not seem to be sent back    
        #self.send_pgn_request(0x1FFD6)   # Inverter AC Status 2 (aggregate RMS V & F)
        #self.send_pgn_request(0x1FFDD)   # Inverter Power Summary (P, Q, S, PF)
        #self.send_pgn_request(0x1FFCC)   # Inverter DC-bus Status (Voltage & Current)
        #self.send_pgn_request(0x1FEEB)   # Product Identification  - FirmwareVersion, ProductId, ManufacturerCode, DeviceInstance

        if self.verbose:
            logger.info("Service initialization complete")
            for path in sorted(self._InverterService.exported_paths):
                logger.info(f"[REGISTERED][{self._InverterService.descriptor}] {path}")
            for path in sorted(self._ChargerService.exported_paths):
                logger.info(f"[REGISTERED][{self._ChargerService.descriptor}]  {path}")
                
        self._InverterService['/Status'] = 'ok'
        self._ChargerService['/Status']  = 'ok'
        

    def register_path(self, service, path, value=None, writeable=False, unit=None, description=None):
        
        # Registers a D-Bus path on the given service with optional unit and description metadata.
        # Compatible with vedbus.py on Venus OS 2.7+.
        

        # Only register once
        if path not in service.exported_paths:
            # Legacy call: name, value, writeable flag only - unit/description is not supported directly pre 3.5? somewhere in there.
            item = service.add_path(path, value, writeable=writeable)

            # Manually inject metadata for pre-3.6 compatibility
            if unit is not None:
                item.unit = unit
            if description is not None:
                item.description = description
        else:
            item = service.exported_paths[path]
            
            if value is not None:
                service[path] = value       # → pushes to D-Bus  and <— ensure defaults overwrite earlier None

                logger.info(f"[register_path] '{path}' on service {service.descriptor}, updated.  value: {value!r} ")
                
            else:    
                logger.info(f"[register_path] '{path}' already exists on {service.descriptor}, skipping creation.    value: {value!r}")
                
        return item
    
    # send a request to the unit
    def send_pgn_request(self, pgn: int, global_request: bool = False) -> bool:
        try:
            da         = 0xFF if global_request else (self.primary_source & 0xFF)  # J1939 Request is PDU1 (PF=0xEA): PS is the Destination Address (DA).
                                                                   # Use 0xFF for GLOBAL; otherwise unicast to the target at primary_source.

            sa         = (self.primary_source & 0xFF)              # Source Address (SA) 
                                                                   # Here we send from primary_source as well.
 
            can_id     = 0x18EA0000 | (da << 8) | sa               # 29-bit CAN ID: Priority=6 + PF=0xEA (Request) + PS=DA + SA.
                                                                   # GLOBAL example: DA=0xFF, SA=0x42 → 0x18EAFF42; Unicast example: DA=0x42, SA=0x42 → 0x18EA4242.
            
            pgn_bytes  = struct.pack('<I', pgn)[0:3]               # Little-endian PGN, 3 bytes
            data       = pgn_bytes + b'\xFF' * 5                   # Pad to 8 bytes
            frame      = struct.pack("=IB3x", can_id, 8) + data    # Full CAN frame

            # it has errorred, let's just retry 5 times before giving up.  not a big deal 
            for attempt in range(6):
                try:
                    self.socket.send(frame)
            
                    logger.info(f"Sent PGN request for 0x{pgn:05X}")
                    return True
                except OSError as e:
                    if attempt < 6:
                        time.sleep(0.05 )
                        continue
                        
                    return False
        except Exception as e:
            logger.error(f"Failed to send PGN request 0x{pgn:05X}: {e}")


 
    # Format CAN frame for logging
    def format_can_frame(self, dgn, dlc, data):
        hexdata = ' '.join(f'{b:02X}' for b in data)
        return f"DGN=0x{dgn:05X} | DLC={dlc} | Data={hexdata}"

    
    # Calculate power values from voltage and current paths
    def update_derived_values(self):
        
        # Triggered whenever we receive a PGN listed in DERIVED_DGNS.
        # • Recomputes V×I power for individual measurement points
        # • Sums AC totals (input / output) and mirrors them to aliases
        # compute and publish P = V × I for inverter service
        def compute_power(dst_path: str, v_path: str, c_path: str) -> None:
            try:
                # Fetch the source D-Bus items
                v_item = self._InverterService.exported_paths.get(v_path)
                c_item = self._InverterService.exported_paths.get(c_path)

                # Skip if missing/placeholder ([], None). Zeros are OK.
                if (v_item is None or c_item is None or v_item.value in (None, [])) or (c_item.value in (None, [])):
                    return

                # Calculate and publish power
                p = round(v_item.value * c_item.value, 1)
                self._InverterService[dst_path] = p

                logger.info(f"[{self.frame_count:06}] [DERIVED - COMPUTE POWER] PGN=0x{self.last_dgn:05X} SRC=0x{self.last_src:02X} DERIVED {dst_path}={p:.1f} W (V={v_item.value} V × I={c_item.value} A)")

            except Exception as e:      # Full traceback only when debug flag set
                if self.debug:
                    logger.exception(f"[{self.frame_count:06}] [DERIVED FAIL ] PGN=0x{getattr(self,'last_dgn',0):05X} SA=0x{getattr(self,'last_src',0):02X} {dst_path} – {e}")


        #  AC totals for single-phase (L1) group
        def compute_totals(base_path: str, aliases: list[str] | None = None) -> None:
            try:
                v_item = self._InverterService[f"{base_path}/L1/V"]
                c_item = self._InverterService[f"{base_path}/L1/I"]

                if v_item is None or c_item is None or v_item == 0.0 or c_item == 0.0:
                    return

                # Aggregate; Freedom XC has only one phase
                p_total = round(v_item * c_item, 1)
                i_total = round(c_item, 1)

                # Write to canonical total paths
                self._InverterService[f"{base_path}/P"] = p_total
                self._InverterService[f"{base_path}/I"] = i_total

                # Mirror onto any alias prefixes (e.g. /Ac/Grid, /System/Ac)
                if aliases:
                    for alias in aliases:
                        self._InverterService[f"{alias}/P"] = p_total
                        self._InverterService[f"{alias}/I"] = i_total

                logger.info(f"[{self.frame_count:06}] [DERIVED - TOTALS] PGN=0x{getattr(self,'last_dgn',0):05X} SA=0x{getattr(self,'last_src',0):02X} DERIVED {base_path} P={p_total:.1f} W I={i_total:.1f} A (L1)")
                

            except Exception as e:
                if self.debug:
                    logger.exception(f"[{self.frame_count:06}] [DERIVED TOTALS FAIL ]PGN=0x{getattr(self,'last_dgn',0):05X} SA=0x{getattr(self,'last_src',0):02X}{base_path} – {e}")


        # Individual power paths (DC & AC) – single-phase Freedom XC
        compute_power('/Dc/0/Power',       '/Dc/0/Voltage',     '/Dc/0/Current')

        compute_power('/Ac/In/L1/P',       '/Ac/In/L1/V',       '/Ac/In/L1/I')
        compute_power('/Ac/ActiveIn/L1/P', '/Ac/ActiveIn/L1/V', '/Ac/ActiveIn/L1/I')
        #compute_power('/Ac/Out/L1/P',      '/Ac/Out/L1/V',      '/Ac/Out/L1/I')

        # Totals + aliases  (/Ac/In → /Ac/Grid ,  /Ac/Out → /System/Ac)
        compute_totals('/Ac/In',   aliases=['/Ac/Grid'])
        compute_totals('/Ac/Out',  aliases=['/Ac/Out/Total'])        
        #compute_totals('/Ac/Out', aliases=['/System/Ac'])



    def process_multiFrames(self, dgn: int, src: int, data: bytes) -> bool:
        #  - ECFF (TP.CM/BAM announce) → start assembler
        #  - EBFF (TP.DT data)         → append, finish, classify
        # On completion: if payload text lacks 'XANTREX', add SA to SA_toSkip.
        # Returns True when the frame was handled here.
        # Fast drop: already known non-Xantrex TP traffic
        
        if dgn in (0x0ECFF, 0x0EBFF) and src in self.SA_toSkip:
            logger.info(f"[MULTI FRAME PROCESSOR] SA=0x{src:02X} DGN=0x{dgn:05X} Skip=1")
            return True

        # ----- ECFF (TP.CM/BAM) -----
        if dgn == 0x0ECFF:
            # Only BAM (0x20); ignore RTS/CTS/ABORT in this lean path
            if len(data) < 8 or data[0] != 0x20:
                logger.info(f"[MULTI FRAME PROCESSOR] SA=0x{src:02X} DGN=0x{dgn:05X} Skip=2")
                return True

            # Start (or reset) assembler for this SA
            self.multiframe_assemblies[src] = {
                "len": int.from_bytes(data[1:3], "little"),  # total bytes announced
                "need": data[3],                              # number of TP.DT packets to expect
                "seq": 1,                                     # next expected TP.DT sequence number
                "pgn": int.from_bytes(data[5:8], "little"),   # target PGN being transported
                "buf": bytearray(),                           # accumulator (7 bytes per DT)
                "deadline": time.monotonic() + 2.0,           # simple timeout (seconds)
            }
            st = self.multiframe_assemblies[src]
            logger.info(f"[MULTI FRAME PROCESSOR] SA=0x{src:02X} PGN=0x{st['pgn']:06X} LEN={st['len']} PKTS={st['need']}" )
            return True

        # ----- EBFF (TP.DT) -----
        if dgn == 0x0EBFF:
            if src not in self.multiframe_assemblies:
                logger.info(f"[MULTI FRAME PROCESSOR] SA=0x{src:02X} DGN=0x{dgn:05X} Skip=3")
                return True
            st = self.multiframe_assemblies[src]

            # Timeout cleanup (lost DTs / stalled transfer)
            if time.monotonic() > st["deadline"]:
                logger.warning(f"[{self.frame_count:06}] [MULTI FRAME PROCESSOR TIMEOUT] SA=0x{src:02X} | DGN=0x{dgn:05X} | NEED={st['need']} | ACTION=Assembler dropped")
                del self.multiframe_assemblies[src]
                return True

            # Require seq + at least 1 data byte
            if len(data) < 2:
                logger.info(f"[{self.frame_count:06}] [MULTI FRAME PROCESSOR DROP] SA=0x{src:02X} | DGN=0x{dgn:05X} | REASON=short-dt | LEN={len(data)} | DATA=[{data.hex(' ').upper()}]")
                del self.multiframe_assemblies[src]
                return True

            # Enforce in-order DT sequence
            if data[0] != st["seq"]:
                logger.warning(f"[{self.frame_count:06}] [MULTI FRAME PROCESSOR SEQ] SA=0x{src:02X} | DGN=0x{dgn:05X} | EXP={st['seq']} | GOT={data[0]} | ACTION=Assembler dropped")
                del self.multiframe_assemblies[src]
                return True

            # Append 7 data bytes and advance counters/deadline
            st["buf"] += data[1:8]
            st["seq"] += 1
            st["need"] -= 1
            st["deadline"] = time.monotonic() + 2.0

            # Finished this BAM?
            if st["need"] == 0:
                payload = bytes(st["buf"])[: st["len"]]  # trim to announced len
                pgn     = st["pgn"]
                del self.multiframe_assemblies[src]


                txt_raw = payload.decode("ascii", "ignore").strip("\x00 ").strip()
                txt_up  = txt_raw.upper() 

                # sanitize non-printables for log visibility 
                assembled_txt = ''.join(ch if 32 <= ord(ch) < 127 else '.' for ch in txt_raw)                
                
                if "XANTREX" in txt_up:
                    if src not in self.xantrex_sources:
                        # mark SA as Xantrex 
                        self.xantrex_sources[src] = 129   #  inverter / charger
                        
                    #  we hard code the version to the latest.  If and when the dgn comes in, set the path correctly    
                    FIRMWARE_VERSION = re.search(r'U3:0*([0-9]{1,2}\.[0-9]{2})', assembled_txt)
                    if FIRMWARE_VERSION != None:
                        self._InverterService['/FirmwareVersion'] = FIRMWARE_VERSION   
                        self._ChargerService['/FirmwareVersion']  = FIRMWARE_VERSION   
                    
                    logger.info(f"[{self.frame_count:06}] [MULTI FRAME PROCESSOR DONE] SA=0x{src:02X} | DGN=0x{dgn:05X} | PGN=0x{pgn:06X} | BYTES={len(payload)} | ASSEMBLED={assembled_txt} | NAME=XANTREX")
                else:
                    # classify as not Xantrex
                    self.SA_toSkip.add(src)
                    logger.info(f"[{self.frame_count:06}] [MULTI FRAME PROCESSOR DONE] SA=0x{src:02X} | DGN=0x{dgn:05X} | PGN=0x{pgn:06X} | BYTES={len(payload)} | ASSEMBLED={assembled_txt} | NAME=OTHER → SA_toSkip")


            return True

        # Not a Multi Frame PGN
        return False


    def state_is_assisting(self):
        grid_current    = self._InverterService['/Ac/Grid/L1/I']
        battery_current = self._InverterService['/Dc/0/Current']

        # only emit “Assisting” when the grid is present AND battery is discharging (< 0 A)
        if grid_current and battery_current is not None and  battery_current < 0:
            self._InverterService['/State'] = 10   # venus os assisting value

    def state_is_passthru(self):
        passthrough_is = self._ChargerService['/Ac/PassThrough/Active']
        if passthrough_is is not None:
            if bool(passthrough_is.value):
                self._ChargerService['/State'] = 8
            return
         

    # Process a single incoming CAN frame, decode its RV-C data, and update D-Bus paths.
    # Args:
    #    source: File descriptor of the CAN socket.
    #    condition: GLib IO condition (e.g., GLib.IO_IN).
    # Returns:
    #    bool: True to continue processing, False to stop.

    def handle_can_frame(self, source, condition):
        self.frame_count += 1
        
        processed    = 0
        unchanged    = 0
        skipped_none = 0
        errors       = 0
        
        
        
        # === Extract and Decode CAN ID and Data ===
        try:
            # Receive and Parse a J1939/RV-C CAN Frame ===
            # Receive one CAN frame, decode it via RV-C PGN maps, and publish to D-Bus.
            frame = self.socket.recv(16)
            
            # Validate minimum CAN header (8 bytes)
            if len(frame) < 8:
                raise ValueError(f"Received too short CAN frame header: {len(frame)} bytes")
                
            # Extract CAN ID (29-bit extended ID) and DLC (Data Length Code)
            # Format: =IB3x → 4 bytes CAN ID (uint32), 1 byte DLC, skip 3 padding bytes
            can_id, can_dlc = struct.unpack("=IB3x", frame[0:8])
            
            # Use available data, even if less than DLC  
            available_dlc = min(can_dlc, len(frame) - 8)
            if available_dlc == 0:
                self.error_count += 1
                logger.warning(f"[{self.frame_count:06}] [NO DATA] Frame=0x{can_id:08X} | DGN=0x{(can_id & 0x1FFFF) >> 8:05X} | DLC={can_dlc} | No data bytes available")
                return True            
                
            # Slice out the actual CAN data payload (up to 8 bytes)
            data = memoryview(frame[8:8 + available_dlc])

            
            # === Decode CAN ID into J1939 / RV-C fields ===
            # According to J1939 (and RV-C which is built on top of it), a 29-bit CAN ID has:
            #
            #   | Priority (3) | Reserved (1) | Data Page (1) | PDU Format (8) | PDU Specific (8) | Source Address (8) |
            #     <-----------  bits 26–28  -> <-------- bits 24–25, 16–23, 8–15 ----------> <----- bits 0–7 --------->
            #
            # PGN (Parameter Group Number) spans bits 8–25 = 18 bits
            # SRC (SouRCe Address) = bits 0–7
            # In RV-C, DGN == PGN (DGN = Diagnostic Group Number)
            pgn = (can_id >> 8) & 0x3FFFF   # Extract PGN from bits 8–25 (18 bits)
            src = can_id & 0xFF             # Extract Source Address from bits 0–7
            dgn = pgn                       # In RV-C, the DGN is just the PGN
            


            # Find Xantrex sources from NAME (EE00) - Claims
            if (dgn == 0x1EE00) or (dgn == 0x00EE00):
                if len(data) >= 8:
                    
                    # Manufacturer = ((b2>>5) | (b3<<3)) & 0x7FF   ; Function = byte 5
                    mfg = ((data[2] >> 5) | (data[3] << 3)) & 0x7FF
                    if mfg == 119:  # Xantrex
                        func = data[5]
                        
                        if src not in self.xantrex_sources:
                            self.xantrex_sources[src] = func
                            logger.debug(f"[{self.frame_count:06}] [CAN ID] 0x{can_id:08X} → PGN=0x{pgn:05X} DGN=0x{dgn:05X} SRC=0x{src:02X} DLC={can_dlc} DATA=[{data.hex(' ').upper()}]")
                            logger.info(f"[{self.frame_count:06}] [XANTREX SOURCE FOUND] SA=0x{src:02X} function=0x{func:02X}" )
                return True
            else:
                # if sources are not yet set, send the request 
                # if we are here, income data, if this is not set, most likely the first time.
                if not self.xantrex_sources:   # hard coding the sources for now...
                    self.send_pgn_request(0x00EE00, 0xFF)   # Address Claimed
                    #self.send_pgn_request(0x1FEEB,  0xFF)   # Product Identification
                    #self.send_pgn_request(0x1FFDE,  0xFF)   # Inverter Model Info
                    
                    #logger.info(f"[{self.frame_count:06}] [XANTREX SOURCE REQUEST] 0x{can_id:08X} → PGN=0x{pgn:05X} DGN=0x{dgn:05X} SRC=0x{src:02X} DLC={can_dlc} DATA=[{data.hex(' ').upper()}]")
                    
                    #continue #processing the frame.            
                else:
                    if self.xantrex_sources and (src not in self.xantrex_sources):
                        logger.info(f"[{self.frame_count:06}] [CAN ID - SOURCE SKIPPED] 0x{can_id:08X} → PGN=0x{pgn:05X} DGN=0x{dgn:05X} SRC=0x{src:02X} DLC={can_dlc} DATA=[{data.hex(' ').upper()}]")
                        return True
                        
            logger.debug(f"[{self.frame_count:06}] [CAN ID] 0x{can_id:08X} → PGN=0x{pgn:05X} DGN=0x{dgn:05X} SRC=0x{src:02X} DLC={can_dlc} DATA=[{data.hex(' ').upper()}]")
            if dgn in (0x0ECFF, 0x0EBFF):
                if self.process_multiFrames(dgn, src, data):
                    return True # consumed
           
            # a charger value but if the volts is 0 throw the complete frame away    
            if dgn == 0x1FFCA:   
                v = safe_u16(data, 1, 0.05)   # AC In L1 Voltage
                if v is None  or v <= 90:  #  when is is not charging the voltage is 0 but let's expand the capture
                    logger.info(f"[{self.frame_count:06}] [CAN ID - FFCA SKIPPED] 0x{can_id:08X} → PGN=0x{pgn:05X} DGN=0x{dgn:05X} SRC=0x{src:02X} DLC={can_dlc} DATA=[{data.hex(' ').upper()}]")                    
                    return True # consumed


            
        except (OSError, ValueError) as e:
            self.error_count += 1
            if self.debug:
                logger.error(f"[RECV ERROR] Failed to read from CAN socket: {e}")
            return True
            
        # Look up this DGN in our pre-built map.
        dgn_items = self._combined_dgns.get(dgn)
        
        #  the frame id/DGN was not found
        if dgn_items is None:            # -------------------- Unknown DGN --------------------
            # If DGN is not recognized, log once and ignore            
            
            self.unmapped_counts[dgn] = self.unmapped_counts.get(dgn, 0) + 1

            if dgn not in self.unmapped_seen:
                hex_data = ' '.join(f"{b:02X}" for b in data)
                try:
                    ascii_data = data.decode('ascii')
                    if not all(32 <= ord(c) <= 126 for c in ascii_data):
                        ascii_data = "<non-printable>"
                except Exception:
                    ascii_data = "<non-ascii>"
                    
                logger.info(f"[{self.frame_count:06}] [UNMAPPED] Frame=0x{can_id:08X} | DGN=0x{dgn:05X} | DLC={can_dlc} | Data=[{hex_data}] | ASCII='{ascii_data}'")
                self.unmapped_seen.add(dgn)
            
                # Keep the unmapped set within max size
                if len(self.unmapped_seen) > MAX_UNMAPPED_DGNS:
                    removed = self.unmapped_seen.pop()
                    logger.debug(f"[{self.frame_count:06}] [UNMAPPED SET] Removed DGN 0x{removed:05X} to cap size")
                
            return True  
            
        name_hint           = self._dgn_name_hints[dgn]
        services_touched    = set()

        # --- Decode all D-Bus values associated with this DGN and push to D-Bus  ---
        # item = (path, decoder, unit, description, dbus_paths, service)
        for item in dgn_items:
            try:
                # Check for unexpected tuple lengths before unpacking
                if len(item) != 6:
                    raise ValueError(f"[{self.frame_count:06}] [DGN ERROR] Unexpected tuple size for {item}")
                    
                path, decoder, unit, description, dbus_paths, service = item

                # Safely decode data using the provided decoder function.
                # If decoding fails, log the error and increment error counter.
                try:
                    value = decoder(data)
                    
                except Exception as e:
                    self.error_count += 1
                    errors           += 1 
                    logger.error(f"[{self.frame_count:06}] [DECODE ERROR] {path}: {e} | decoder={getattr(decoder, '__name__', repr(decoder))} | data={data.hex(' ').upper()}")
                    continue
                    
                # If decoding failed (returned None), skip this path
                if value is None:
                    skipped_none += 1    
                    continue

                # The DGN is known, but the specific path might not exist on this *service* 
                # Instead of raising KeyError (which would drop *all* remaining signals in the same frame) we log a warning and move on so the
                # other signals can still be processed.
                if path not in dbus_paths:
                    logger.error(f"[{self.frame_count:06}] [MISSING PATH][{service.descriptor}] DGN=0x{dgn:05X} | path={path} | data={data.hex().upper()}")
                    continue
                    
                # special odd handling, I have not come up with a cleaner way to deal with.  
                        
               
                # Only accept inverter status (FFD4) from D0 → skip everything else  Do not like to hard code this...
                # FFD4 seems to be correct for inverter only when it comes from source D0 I hate to hard code this, but xantrex does not respond when I ask 
                if dgn == 0x1FFD4:
                    # Only allow inverter status from D0 → inverter service
                    if src == 0xD0 and service is self._InverterService:      # this gives us the correct /State value   Only for the inverter
                        pass  # allowed    we may want this to send to charger service if 42 or another source, but for now we do this.
                        
                    else:    
                        continue  # skip everything else for FFD4

                    
                if dgn == 0x1FFD7:
                    # Only allow frequency paths from D0
                    if src == 0xD0 and path in ('/Ac/Out/L1/F', '/Ac/Out/F'):   # frequencey from 42 is not correct, but it is good from d0 so
                        pass  # allowed
                    elif src == self.primary_source and path not in ('/Ac/Out/L1/F', '/Ac/Out/F'):   
                        pass  # allowed
                    else:
                        continue  # skip
                
                    
                try:
                    service[path] = value       # → pushes to D-Bus
                        
                    # DGN is known and matched; value was decoded and now SENT                        
                    logger.info(f"[{self.frame_count:06}] [SENT][{service.descriptor}] DGN=0x{dgn:05X} | path={path} | value={value} {unit} | desc=\"{description}\" | raw={data.hex(' ').upper()}")
                        
                    #if dgn in (0x1FFCB, 0x1FFDD, 0x1FFD6, 0x1FFD7, 0x1FFDC):
                    #    logger.info(f"[GUIDMODS DISPLAY] DGN=0x{dgn:05X} | path={path} | value={value} {unit} | desc=\"{description}\"")

                    processed += 1
                    services_touched.add(service)
                        
                except Exception as send_error:
                    logger.error(f"[{self.frame_count:06}] [DBUS SEND ERROR][{service.descriptor}] DGN=0x{dgn:05X} | path={path} | value={value} {unit} | desc=\"{description}\" | raw={data.hex(' ').upper()} | {send_error}")
                    continue
                        

            except Exception as e:
                self.error_count += 1
                if self.debug:
                    logger.error( f"[{self.frame_count:06}] [DECODE ERROR 2] {path}: {e} | {self.format_can_frame(dgn, can_dlc, data)}" )

        timestamp = time.time()  
        for svc in services_touched:
            svc['/Mgmt/LastUpdate']        = timestamp
            logger.info(f"[{self.frame_count:06}] [FRAME SUMMARY][{svc.descriptor.upper()}] [DGN 0x{dgn:05X}] → {processed} sent, {unchanged} unchanged, {skipped_none} null values, {errors} errors")
 
        # After decoding known paths, calculate and send derived values
        if dgn in DERIVED_DGNS:
            # cache frame context for logging
            self.last_dgn   = pgn 
            self.last_src   = src
            self.last_canid = can_id

            self.update_derived_values()  

        # increment a different frame counter to trigger and run state checks every 60 frames (~5s)
        self._frame_counter += 1
        if self._frame_counter >= 65:   # about 5s on my pi  Use a counter just to be easier 
            try:
                logger.info(f"[{self.frame_count:06}] [FRAME STATE CHECK] Running /State derivations")
                self.state_is_passthru()
                self.state_is_assisting()
            except Exception as e:
                logger.error(f"[{self.frame_count:06}] [FRAME STATE ERROR] Running /State derivations Error: %s", e)
            # reset counter
            self._frame_counter = 0

        return True


    def start_heartbeat(self, interval=5):
        def sync_mode_from_status(self):
            inverter_service = self._InverterService
            MODE_ON, MODE_OFF = 3, 4
            
            try:
                raw_status  = inverter_service['/Status']   # int when on; None when not on
                is_off      = (raw_status is None) or (raw_status == 0)
                target_mode = MODE_OFF if is_off else MODE_ON
                
                if inverter_service['/Mode'] != target_mode:
                    inverter_service['/Mode'] = target_mode
                return True
                
            except Exception as error:
                try:
                    if inverter_service['/Mode'] != MODE_OFF:
                        inverter_service['/Mode'] = MODE_OFF
                except Exception:
                    pass
                    logger.warning("[SET MODE ERROR] sync_mode_from_status: forcing Off (%s)", error)
                    
                return False
        
        def _publish_heartbeat():
            """
            Inner function that runs in the background thread and updates
            the last_heartbeat timestamp and the ProcessAlive D-Bus path.
            """
            try:
                # Record the current timestamp for heartbeat tracking
                self.last_heartbeat     = time.time()
                self.heartbeat_counter += 1


                # Update the D-Bus /ProcessAlive path to indicate activity
                # Update both a counter and a timestamp
                # Inverter heartbeat updates
                self._InverterService['/Mgmt/ProcessAlive']      = self.heartbeat_counter
                self._InverterService['/Mgmt/LastUpdate']        = self.last_heartbeat

                # Charger heartbeat updates
                self._ChargerService['/Mgmt/ProcessAlive']       = self.heartbeat_counter
                self._ChargerService['/Mgmt/LastUpdate']         = self.last_heartbeat

                # Optionally log the heartbeat timestamp if verbose/debug is enabled
                logger.info(f"[HEARTBEAT] {self.last_heartbeat}")
                
                # check /Status to set  /mode  rv-c from xantrex does not send it.
                sync_mode_from_status(self)

            except Exception as e:
                logger.error(f"[HEARTBEAT ERROR] {e}")                    
            return True
            
        self.timeout_heartbeat = GLib.timeout_add_seconds(interval, _publish_heartbeat)
        _publish_heartbeat()          # first beat immediately

        

    # Cleanup CAN socket
    def cleanup(self):
        if self.verbose:
            if self.unmapped_counts:
                logger.info("[UNMAPPED SUMMARY] DGNs seen but not mapped:")
                for dgn, count in sorted(self.unmapped_counts.items()):
                    logger.info(f"  - DGN 0x{dgn:05X}: {count} time(s)")

        # === Summary Block ===
        unmapped_total = sum(self.unmapped_counts.values())
        successful_decodes = self.frame_count - unmapped_total - self.error_count

        try:
            assert (unmapped_total + self.error_count + successful_decodes) == self.frame_count
        except AssertionError:
            logger.warning("⚠️ Frame count mismatch in summary!")

        logger.info("=== Shutdown Summary ===")
        logger.info(f"  Total frames received      : {self.frame_count}")
        logger.info(f"  Decoded successfully       : {successful_decodes}")
        logger.info(f"  Unmapped DGNs              : {unmapped_total}")
        logger.info(f"  Decode errors              : {self.error_count}")
        logger.info("==========================")

        # === Cleanup Resources ===
        # Define each cleanup step as (description, callable)
        steps = [
            ("remove watch_id",              lambda: ((GLib.source_remove(self.watch_id), setattr(self, 'watch_id', None))[1] if getattr(self, 'watch_id', None) else None)),
            ("quit main loop",               lambda: self.loop.quit() if getattr(self, 'loop', None) else None),
            ("close CAN socket",             lambda: self.socket.close() if getattr(self, 'socket', None) else None),
            ("close inverter bus",           lambda: getattr(self, 'inverter_bus', None) and self.inverter_bus.close()),
            ("close charger  bus",           lambda: getattr(self, 'charger_bus',  None) and self.charger_bus.close()),
            ("remove timeout_heartbeat",     lambda: GLib.source_remove(self.timeout_heartbeat) if getattr(self, 'timeout_heartbeat', None) else None),
        ]

        # Run each step and catch its errors individually
        for desc, action in steps:
            try:
                action()
                
            except Exception as e:
                logger.error(f"[CLEAN UP] Error during '{desc}': {e}")
            
            
# === Main Application Entry Point ===
def main():
    service = None  # Ensure reference exists even if initialization fails

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        if service and service.loop:
            service.loop.quit()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
      
        # Initialize the service with flags from CLI arguments
        service = XantrexService(debug = args.debug, verbose = args.verbose)

        # Set up the main GLib event loop for D-Bus
        service.loop = GLib.MainLoop()

        # startup log message if verbose is enabled
        logger.info("Starting main loop...")

        # Start the background thread that periodically updates ProcessAlive
        #service.start_heartbeat_thread()
        service.start_heartbeat(5)


        # Enter the main loop (non-blocking events will be handled here)
        service.loop.run()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if service:
            service._InverterService['/Status'] = 'offline'
            service._ChargerService['/Status']  = 'offline'
            service.cleanup()

        logger.info("Service stopped")


if __name__ == '__main__':
    main()