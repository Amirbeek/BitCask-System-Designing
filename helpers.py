# helpers.py
import time
import struct
import zlib
from typing import Optional, Tuple
from header import Header, HEADER_FMT

PAYLOAD_FMT = "<QQII"  # ts(8B), exp(8B), key_len(4B), val_len(4B)
HEADER_SIZE = struct.calcsize(HEADER_FMT)

def now_ms() -> int:
    return int(time.time() * 1000)

def build_payload(ts: int, exp: int, key: bytes, value: bytes) -> bytes:
    return struct.pack(PAYLOAD_FMT, ts, exp, len(key), len(value)) + key + value

def compute_crc(payload: bytes) -> int:
    return zlib.crc32(payload) & 0xFFFFFFFF

def encode_record(key: bytes, value: bytes, ts: Optional[int] = None, exp: int = 0) -> Tuple[bytes, int]:
    if ts is None:
        ts = now_ms()
    payload = build_payload(ts, exp, key, value)
    crc = compute_crc(payload)
    hdr = Header(crc, ts, exp, len(key), len(value)).encode()
    record = hdr + key + value
    return record, len(record)

def decode_record(buf: bytes) -> Tuple[str, bytes, Header]:
    """Bufferdan to'liq rekordni o'qish (header + key + value, CRC tekshiruv bilan)."""
    if len(buf) < HEADER_SIZE:
        raise ValueError("Buffer too small for header")
    hdr = Header.decode(buf[:HEADER_SIZE])
    body = buf[HEADER_SIZE:]
    if len(body) != hdr.KeySize + hdr.ValSize:
        raise ValueError("Body size mismatch")
    key = body[:hdr.KeySize]
    value = body[hdr.KeySize:]
    payload = build_payload(hdr.Timestamp, hdr.Expiry, key, value)
    if compute_crc(payload) != hdr.Checksum:
        raise ValueError("CRC mismatch (possibly partial/corrupted write)")
    return key.decode("utf-8"), value, hdr
