# header.py
import struct

# CRC32(4B), Timestamp(8B), Expiry(8B), KeySize(4B), ValSize(4B)
HEADER_FMT = "<IQQII"

class Header:
    def __init__(self, checksum=0, timestamp=0, expiry=0, key_size=0, val_size=0):
        self.Checksum = int(checksum)
        self.Timestamp = int(timestamp)
        self.Expiry   = int(expiry)
        self.KeySize  = int(key_size)
        self.ValSize  = int(val_size)

    def encode(self) -> bytes:
        return struct.pack(
            HEADER_FMT,
            self.Checksum,
            self.Timestamp,
            self.Expiry,
            self.KeySize,
            self.ValSize
        )

    @classmethod
    def decode(cls, buf: bytes):
        if len(buf) != struct.calcsize(HEADER_FMT):
            raise ValueError("Header size mismatch")
        checksum, ts, exp, ksz, vsz = struct.unpack(HEADER_FMT, buf)
        return cls(checksum, ts, exp, ksz, vsz)
