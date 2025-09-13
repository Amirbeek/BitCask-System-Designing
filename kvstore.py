from pathlib import Path
from typing import Optional, Tuple
from datafile import DataFile
from header import Header
from helpers import (
    encode_record, decode_record, now_ms,
    HEADER_SIZE
)

class KVStore:
    def __init__(self, dir_path: str, seg_name: str = "000001.log"):
        seg_path = Path(dir_path) / seg_name
        self.df = DataFile(str(seg_path), str(seg_path), file_id=1)
        self.index: dict[str, Tuple[int, int, bool]] = {}
        self._replay()

    def put(self, key: str, value: bytes, expiry_ms: int = 0):
        k = key.encode("utf-8")
        rec, total = encode_record(k, value, ts=now_ms(), exp=expiry_ms)
        off = self.df.append(rec)
        self.index[key] = (off, total, False)

    def delete(self, key: str):
        meta = self.index.get(key)
        if meta is None:
            raise ValueError("Key not found")
        k = key.encode("utf-8")
        rec, total = encode_record(k, b"", ts=now_ms(), exp=0)  # tombstone
        off = self.df.append(rec)
        self.index[key] = (off, total, True)

    def get(self, key: str) -> Optional[bytes]:
        meta = self.index.get(key)
        if not meta:
            return None
        off, total, is_tomb = meta
        if is_tomb:
            return None
        buf = self.df.read_at(total, off)
        k_str, val, hdr = decode_record(buf)
        if hdr.Expiry and now_ms() > hdr.Expiry:
            return None
        return val

    def _replay(self):
        """Cold-start tiklash: faylni boshidan oxirigacha skanerlash."""
        pos = 0
        sz = self.df.size()
        while pos + HEADER_SIZE <= sz:
            hdr_buf = self.df.read_at(HEADER_SIZE, pos)
            try:
                hdr = Header.decode(hdr_buf)
            except Exception:
                break  # header buzilgan bo'lsa, shu yerdan to'xtaymiz
            total = HEADER_SIZE + hdr.KeySize + hdr.ValSize
            if pos + total > sz:
                break  # qisman yozilgan rekord
            body = self.df.read_at(hdr.KeySize + hdr.ValSize, pos + HEADER_SIZE)
            try:
                key_str, value, _ = decode_record(hdr_buf + body)
            except Exception:
                break
            is_tomb = (hdr.ValSize == 0)
            self.index[key_str] = (pos, total, is_tomb)
            pos += total

    def close(self):
        self.df.close()
