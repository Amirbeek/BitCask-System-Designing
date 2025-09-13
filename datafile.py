import os
import threading
from pathlib import Path

class DataFile:
    def __init__(self, writer_path: str | Path, reader_path: str | Path, file_id: int):
        self.id = file_id

        self.writer_path = Path(writer_path)
        self.reader_path = Path(reader_path)
        self.writer_path.parent.mkdir(parents=True, exist_ok=True)
        self.reader_path.parent.mkdir(parents=True, exist_ok=True)
        self.writer_path.touch(exist_ok=True)
        if self.reader_path != self.writer_path:
            self.reader_path.touch(exist_ok=True)

        self.writer = open(self.writer_path, "rb+")
        self.reader = open(self.reader_path, "rb")

        self.writer.seek(0, os.SEEK_END)

        self.offset = self.writer.tell()
        self._wlock = threading.Lock()
        self._rlock = threading.Lock()

    def append(self, data: bytes) -> int:
        with self._wlock:
            start = self.offset
            self.writer.seek(self.offset, os.SEEK_SET)
            self.writer.write(data)
            self.writer.flush()
            self.offset += len(data)
            return start

    def read_at(self, size: int, position: int) -> bytes:
        with self._rlock:
            self.reader.seek(position, os.SEEK_SET)
            return self.reader.read(size)

    def size(self) -> int:
        return self.offset

    def close(self):
        try:
            self.writer.close()
        finally:
            self.reader.close()
