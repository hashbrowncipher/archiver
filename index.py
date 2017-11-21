from decompress import chacha20_decrypt
from os import pread
from tarfile import TarInfo
import tarfile
import tarfile as tf
from binascii import unhexlify
import sys
import lz4.block
import sqlite3
import boto3

class S3Reader(object):
    def __init__(self, metadata_from_block, metadata_from_pos, decrypt,
        s3_object):
        self._next_block = 0
        self._next_offset = 0
        self._pos = 0
        self._buffer = bytes()

        self._metadata_from_block = metadata_from_block
        self._metadata_from_pos = metadata_from_pos
        self._s3_object = s3_object
        self._decrypt = decrypt

    def _get_next_block(self):
        meta = self._metadata_from_block(self._next_block)
        if meta is None:
            return bytes()
        in_pos, in_size, out_pos, out_size = meta
        assert self._pos - in_pos == self._next_offset

        end = out_pos + out_size - 1
        resp = self._s3_object.get(Range="bytes={}-{}".format(out_pos, end))
        encrypted = resp["Body"].read()
        compressed = self._decrypt(self._next_block, encrypted)
        del encrypted
        ret = lz4.block.decompress(compressed, in_size)[self._next_offset:]
        self._next_block += 1
        self._next_offset = 0
        return ret

    def tell(self):
        return self._pos

    def seek(self, to):
        if to == self._pos:
            return

        if to % 512 == 511:
            to += 1

        size = to - self._pos
        self._pos = to
        if size >= 0 and size < len(self._buffer):
            self._buffer = self._buffer[size:]
            return

        self._buffer = bytes()
        block_num, in_pos, in_size = self._metadata_from_pos(self._pos)
        assert self._pos >= in_pos
        assert self._pos < in_pos + in_size
        self._next_block = block_num
        self._next_offset = self._pos - in_pos

    def read(self, size):
        if size == 1:
            # lie here, it's cheaper
            return b'0'

        shortfall = size - len(self._buffer)
        if shortfall > 0:
            self._buffer += self._get_next_block()

        self._pos += size
        ret = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return ret

METADATA_FROM_POS = """
SELECT id, in_pos, in_size FROM blocks
WHERE in_pos <= ? ORDER BY in_pos DESC LIMIT 1
"""

METADATA_FROM_BLOCK = """
SELECT in_pos, in_size, out_pos, out_size FROM blocks
WHERE id = ?
"""

INSERT_FILE = """
INSERT INTO files (name, size, mtime, mode, type, linkname, offset)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

class DAO(object):
    def __init__(self, filename):
        conn = sqlite3.connect(sys.argv[1], isolation_level=None)
        self._cursor = conn.cursor()

    def get_key(self):
        self._cursor.execute('SELECT key FROM meta')
        (key,) = self._cursor.fetchone()
        return key

    def metadata_from_pos(self, pos):
        self._cursor.execute(METADATA_FROM_POS, (pos,))
        return self._cursor.fetchone()

    def metadata_from_block(self, block):
        self._cursor.execute(METADATA_FROM_BLOCK, (block,))
        return self._cursor.fetchone()

    def index_file(self, *args):
        self._cursor.execute(INSERT_FILE, args)

    def truncate_files(self):
        self._cursor.execute("DELETE FROM files")

d = DAO(sys.argv[1])
decrypt = chacha20_decrypt(unhexlify(d.get_key()))
s3_object = boto3.resource('s3').Object(sys.argv[2], sys.argv[3])
reader = S3Reader(d.metadata_from_block, d.metadata_from_pos, decrypt,
    s3_object)
reader.seek(5059291648)
tf = tarfile.TarFile(fileobj=reader)
for ti in tf:
    print((ti.name, ti.offset))
    try:
        d.index_file(ti.name, ti.size, ti.mtime, ti.mode, ti.type, ti.linkname, ti.offset)
    except UnicodeEncodeError:
        pass
