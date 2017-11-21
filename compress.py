from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from binascii import hexlify
import boto3
from os import pipe
import io
import lz4.block
import struct
import sys
import tempfile
from time import time
from multiprocessing import Process

class Encryptor(object):
    def __init__(self, key):
        self._aead = ChaCha20Poly1305(key)
        self._counter = 0

    def process(self, buf):
        nonce = struct.pack('<Qxxxx', self._counter)
        self._counter += 1
        return self._aead.encrypt(nonce, buf, None)

    @classmethod
    def new(cls):
        key = ChaCha20Poly1305.generate_key()
        return key, cls(key)

def compress(buf):
    return lz4.block.compress(buf, store_size=False)

def decompress(buf):
    return lz4.block.decompress(buf, len(buf))

def compress_loop():
    block_num = 0
    in_count = 0
    out_size = 0

    while True:
        in_data = sys.stdin.buffer.read(65536)
        if not in_data:
            break
        out_data = compress(in_data)
        out_size = len(out_data)
        yield in_count, out_data
        in_count += len(in_data)
        block_num += 1


s3 = boto3.resource('s3')
bucket = s3.Bucket('backups-kehu504bfzn5ook')

def upload(label, reader, writer):
    writer.close()
    blob = bucket.Object(label)
    blob.upload_fileobj(reader)

meta = []
key, e = Encryptor.new()
meta_file = tempfile.TemporaryFile(dir='/tmp/')
meta_file.write(hexlify(key))
out_pos = 0

(r, w) = pipe()
reader = io.open(r, 'rb')
writer = io.open(w, 'wb')
label = '{:016x}'.format(int(time() * 1e9))
p = Process(target=upload, args=(label,reader,writer))
del reader
p.start()

for block_num, d in enumerate(compress_loop()):
    in_pos, buf = d
    buf = e.process(buf)
    writer.write(buf)
    size = len(buf)
    meta_file.write('{} {} {} {}\n'.format(block_num, in_pos, out_pos, size).encode('ascii'))
    out_pos += size

writer.close()
p.join()
sys.stderr.write('Finished uploading data\n')

meta_file.seek(0)
meta_blob = bucket.Object(label + '.meta')
key, encryptor = Encryptor.new()
data = encryptor.process(meta_file.read())
meta_blob.put(Body=data)
sys.stdout.buffer.write(hexlify(key))
sys.stdout.write(' {}\n'.format(label))
