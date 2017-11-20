from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from binascii import hexlify
from google.cloud import storage
from os import pipe
import io
import lz4.block
import struct
import sys
from time import time
from multiprocessing import Process

def encrypt(bufs):
    key = ChaCha20Poly1305.generate_key()
    yield key
    chacha = ChaCha20Poly1305(key)
    counter = 0
    for buf in bufs:
        nonce = struct.pack('<Qxxxx', counter)
        counter += 1
        yield chacha.encrypt(nonce, buf, None)

def compress(buf):
    return lz4.block.compress(buf, store_size=False)

def decompress(buf):
    return lz4.block.decompress(buf, len(buf))

def compress_loop(meta):
    block_num = 0
    in_count = 0
    out_size = 0

    while True:
        in_data = sys.stdin.buffer.read(65536)
        if not in_data:
            break
        out_data = compress(in_data)
        yield out_data
        out_size = len(out_data)
        meta.append((block_num, in_count, out_size))
        in_count += len(in_data)
        block_num += 1

def upload(label, reader, writer):
    client = storage.Client(project='backups')
    bucket = client.get_bucket('backups-echi7ma0au5kuca')
    blob = bucket.blob(label)

    writer.close()
    blob.upload_from_file(reader)

meta = []
encryptor = encrypt(compress_loop(meta))
key = next(encryptor)
(r, w) = pipe()
reader = io.open(r, 'rb')
writer = io.open(w, 'wb')
label = '{:016x}'.format(int(time() * 1e9))
p = Process(target=upload, args=(label,reader,writer))
del reader
p.start()
for o in encryptor:
    writer.write(o)
writer.close()
p.join()
sys.stderr.write('Finished uploading data\n')
out = 0
meta_file = io.BytesIO()
meta_file.write(hexlify(key))
meta_file.write(b'\n')
for l in meta:
    num, in_, size = l
    # add authentication tag
    size += 16
    meta_file.write('{} {} {} {}\n'.format(num, in_, out, size).encode('ascii'))
    out += size
meta_file.seek(0)
client = storage.Client(project='backups')
bucket = client.get_bucket('backups-echi7ma0au5kuca')
meta_blob = bucket.blob(label + '.meta')
encryptor = encrypt([meta_file.getvalue()])
key = next(encryptor)
data = next(encryptor)
meta_blob.upload_from_string(data)
sys.stdout.buffer.write(hexlify(key))
sys.stdout.write('\n')
