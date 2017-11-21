from decompress import chacha20_decrypt
from os import pread
from tarfile import TarInfo
import tarfile
from binascii import unhexlify
import sys
import lz4.block

key = sys.stdin.read(65)[:-1]
key = unhexlify(key)
decrypt = chacha20_decrypt(key)
data = open(sys.argv[1], 'rb')
data_fd = data.fileno()
for l in sys.stdin:
    num, in_start, _, out_size = l.rstrip().split()
    num = int(num)
    in_start = int(in_start)
    out_size = int(out_size)
    plaintext = decrypt(num, data.read(out_size), )
    raw = lz4.block.decompress(plaintext, 65536)
    pos = 0
    ti = TarInfo.frombuf(raw[pos:pos+512], 'utf-8', "surrogateescape")
    info = (ti.name, ti.size, ti.mtime, oct(ti.mode), ti.type, ti.linkname)
    print(info)
    pos += 512 + ti.size
    ti = TarInfo.frombuf(raw[pos:pos+512], 'utf-8', "surrogateescape")
    info = (ti.name, ti.size, ti.mtime, oct(ti.mode), ti.type, ti.linkname)
    print(info)

    break
