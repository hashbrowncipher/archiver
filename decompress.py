#!/usr/bin/env python
import binascii
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
import struct
import sys

def chacha20_decrypt(key):
    aead = ChaCha20Poly1305(key)
    def decrypt(counter, data):
        nonce = struct.pack('<Qxxxx', counter)
        return aead.decrypt(nonce, data, None)
    return decrypt

def main():
    sys.stderr.write('Key: ')
    key = binascii.unhexlify(input())
    data = open(sys.argv[1], 'rb').read()
    sys.stdout.buffer.write(chacha20_decrypt(key)(0, data))


if __name__ == '__main__':
    main()
