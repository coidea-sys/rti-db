#!/usr/bin/env python3
"""修补 ld_prime 链接的 Mach-O：符号表字符串池 8 字节对齐 + adhoc 重签。

症状：dlopen 报 "mis-aligned LINKEDIT string pool"。
根因：新版 Xcode ld_prime 把 LC_SYMTAB 字符串池放在 4 字节对齐（而非 8）处，
dyld 拒绝加载。此脚本在字符串池前插入填充、修正所有受影响的加载命令，
移除旧签名并以 adhoc 重签。

用法：python3 fix-macho-linkedit.py <input.so> <output.so>
"""
import struct
import subprocess
import sys


def load_commands(data: bytearray):
    ncmds, = struct.unpack_from("<I", data, 16)
    sizeofcmds, = struct.unpack_from("<I", data, 20)
    off = 32
    end = 32 + sizeofcmds
    while off < end:
        cmd, cmdsize = struct.unpack_from("<2I", data, off)
        yield off, cmd, cmdsize
        off += cmdsize


def main():
    src, dst = sys.argv[1], sys.argv[2]
    subprocess.run(["codesign", "--remove-signature", src], check=True)
    data = bytearray(open(src, "rb").read())
    assert struct.unpack_from("<I", data, 0)[0] == 0xFEEDFACF, "not Mach-O 64"

    stroff = symend = None
    seg_le = None
    for off, cmd, _ in load_commands(data):
        if cmd == 0x2:  # LC_SYMTAB
            _, _, stroff, _ = struct.unpack_from("<4I", data, off + 8)
        if cmd == 0x19 and data[off + 8:off + 20].rstrip(b"\x00") == b"__LINKEDIT":
            seg_le = off
    assert stroff is not None and seg_le is not None
    if stroff % 8 == 0:
        print("already aligned")
        return

    pad = 8 - stroff % 8
    linkedit_fileoff, = struct.unpack_from("<Q", data, seg_le + 40)
    new_eof = len(data) + pad

    for off, cmd, _ in load_commands(data):
        if cmd == 0x2:
            struct.pack_into("<I", data, off + 16, stroff + pad)
        elif cmd == 0x19 and data[off + 8:off + 20].rstrip(b"\x00") == b"__LINKEDIT":
            struct.pack_into("<Q", data, off + 32, (new_eof - linkedit_fileoff + 0xFFF) & ~0xFFF)
            struct.pack_into("<Q", data, off + 48, new_eof - linkedit_fileoff)

    fixed = data[:stroff] + b"\x00" * pad + data[stroff:]
    open(dst, "wb").write(fixed)
    subprocess.run(["codesign", "--sign", "-", "--force", dst], check=True)
    print(f"patched: stroff {stroff} -> {stroff + pad}, written {dst}")


if __name__ == "__main__":
    main()
