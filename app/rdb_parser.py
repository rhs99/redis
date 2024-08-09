import os
import struct


class RDBParser:
    def __init__(self, dir, db_filename):
        self.dir = dir
        self.db_filename = db_filename

    def parse(self):
        result = []

        if self.dir and self.db_filename:
            rdb_file_path = os.path.join(self.dir, self.db_filename)
            if os.path.exists(rdb_file_path):
                with open(rdb_file_path, "rb") as f:
                    while operand := f.read(1):
                        if operand == b"\xfb":
                            break
                    f.read(2)

                    has_exp = 0

                    while operand := f.read(1):
                        if operand == b"\x00":
                            length = struct.unpack("B", f.read(1))[0]
                            if length >> 6 == 0b00:
                                length = length & 0b00111111
                            else:
                                length = 0
                            key = f.read(length).decode()

                            length = struct.unpack("B", f.read(1))[0]
                            if length >> 6 == 0b00:
                                length = length & 0b00111111
                            else:
                                length = 0
                            val = f.read(length).decode()

                            if has_exp == 1:
                                result[-1][0] = key
                                result[-1][1] = val
                            else:
                                result.append([key, val, None])

                            has_exp = 0

                        elif operand == b"\xfc":
                            has_exp = 1
                            exp = int.from_bytes(f.read(8), "little")
                            result.append(["", "", exp])
                        elif operand == b"\xfd":
                            has_exp = 1
                            exp = int.from_bytes(f.read(4), "little")
                            result.append(["", "", exp * 1000])
                        else:
                            break

        return result
