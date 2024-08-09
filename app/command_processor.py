import time


class CommandProcessor:
    def __init__(self, conn, redis):
        self.conn = conn
        self.redis = redis

    def process_command(
        self,
        cmd: bytes,
        args: list[bytes],
    ):

        if (
            self.redis.queues.get(self.conn, None) is not None
            and cmd != b"exec"
            and cmd != b"discard"
        ):
            q = self.redis.queues.get(self.conn)
            q.append((cmd, args))
            self.conn.send("+QUEUED\r\n".encode())
            return

        name = cmd.decode()

        if name == "ping":
            self.conn.send("+PONG\r\n".encode())

        elif name == "echo":
            self.conn.send(
                f"${len(args[0].decode())}\r\n{args[0].decode()}\r\n".encode()
            )

        elif name == "type":
            key = args[0].decode()

            if key in self.redis.storage.stream:
                self.conn.send("+stream\r\n".encode())
                return

            val = self.redis.storage.get(key)

            if val is None:
                self.conn.send("+none\r\n".encode())
            else:
                self.conn.send("+string\r\n".encode())

        elif name == "set":
            key = args[0].decode()
            val = args[1].decode()

            exp = None
            if len(args) > 2 and args[2].decode().lower() == "px":
                delta = args[3].decode()
                exp = float(delta)

            self.redis.storage.set(key, val, exp)

            for replica in self.redis.replicas:
                replica.send(
                    f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n".encode()
                )

            self.redis.any_set_cmd = True
            return "+OK\r\n"

        elif name == "get":
            key = args[0].decode()
            val = None

            result = self.redis.rdb_parser.parse()

            if len(result) > 0:
                for ck, cv, ce in result:
                    if ck == key:
                        if ce is not None:
                            if time.time() * 1000 <= ce:
                                val = cv
                        else:
                            val = cv

                        break

            if not val:
                val = self.redis.storage.get(key)

            if val is not None:
                return f"${len(val)}\r\n{val}\r\n"
            else:
                return "$-1\r\n"

        elif name == "incr":
            key = args[0].decode()
            val = self.redis.storage.get(key)
            if val is None:
                val = 0

            if not isinstance(val, int):
                try:
                    val = int(val)
                except Exception:
                    return "-ERR value is not an integer or out of range\r\n"

            new_val = val + 1

            self.redis.storage.set(key, str(new_val))
            return f":{new_val}\r\n"

        elif name == "xadd":
            key = args[0].decode()
            id = args[1].decode()

            prev = self.redis.storage.get(key)
            prev_ms = 0
            prev_sq = 0
            curr_ms = None
            curr_sq = None

            if prev is not None:
                prev_id = prev.split(",")[-1].split(" ")[0]
                prev_ms = int(prev_id.split("-")[0])
                prev_sq = int(prev_id.split("-")[1])

            if id == "*":
                if prev is not None:
                    curr_ms = prev_ms
                    curr_sq = prev_sq + 1
                else:
                    curr_ms = int(time.time() * 1000)
                    curr_sq = 0
            elif "-*" in id:
                curr_ms = int(id.split("-")[0])
                curr_sq = 0 if curr_ms > 0 else 1

                if prev is not None:
                    if curr_ms < prev_ms:
                        self.conn.send(
                            "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".encode()
                        )
                        return
                    elif curr_ms == prev_ms:
                        curr_sq = prev_sq + 1
            else:
                curr_ms = int(id.split("-")[0])
                curr_sq = int(id.split("-")[1])

                if curr_ms < 0 or (curr_ms == 0 and curr_sq <= 0):
                    self.conn.send(
                        "-ERR The ID specified in XADD must be greater than 0-0\r\n".encode()
                    )
                    return

            valid = False
            if curr_ms > prev_ms or (curr_ms == prev_ms and curr_sq > prev_sq):
                valid = True

            if not valid:
                self.conn.send(
                    "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".encode()
                )
                return

            pairs = args[2:]

            new_id = f"{curr_ms}-{curr_sq}"
            val = new_id

            for item in pairs:
                val = f"{val} {item.decode()}"

            if prev is not None:
                val = f"{prev},{val}"

            self.redis.storage.set(key, val)
            self.redis.storage.stream.add(key)

            self.conn.send(f"${len(new_id)}\r\n{new_id}\r\n".encode())

        elif name == "xrange":
            key = args[0].decode()
            start_id = args[1].decode()
            end_id = args[2].decode()

            start_ms = None
            start_sq = 0
            end_ms = None
            end_sq = 10**9

            if start_id == "-":
                start_ms = 0
            elif "-" in start_id:
                start_ms = int(start_id.split("-")[0])
                start_sq = int(start_id.split("-")[1])
            else:
                start_ms = int(start_id)

            if end_id == "+":
                end_ms = 10**20
            elif "-" in end_id:
                end_ms = int(end_id.split("-")[0])
                end_sq = int(end_id.split("-")[1])
            else:
                end_ms = int(end_id)

            val = self.redis.storage.get(key)

            if val is None:
                self.conn.send("*0\r\n".encode())
                return

            entries = val.split(",")
            res = []

            for entry in entries:
                parts = entry.split(" ")
                curr_ms, curr_sq = parts[0].split("-")

                curr_ms = int(curr_ms)
                curr_sq = int(curr_sq)

                include = False
                if curr_ms >= start_ms and curr_ms <= end_ms:
                    if curr_ms == start_ms:
                        if curr_ms == end_ms:
                            include = bool(
                                curr_sq >= start_sq and curr_sq <= end_sq
                            )
                        else:
                            include = bool(curr_sq >= start_sq)
                    elif curr_ms == end_ms:
                        include = bool(curr_sq <= end_sq)
                    else:
                        include = True

                if include:
                    res.append([parts[0], parts[1:]])

            temp = f"*{len(res)}\r\n"

            for r in res:
                temp = f"{temp}*{len(r)}\r\n"
                temp = f"{temp}${len(r[0])}\r\n{r[0]}\r\n"
                temp = f"{temp}*{len(r[1])}\r\n"
                for cv in r[1]:
                    temp = f"{temp}${len(cv)}\r\n{cv}\r\n"

            self.conn.send(temp.encode())

        elif name == "xread":
            wait_until_found = False
            if args[0].decode() == "block":
                timeout = None
                if args[1] == b"0" or args[1] == b"\\x00":
                    timeout = 0
                else:
                    timeout = float(args[1].decode())

                args = args[2:]

                if args[2].decode() == "$":
                    key = args[1].decode()
                    prev = self.redis.storage.get(key)

                    start_ms = 0
                    start_sq = 0

                    if prev is not None:
                        last_id = prev.split(",")[-1].split(" ")[0]
                        start_ms = int(last_id.split("-")[0])
                        start_sq = int(last_id.split("-")[1])

                    args[2] = f"{start_ms}-{start_sq}".encode()

                if timeout == 0:
                    wait_until_found = True
                else:
                    start_at = time.time()
                    while True:
                        end_at = time.time()
                        if (end_at - start_at) * 1000 > timeout:
                            break

            args = args[1:]
            streams = []
            ids = []
            offset = len(args) // 2

            for i in range(offset):
                streams.append(args[i])
                ids.append(args[i + offset])

            res = []

            while True:
                for i in range(offset):
                    key = streams[i].decode()
                    start_id = ids[i].decode()

                    val = self.redis.storage.get(key)
                    if val is None:
                        continue

                    entries = val.split(",")

                    start_ms = None
                    start_sq = 0

                    if start_id == "-":
                        start_ms = 0
                    elif "-" in start_id:
                        start_ms = int(start_id.split("-")[0])
                        start_sq = int(start_id.split("-")[1])
                    else:
                        start_ms = int(start_id)

                    inner_res = []

                    for entry in entries:
                        parts = entry.split(" ")
                        curr_ms, curr_sq = parts[0].split("-")

                        curr_ms = int(curr_ms)
                        curr_sq = int(curr_sq)

                        include = False
                        if curr_ms >= start_ms:
                            if curr_ms == start_ms:
                                include = bool(curr_sq > start_sq)
                            else:
                                include = True

                        if include:
                            inner_res.append([parts[0], parts[1:]])

                    if len(inner_res) > 0:
                        res.append([key, inner_res])

                if not wait_until_found or len(res) > 0:
                    break

            if len(res) == 0:
                self.conn.send("$-1\r\n".encode())
                return

            temp = f"*{len(res)}\r\n"

            for r in res:
                temp = f"{temp}*{len(r)}\r\n"
                temp = f"{temp}${len(r[0])}\r\n{r[0]}\r\n"
                temp = f"{temp}*{len(r[1])}\r\n"
                for cv in r[1]:
                    temp = f"{temp}*{len(cv)}\r\n"
                    temp = f"{temp}${len(cv[0])}\r\n{cv[0]}\r\n"
                    temp = f"{temp}*{len(cv[1])}\r\n"
                    for inv in cv[1]:
                        temp = f"{temp}${len(inv)}\r\n{inv}\r\n"

            self.conn.send(temp.encode())

        elif name == "multi":
            self.redis.queues[self.conn] = []
            self.conn.send("+OK\r\n".encode())

        elif name == "exec":
            q = self.redis.queues.get(self.conn, None)
            if q is None:
                self.conn.send("-ERR EXEC without MULTI\r\n".encode())
                return

            self.redis.queues.pop(self.conn)

            if len(q) == 0:
                self.conn.send("*0\r\n".encode())
                return

            temp = f"*{len(q)}\r\n"

            for c, a in q:
                t = self.process_command(c, a)
                temp = f"{temp}{t}"

            self.conn.send(temp.encode())

        elif name == "discard":
            q = self.redis.queues.get(self.conn, None)
            if q is None:
                self.conn.send("-ERR DISCARD without MULTI\r\n".encode())
                return

            self.redis.queues.pop(self.conn)
            self.conn.send("+OK\r\n".encode())

        elif name == "info":
            res = f"role:{self.redis.role}\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0"
            self.conn.send(f"${len(res)}\r\n{res}\r\n".encode())

        elif name == "config":
            key = args[1].decode().lower()
            if key == "dir":
                self.conn.send(
                    f"*2\r\n$3\r\ndir\r\n${len(self.redis.rdb_parser.dir)}\r\n{self.redis.rdb_parser.dir}\r\n".encode()
                )
            elif key == "dbfilename":
                self.conn.send(
                    f"*2\r\n$10\r\ndbfilename\r\n${len(self.redis.rdb_parser.db_filename)}\r\n{self.redis.rdb_parser.db_filename}\r\n".encode()
                )

        elif name == "keys":
            result = self.redis.rdb_parser.parse()

            if len(result) > 0:
                temp = ""
                for key, _, _ in result:
                    temp = f"{temp}\r\n${len(key)}\r\n{key}"

                self.conn.send(f"*{len(result)}{temp}\r\n".encode())
            else:
                self.conn.send("*0\r\n".encode())

        elif name == "wait":
            req_num_replicas = int(args[0].decode())
            timeout_ms = float(args[1].decode())

            if req_num_replicas == 0:
                self.conn.send(f":0\r\n".encode())
                return

            self.redis.updated_replica_cnt = 0
            for r in self.redis.replicas:
                r.send(
                    "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".encode()
                )

            start_at = time.time()
            while True:
                end_at = time.time()
                if (end_at - start_at) * 1000 > timeout_ms:
                    break

            self.conn.send(
                f":{self.redis.updated_replica_cnt if self.redis.any_set_cmd else len(self.redis.replicas)}\r\n".encode()
            )

        elif name == "replconf":
            if args[0].decode() == "listening-port":
                self.conn.send("+OK\r\n".encode())
            elif args[0].decode() == "capa":
                self.conn.send("+OK\r\n".encode())
            elif args[0].decode().lower() == "ack":
                self.redis.updated_replica_cnt += 1

        elif name == "psync":
            self.conn.send(
                "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".encode()
            )

            rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
            rdb_content = bytes.fromhex(rdb_hex)
            res = f"${len(rdb_content)}\r\n".encode()
            self.conn.send(res + rdb_content)

            self.redis.replicas.append(self.conn)
