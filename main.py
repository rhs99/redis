import time
import socket
import argparse
import threading
import struct
import os

from storage import Storage
from decoder import RESPDecoder


storage = Storage()
replicas = []
number_of_bytes_processed = 0
updated_replica_cnt = 0
any_set_cmd = False
queues = dict()


def handle_master_connection(conn: socket.socket, replica_port):
    global number_of_bytes_processed

    decoder = RESPDecoder(conn)

    conn.send("*1\r\n$4\r\nping\r\n".encode())
    print(decoder.decode_simple_string())

    conn.send(
        f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{replica_port}\r\n".encode()
    )
    print(decoder.decode_simple_string())

    conn.send("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode())
    print(decoder.decode_simple_string())

    conn.send("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode())

    print(decoder.decode_simple_string())
    print(decoder.decode_rdb())

    while True:
        decoded = decoder.decode()
        if decoded is None:
            break

        if isinstance(decoded, bytes):
            command = decoded.lower()
            args = None
        else:
            command = decoded[0].lower()
            args = decoded[1:]

        if command.decode() == "ping":
            number_of_bytes_processed += len(b"*1\r\n$4\r\4PING\r\n")
        elif command.decode() == "set":
            key = args[0].decode()
            val = args[1].decode()

            exp = None
            delta = None

            if len(args) > 2 and args[2].decode().lower() == "px":
                delta = args[3].decode()
                exp = float(delta)
            storage.set(key, val, exp)

            processed_cmd = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
            if exp:
                processed_cmd = f"*5\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n$2\r\npx\r\n${len(delta)}\r\n{delta}\r\n"

            number_of_bytes_processed += len(processed_cmd)

        elif command.decode() == "replconf":
            processed_cmd = str(number_of_bytes_processed)
            conn.send(
                f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(processed_cmd)}\r\n{processed_cmd}\r\n".encode()
            )

            number_of_bytes_processed += len(
                b"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n"
            )


class Redis:
    def __init__(self, args):
        self.port = int(args.port)
        self.dir = args.dir
        self.db_filename = args.db_filename
        self.role = "master" if args.master_addr is None else "slave"

        if args.master_addr:
            master_host, master_port = args.master_addr.split(" ")

            replica_to_master_conn = socket.create_connection(
                (master_host, int(master_port))
            )

            threading.Thread(
                target=handle_master_connection,
                args=(replica_to_master_conn, self.port),
                daemon=True,
            ).start()


def read_rdb_data(dir, dbfilename):
    result = []

    if dir and dbfilename:
        rdb_file_path = os.path.join(dir, dbfilename)
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
                        exp = int.from_bytes(f.read(8), 'little')
                        result.append(["", "", exp])
                    elif operand == b"\xfd":
                        has_exp = 1
                        exp = int.from_bytes(f.read(4), 'little')
                        result.append(["", "", exp*1000])
                    else:
                        break

    return result

def helper(conn: socket.socket, redis: Redis, cmd: bytes, args: list[bytes]):
    global updated_replica_cnt
    global any_set_cmd

    if queues.get(conn, None) is not None and cmd != b"exec" and cmd != b"discard":
        q = queues.get(conn)
        q.append((cmd, args))
        conn.send("+QUEUED\r\n".encode())
        return

    name = cmd.decode()

    if name == "ping":
        conn.send("+PONG\r\n".encode())

    elif name == "echo":
        conn.send(
            f"${len(args[0].decode())}\r\n{args[0].decode()}\r\n".encode()
        )

    elif name == "set":
        key = args[0].decode()
        val = args[1].decode()

        exp = None
        if len(args) > 2 and args[2].decode().lower() == "px":
            delta = args[3].decode()
            exp = float(delta)

        storage.set(key, val, exp)

        for replica in replicas:
            replica.send(
                f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n".encode()
            )

        any_set_cmd = True
        return "+OK\r\n"

    elif name == "get":
        key = args[0].decode()
        val = None

        result = read_rdb_data(redis.dir, redis.db_filename)

        if len(result) > 0:
            for ck, cv, ce in result:
                if ck == key:
                    if ce is not None:
                        if time.time()*1000 <= ce:
                            val = cv
                    else:
                        val = cv
                        
                    break

        if not val:
            val = storage.get(key)

        if val is not None:
            return f"${len(val)}\r\n{val}\r\n"
        else:
            return "$-1\r\n"

    elif name == "incr":
        key = args[0].decode()
        val = storage.get(key)
        if val is None:
            val = 0

        if not isinstance(val, int):
            try:
                val = int(val)
            except Exception:
                return "-ERR value is not an integer or out of range\r\n"
            
        new_val = val + 1

        storage.set(key, str(new_val))
        return f":{new_val}\r\n"
    
    elif name == "multi":
        queues[conn] = []
        conn.send("+OK\r\n".encode())
    
    elif name == "exec":
        q = queues.get(conn, None)
        if q is None:
            conn.send("-ERR EXEC without MULTI\r\n".encode())
            return
        
        queues.pop(conn)

        if len(q) == 0:
            conn.send("*0\r\n".encode())
            return     

        temp = f"*{len(q)}\r\n"

        for c, a in q:
            t = helper(conn, redis, c, a)
            temp = f"{temp}{t}"
        
        conn.send(temp.encode())
    
    elif name == "discard":
        q = queues.get(conn, None)
        if q is None:
            conn.send("-ERR DISCARD without MULTI\r\n".encode())
            return
        
        queues.pop(conn)
        conn.send("+OK\r\n".encode())


    elif name == "info":
        res = f"role:{redis.role}\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0"
        conn.send(f"${len(res)}\r\n{res}\r\n".encode())

    elif name == "config":
        key = args[1].decode().lower()
        if key == "dir":
            conn.send(f"*2\r\n$3\r\ndir\r\n${len(redis.dir)}\r\n{redis.dir}\r\n".encode())
        elif key == "dbfilename":
            conn.send(f"*2\r\n$10\r\ndbfilename\r\n${len(redis.db_filename)}\r\n{redis.db_filename}\r\n".encode())

    elif name == "keys":
        result = read_rdb_data(redis.dir, redis.db_filename)
        if len(result) > 0:
            temp = ""
            for key, _, _ in result:
                temp = f"{temp}\r\n${len(key)}\r\n{key}"

            conn.send(f"*{len(result)}{temp}\r\n".encode())
        else:
            conn.send("*0\r\n".encode())

    elif name == "wait":
        req_num_replicas = int(args[0].decode())
        timeout_ms = float(args[1].decode())

        if req_num_replicas == 0:
            conn.send(f":0\r\n".encode())
            return

        updated_replica_cnt = 0
        for r in replicas:
            try:
                r.send(
                    "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".encode()
                )
            except Exception as e:
                print(str(e))

        start_at = time.time()
        while True:
            end_at = time.time()
            if (end_at - start_at) * 1000 > timeout_ms:
                break

        conn.send(
            f":{updated_replica_cnt if any_set_cmd else len(replicas)}\r\n".encode()
        )

    elif name == "replconf":
        if args[0].decode() == "listening-port":
            print(args[1].decode())
            conn.send("+OK\r\n".encode())
        elif args[0].decode() == "capa":
            print(args[1].decode())
            conn.send("+OK\r\n".encode())
        elif args[0].decode().lower() == "ack":
            updated_replica_cnt += 1

    elif name == "psync":
        conn.send(
            "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".encode()
        )

        rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        rdb_content = bytes.fromhex(rdb_hex)
        res = f"${len(rdb_content)}\r\n".encode()
        conn.send(res + rdb_content)

        replicas.append(conn)


def handle_connection(conn: socket.socket, redis: Redis):
    with conn:
        while True:
            decoded = RESPDecoder(conn).decode()

            if decoded is None:
                break

            if isinstance(decoded, bytes):
                command = decoded.lower()
                args = None
            else:
                command = decoded[0].lower()
                args = decoded[1:]

            ret = helper(conn, redis, command, args)
            if ret is not None:
                conn.send(ret.encode())


def main(args):
    redis = Redis(args)

    server_socket = socket.create_server(
        ("localhost", int(redis.port)), reuse_port=True
    )

    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_connection, args=(conn, redis)).start()


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--port", dest="port", default=6379)
    arg_parser.add_argument("--replicaof", dest="master_addr", default=None)
    arg_parser.add_argument("--dir", dest="dir", default="/tmp")
    arg_parser.add_argument("--dbfilename", dest="db_filename", default="dump.rdb")
    args = arg_parser.parse_args()

    main(args)
